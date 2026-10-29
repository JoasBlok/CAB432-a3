// worker.mjs
// ECS worker: polls SQS for media jobs, downloads the source video, converts it
// with ffmpeg, uploads the result to S3 and records the outcome in DynamoDB.
//
// Job message (JSON body):
//   {
//     "cacheKey": "abc123",
//     "sourceUrl": "https://example.com/video.mp4",
//     "task": "mp3",                      // optional, defaults to "mp3"
//     "options": { "bitrate": "192k" }    // optional, task specific
//   }

import { spawn } from "node:child_process";
import dns from "node:dns";
import { createReadStream, createWriteStream } from "node:fs";
import { mkdir, mkdtemp, rm, stat } from "node:fs/promises";
import http from "node:http";
import https from "node:https";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import { pathToFileURL } from "node:url";

import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, UpdateCommand } from "@aws-sdk/lib-dynamodb";

/* -------------------------------------------------------------------------- */
/* Config + logging                                                           */
/* -------------------------------------------------------------------------- */

function requireEnv(name) {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required environment variable ${name}`);
  return value;
}

export function loadConfig() {
  return {
    queueUrl: requireEnv("QUEUE_URL"),
    tableName: requireEnv("TABLE_NAME"),
    bucketName: requireEnv("BUCKET_NAME"),
    s3KeyPrefix: process.env.S3_KEY_PREFIX ?? "mp3/",
    workDir: process.env.WORK_DIR ?? path.join(os.tmpdir(), "media-worker"),
    maxDownloadBytes: Number(process.env.MAX_DOWNLOAD_BYTES ?? 1024 ** 3), // 1 GiB
    downloadTimeoutMs: Number(process.env.DOWNLOAD_TIMEOUT_MS ?? 15 * 60_000),
    ffmpegTimeoutMs: Number(process.env.FFMPEG_TIMEOUT_MS ?? 30 * 60_000),
    // Should match maxReceiveCount on the queue's redrive policy.
    maxAttempts: Number(process.env.MAX_ATTEMPTS ?? 3),
    visibilityTimeoutSec: Number(process.env.VISIBILITY_TIMEOUT_SEC ?? 300),
    heartbeatSec: Number(process.env.HEARTBEAT_SEC ?? 120),
    retryDelaySec: Number(process.env.RETRY_DELAY_SEC ?? 30),
    // Development only: allows downloads from private/loopback addresses.
    allowPrivateHosts: process.env.ALLOW_PRIVATE_HOSTS === "true",
  };
}

function log(level, message, extra = {}) {
  const line = JSON.stringify({
    level,
    message,
    time: new Date().toISOString(),
    ...extra,
  });
  (level === "error" ? console.error : console.log)(line);
}

/**
 * permanent = true  -> retrying will not help (bad input, unsupported file...).
 *                      The job is marked FAILED and the message is deleted.
 * permanent = false -> possibly transient. The message is left for another
 *                      attempt until maxAttempts is reached.
 */
export class JobError extends Error {
  constructor(message, { permanent = false, cacheKey } = {}) {
    super(message);
    this.name = "JobError";
    this.permanent = permanent;
    this.cacheKey = cacheKey;
  }
}

/* -------------------------------------------------------------------------- */
/* Job parsing + task registry                                                */
/* -------------------------------------------------------------------------- */

const MP3_BITRATES = new Set([
  "64k",
  "96k",
  "128k",
  "160k",
  "192k",
  "256k",
  "320k",
]);

// Add new operations here (and a matching run() implementation).
const TASKS = {
  mp3: {
    outputExt: "mp3",
    contentType: "audio/mpeg",
    parseOptions(options = {}) {
      if (
        typeof options !== "object" ||
        options === null ||
        Array.isArray(options)
      ) {
        throw new Error("'options' must be an object");
      }
      const bitrate = options.bitrate ?? "192k";
      if (!MP3_BITRATES.has(bitrate)) {
        throw new Error(
          `Unsupported bitrate '${bitrate}'. Allowed: ${[...MP3_BITRATES].join(", ")}`,
        );
      }
      return { bitrate };
    },
    run(inputPath, outputPath, options, cfg) {
      return convertToMp3(inputPath, outputPath, options, cfg.ffmpegTimeoutMs);
    },
  },
};

export function parseJob(body) {
  let raw;
  try {
    raw = JSON.parse(body);
  } catch {
    throw new JobError("Message body is not valid JSON", { permanent: true });
  }
  if (typeof raw !== "object" || raw === null) {
    throw new JobError("Message body must be a JSON object", {
      permanent: true,
    });
  }

  const { cacheKey } = raw;
  if (typeof cacheKey !== "string" || !/^[\w.-]{1,256}$/.test(cacheKey)) {
    throw new JobError("Missing or invalid 'cacheKey'", { permanent: true });
  }

  // From here on the job is attributable, so failures can be written to DynamoDB.
  const fail = (message) =>
    new JobError(message, { permanent: true, cacheKey });

  if (typeof raw.sourceUrl !== "string") throw fail("Missing 'sourceUrl'");
  let url;
  try {
    url = new URL(raw.sourceUrl);
  } catch {
    throw fail("'sourceUrl' is not a valid URL");
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw fail("'sourceUrl' must be an http(s) URL");
  }

  const task = raw.task ?? "mp3";
  const spec = TASKS[task];
  if (!spec) throw fail(`Unknown task '${task}'`);

  let options;
  try {
    options = spec.parseOptions(raw.options);
  } catch (err) {
    throw fail(err.message);
  }

  return { cacheKey, sourceUrl: url.toString(), task, options };
}

/* -------------------------------------------------------------------------- */
/* Safe downloader (SSRF protected)                                           */
/* -------------------------------------------------------------------------- */
// Users supply the URL, and an ECS task can reach the container credentials
// endpoint (169.254.170.2) and other internal addresses. So every connection,
// including each redirect hop, is checked against private/reserved ranges at
// connect time (which also defeats DNS rebinding).

const blockedRanges = new net.BlockList();
for (const [addr, prefix] of [
  ["0.0.0.0", 8],
  ["10.0.0.0", 8],
  ["100.64.0.0", 10],
  ["127.0.0.0", 8],
  ["169.254.0.0", 16], // link-local: instance metadata + ECS credentials
  ["172.16.0.0", 12],
  ["192.0.0.0", 24],
  ["192.168.0.0", 16],
  ["198.18.0.0", 15],
  ["224.0.0.0", 4],
  ["240.0.0.0", 4],
]) {
  blockedRanges.addSubnet(addr, prefix, "ipv4");
}
for (const [addr, prefix] of [
  ["::", 128],
  ["::1", 128],
  ["fc00::", 7],
  ["fe80::", 10],
  ["ff00::", 8],
]) {
  blockedRanges.addSubnet(addr, prefix, "ipv6");
}

export function isBlockedAddress(address) {
  const family = net.isIP(address);
  if (family === 0) return true; // not an IP: refuse
  if (family === 4) return blockedRanges.check(address, "ipv4");

  const lower = address.toLowerCase();
  // IPv4-mapped IPv6, dotted form (::ffff:127.0.0.1)
  const dotted = lower.match(/^::ffff:(\d+\.\d+\.\d+\.\d+)$/);
  if (dotted) return blockedRanges.check(dotted[1], "ipv4");
  // IPv4-mapped IPv6, hex form (::ffff:7f00:1), which is what URL parsing produces
  const hex = lower.match(/^::ffff:([0-9a-f]{1,4}):([0-9a-f]{1,4})$/);
  if (hex) {
    const hi = parseInt(hex[1], 16);
    const lo = parseInt(hex[2], 16);
    return blockedRanges.check(
      `${hi >> 8}.${hi & 255}.${lo >> 8}.${lo & 255}`,
      "ipv4",
    );
  }
  return blockedRanges.check(address, "ipv6");
}

function makeGuardedLookup(allowPrivate) {
  return (hostname, options, callback) => {
    if (typeof options === "function") {
      callback = options;
      options = {};
    }
    dns.lookup(hostname, options, (err, address, family) => {
      if (err) return callback(err);
      const list = Array.isArray(address) ? address : [{ address, family }];
      if (
        !allowPrivate &&
        list.some((entry) => isBlockedAddress(entry.address))
      ) {
        return callback(
          Object.assign(
            new Error(`Host '${hostname}' resolves to a disallowed address`),
            {
              code: "EBLOCKED",
            },
          ),
        );
      }
      callback(null, address, family);
    });
  };
}

function assertAllowedUrl(url, allowPrivate) {
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new JobError("Only http(s) URLs are allowed", { permanent: true });
  }
  // dns.lookup is skipped for IP literals, so check those here.
  const host = url.hostname.replace(/^\[|\]$/g, "");
  if (!allowPrivate && net.isIP(host) && isBlockedAddress(host)) {
    throw new JobError("URL points to a disallowed address", {
      permanent: true,
    });
  }
}

function requestOnce(url, { allowPrivate, idleTimeoutMs, signal }) {
  return new Promise((resolve, reject) => {
    const lib = url.protocol === "https:" ? https : http;
    const req = lib.get(
      url,
      {
        lookup: makeGuardedLookup(allowPrivate),
        timeout: idleTimeoutMs,
        signal,
        headers: { "User-Agent": "media-worker/1.0", Accept: "*/*" },
      },
      resolve,
    );
    req.on("timeout", () => req.destroy(new Error("Connection timed out")));
    req.on("error", (err) => {
      if (err.code === "EBLOCKED") {
        reject(new JobError(err.message, { permanent: true }));
      } else {
        reject(err);
      }
    });
  });
}

export async function downloadToFile(sourceUrl, destPath, opts) {
  const {
    maxBytes,
    allowPrivate = false,
    maxRedirects = 5,
    idleTimeoutMs = 30_000,
    totalTimeoutMs = 15 * 60_000,
  } = opts;
  const signal = AbortSignal.timeout(totalTimeoutMs);

  let current = new URL(sourceUrl);
  for (let hop = 0; hop <= maxRedirects; hop++) {
    assertAllowedUrl(current, allowPrivate);
    const res = await requestOnce(current, {
      allowPrivate,
      idleTimeoutMs,
      signal,
    });
    const status = res.statusCode ?? 0;

    if ([301, 302, 303, 307, 308].includes(status)) {
      res.resume();
      const location = res.headers.location;
      if (!location)
        throw new JobError("Redirect without Location header", {
          permanent: true,
        });
      current = new URL(location, current);
      continue;
    }

    if (status !== 200) {
      res.resume();
      const retryable = status === 408 || status === 429 || status >= 500;
      throw new JobError(`Download failed with HTTP ${status}`, {
        permanent: !retryable,
      });
    }

    const declared = Number(res.headers["content-length"]);
    if (Number.isFinite(declared) && declared > maxBytes) {
      res.destroy();
      throw new JobError(
        `File is too large (${declared} bytes, limit ${maxBytes})`,
        {
          permanent: true,
        },
      );
    }

    let received = 0;
    const limiter = new Transform({
      transform(chunk, _encoding, callback) {
        received += chunk.length;
        if (received > maxBytes) {
          return callback(
            new JobError(`File exceeds the ${maxBytes} byte limit`, {
              permanent: true,
            }),
          );
        }
        callback(null, chunk);
      },
    });
    await pipeline(res, limiter, createWriteStream(destPath), { signal });
    if (received === 0)
      throw new JobError("Downloaded file is empty", { permanent: true });
    return received;
  }
  throw new JobError("Too many redirects", { permanent: true });
}

/* -------------------------------------------------------------------------- */
/* ffmpeg                                                                     */
/* -------------------------------------------------------------------------- */

export function convertToMp3(inputPath, outputPath, { bitrate }, timeoutMs) {
  const args = [
    "-hide_banner",
    "-nostdin",
    "-loglevel",
    "error",
    "-y",
    // The input is untrusted. Refuse playlists/containers that reference
    // network or other local resources.
    "-protocol_whitelist",
    "file",
    "-i",
    inputPath,
    "-map",
    "0:a:0", // first audio stream
    "-vn",
    "-codec:a",
    "libmp3lame",
    "-b:a",
    bitrate,
    outputPath,
  ];

  return new Promise((resolve, reject) => {
    const child = spawn("ffmpeg", args, {
      stdio: ["ignore", "ignore", "pipe"],
    });
    let stderr = "";
    let timedOut = false;

    child.stderr.on("data", (chunk) => {
      stderr = (stderr + chunk.toString()).slice(-4000); // keep the tail only
    });

    const timer = setTimeout(() => {
      timedOut = true;
      child.kill("SIGKILL");
    }, timeoutMs);

    child.on("error", (err) => {
      clearTimeout(timer);
      reject(err); // e.g. ffmpeg binary missing: not the job's fault
    });

    child.on("close", (code) => {
      clearTimeout(timer);
      if (code === 0) return resolve();
      if (timedOut) {
        return reject(
          new JobError(`ffmpeg timed out after ${timeoutMs} ms`, {
            permanent: true,
          }),
        );
      }
      const noAudio = /matches no streams/i.test(stderr);
      reject(
        new JobError(
          noAudio
            ? "The file has no audio track"
            : `ffmpeg failed (exit ${code}): ${stderr.trim().split("\n").slice(-3).join(" | ")}`,
          { permanent: true },
        ),
      );
    });
  });
}

/* -------------------------------------------------------------------------- */
/* DynamoDB + SQS helpers                                                     */
/* -------------------------------------------------------------------------- */

// Assumes the table's primary key is just `cacheKey`. If it has a sort key,
// add it to Key below.
async function updateStatus(
  ctx,
  cacheKey,
  status,
  extra = {},
  { onlyIfNotComplete = false } = {},
) {
  const names = { "#status": "status" };
  const values = { ":status": status, ":now": new Date().toISOString() };
  const sets = ["#status = :status", "updatedAt = :now"];

  Object.entries(extra).forEach(([key, value], i) => {
    names[`#x${i}`] = key;
    values[`:x${i}`] = value;
    sets.push(`#x${i} = :x${i}`);
  });

  let condition = "attribute_exists(cacheKey)"; // never resurrect a deleted/expired record
  if (onlyIfNotComplete) {
    condition += " AND #status <> :complete";
    values[":complete"] = "COMPLETE";
  }

  try {
    await ctx.ddb.send(
      new UpdateCommand({
        TableName: ctx.cfg.tableName,
        Key: { cacheKey },
        UpdateExpression: `SET ${sets.join(", ")}`,
        ConditionExpression: condition,
        ExpressionAttributeNames: names,
        ExpressionAttributeValues: values,
      }),
    );
    return true;
  } catch (err) {
    if (err.name === "ConditionalCheckFailedException") return false;
    throw err;
  }
}

function deleteMessage(ctx, message) {
  return ctx.sqs.send(
    new DeleteMessageCommand({
      QueueUrl: ctx.cfg.queueUrl,
      ReceiptHandle: message.ReceiptHandle,
    }),
  );
}

// Keeps the message invisible to other workers while a long job is running.
function startHeartbeat(ctx, message) {
  const timer = setInterval(() => {
    ctx.sqs
      .send(
        new ChangeMessageVisibilityCommand({
          QueueUrl: ctx.cfg.queueUrl,
          ReceiptHandle: message.ReceiptHandle,
          VisibilityTimeout: ctx.cfg.visibilityTimeoutSec,
        }),
      )
      .catch((err) =>
        log("warn", "Visibility heartbeat failed", { error: err.message }),
      );
  }, ctx.cfg.heartbeatSec * 1000);
  timer.unref();
  return { stop: () => clearInterval(timer) };
}

/* -------------------------------------------------------------------------- */
/* Job processing                                                             */
/* -------------------------------------------------------------------------- */

async function runJob(ctx, job, workDir) {
  const spec = TASKS[job.task];
  const inputPath = path.join(workDir, "input"); // no extension: ffmpeg probes the content
  const outputPath = path.join(workDir, `output.${spec.outputExt}`);

  const downloaded = await downloadToFile(job.sourceUrl, inputPath, {
    maxBytes: ctx.cfg.maxDownloadBytes,
    allowPrivate: ctx.cfg.allowPrivateHosts,
    totalTimeoutMs: ctx.cfg.downloadTimeoutMs,
  });
  log("info", "Downloaded source", {
    cacheKey: job.cacheKey,
    bytes: downloaded,
  });

  await spec.run(inputPath, outputPath, job.options, ctx.cfg);
  const { size } = await stat(outputPath);
  log("info", "Conversion finished", {
    cacheKey: job.cacheKey,
    outputBytes: size,
  });

  const outputKey = `${ctx.cfg.s3KeyPrefix}${job.cacheKey}.${spec.outputExt}`;
  await ctx.s3.send(
    new PutObjectCommand({
      Bucket: ctx.cfg.bucketName,
      Key: outputKey,
      Body: createReadStream(outputPath),
      ContentLength: size,
      ContentType: spec.contentType,
    }),
  );
  return outputKey;
}

export async function processMessage(ctx, message) {
  const receiveCount = Number(message.Attributes?.ApproximateReceiveCount ?? 1);

  let job;
  try {
    job = parseJob(message.Body);
  } catch (err) {
    log("error", "Invalid job message", {
      messageId: message.MessageId,
      error: err.message,
    });
    if (err.cacheKey) {
      // Attributable, so tell the user and drop the message.
      await updateStatus(ctx, err.cacheKey, "FAILED", { error: err.message });
      await deleteMessage(ctx, message);
    }
    // Otherwise leave it: it will land in the DLQ for inspection.
    return;
  }

  const heartbeat = startHeartbeat(ctx, message);
  let workDir;
  try {
    workDir = await mkdtemp(path.join(ctx.cfg.workDir, "job-"));

    const claimed = await updateStatus(
      ctx,
      job.cacheKey,
      "PROCESSING",
      { startedAt: new Date().toISOString() },
      { onlyIfNotComplete: true },
    );
    if (!claimed) {
      log("warn", "Record missing or already complete, skipping", {
        cacheKey: job.cacheKey,
      });
      await deleteMessage(ctx, message);
      return;
    }

    log("info", "Job started", {
      cacheKey: job.cacheKey,
      task: job.task,
      attempt: receiveCount,
    });
    const outputKey = await runJob(ctx, job, workDir);

    await updateStatus(ctx, job.cacheKey, "COMPLETE", {
      outputKey,
      completedAt: new Date().toISOString(),
    });
    await deleteMessage(ctx, message);
    log("info", "Job complete", { cacheKey: job.cacheKey, outputKey });
  } catch (err) {
    heartbeat.stop();
    const permanent = err instanceof JobError && err.permanent;
    const exhausted = receiveCount >= ctx.cfg.maxAttempts;
    log("error", "Job failed", {
      cacheKey: job.cacheKey,
      error: err.message,
      permanent,
      attempt: receiveCount,
      willRetry: !permanent && !exhausted,
    });

    try {
      if (permanent || exhausted) {
        await updateStatus(ctx, job.cacheKey, "FAILED", {
          error: err.message.slice(0, 500),
        });
        await deleteMessage(ctx, message);
      } else {
        // Make the message available again after a short delay.
        await ctx.sqs.send(
          new ChangeMessageVisibilityCommand({
            QueueUrl: ctx.cfg.queueUrl,
            ReceiptHandle: message.ReceiptHandle,
            VisibilityTimeout: ctx.cfg.retryDelaySec,
          }),
        );
      }
    } catch (cleanupErr) {
      log("error", "Failed to record job failure", {
        cacheKey: job.cacheKey,
        error: cleanupErr.message,
      });
    }
  } finally {
    heartbeat.stop();
    if (workDir)
      await rm(workDir, { recursive: true, force: true }).catch(() => {});
  }
}

/* -------------------------------------------------------------------------- */
/* Main loop                                                                  */
/* -------------------------------------------------------------------------- */

function sleep(ms, signal) {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms);
    signal.addEventListener(
      "abort",
      () => {
        clearTimeout(timer);
        resolve();
      },
      { once: true },
    );
  });
}

export async function main() {
  const cfg = loadConfig();
  const ctx = {
    cfg,
    sqs: new SQSClient({}),
    s3: new S3Client({}),
    ddb: DynamoDBDocumentClient.from(new DynamoDBClient({})),
  };
  await mkdir(cfg.workDir, { recursive: true });

  if (cfg.allowPrivateHosts)
    log("warn", "ALLOW_PRIVATE_HOSTS is enabled: development use only");

  // ECS sends SIGTERM on stop/scale-in: stop polling, finish the current job.
  const shutdown = new AbortController();
  for (const signal of ["SIGTERM", "SIGINT"]) {
    process.on(signal, () => {
      log("info", `Received ${signal}, finishing the current job then exiting`);
      shutdown.abort();
    });
  }

  log("info", "Worker started", {
    queueUrl: cfg.queueUrl,
    node: process.version,
  });

  while (!shutdown.signal.aborted) {
    let response;
    try {
      response = await ctx.sqs.send(
        new ReceiveMessageCommand({
          QueueUrl: cfg.queueUrl,
          MaxNumberOfMessages: 1, // one job at a time per container
          WaitTimeSeconds: 20, // long polling
          VisibilityTimeout: cfg.visibilityTimeoutSec,
          AttributeNames: ["ApproximateReceiveCount"],
        }),
        { abortSignal: shutdown.signal },
      );
    } catch (err) {
      if (shutdown.signal.aborted) break;
      log("error", "ReceiveMessage failed", { error: err.message });
      await sleep(5000, shutdown.signal);
      continue;
    }

    for (const message of response.Messages ?? []) {
      await processMessage(ctx, message);
    }
  }

  log("info", "Worker stopped");
}

// Only start when run directly (so the module can be imported for tests).
if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href
) {
  main().catch((err) => {
    log("error", "Fatal error", { error: err.message, stack: err.stack });
    process.exit(1);
  });
}
