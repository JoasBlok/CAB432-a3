import { spawn } from "child_process";
import ffmpegPath from "ffmpeg-static";

const proc = spawn(ffmpegPath, [
  "-i",
  "input.mp4",
  "-vn",
  "-acodec",
  "libmp3lame",
  "-b:a",
  "192k",
  "-y",
  "output.mp3",
]);
proc.on("close", (code) => console.log("Exited with code", code));
