FROM node:24-bookworm-slim

# ffmpeg (includes libmp3lame) and CA certificates for HTTPS downloads
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV NODE_ENV=production
WORKDIR /app

# Install dependencies first so this layer is cached between code changes
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY worker.js ./

# Run as the unprivileged user that ships with the node image
USER node

CMD ["node", "worker.mjs"]
