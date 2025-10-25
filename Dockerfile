# Use Node.js 20 LTS as base image
FROM node:20-slim

# Install system dependencies for Playwright
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    ca-certificates \
    procps \
    libxss1 \
    libgconf-2-4 \
    libxrandr2 \
    libasound2 \
    libpangocairo-1.0-0 \
    libatk1.0-0 \
    libcairo-gobject2 \
    libgtk-3-0 \
    libgdk-pixbuf2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Copy package files
COPY package*.json ./

ENV NODE_OPTIONS="--max-old-space-size=30720"

# Install npm dependencies and Playwright artifacts
RUN npm install && \
    npx playwright install-deps && \
    npx playwright install chromium

# Copy application code
COPY . .

# Expose port
EXPOSE 3000

# Start the application with PM2 Runtime (preserves worker on crashes)
CMD ["npx", "pm2-runtime", "--max-restarts", "5", "index.js"]
