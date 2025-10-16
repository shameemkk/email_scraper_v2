# Email Scraper API

A powerful Node.js API for extracting email addresses and Facebook URLs from websites using an internal job queue system with Supabase persistence.

## Features

- 🔍 **Email Extraction**: Finds email addresses from HTML content and mailto links
- 📘 **Facebook URL Detection**: Identifies Facebook profile and page URLs
- 🚀 **Internal Job Queue**: Fast in-memory job processing with concurrent workers
- 💾 **Supabase Integration**: Persistent storage for completed jobs
- 🌐 **Web Crawling**: Multi-page crawling within the same domain
- ⚡ **JavaScript Rendering**: Handles dynamic content with Playwright
- 🔄 **Concurrent Processing**: Multiple jobs processed simultaneously
- 📊 **Job Status Tracking**: Real-time job status and progress monitoring

## Prerequisites

- Node.js (v16 or higher)
- npm or yarn
- Supabase account (optional, for persistent storage)

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd email_scraper
```

2. Install dependencies:
```bash
npm install
```

3. Create a `.env` file in the root directory:
```env
PORT=3000
MAX_CONCURRENT_WORKERS=3
WORKER_BATCH_SIZE=3
RATE_LIMIT_DELAY=500
MAX_DEPTH=2
SUPABASE_URL=your_supabase_url
SUPABASE_ANON_KEY=your_supabase_anon_key
```

## Usage

### Start the Server

```bash
npm start
```

For development with auto-restart:
```bash
npm run dev
```

The API will be available at `http://localhost:3000`

### API Endpoints

#### 1. Queue a Job
```http
POST /extract-emails
Content-Type: application/json

{
  "url": "https://example.com"
}
```

**Response:**
```json
{
  "success": true,
  "message": "Job queued successfully",
  "job_id": "uuid-here",
  "status": "queued",
  "url": "https://example.com"
}
```

#### 2. Check Job Status
```http
GET /job/{job_id}
```

**Response:**
```json
{
  "success": true,
  "job": {
    "job_id": "uuid-here",
    "url": "https://example.com",
    "status": "done",
    "emails": ["contact@example.com", "info@example.com"],
    "facebook_urls": ["https://facebook.com/example"],
    "crawled_urls": ["https://example.com", "https://example.com/about"],
    "pages_crawled": 2,
    "created_at": "2024-01-01T00:00:00.000Z",
    "completed_at": "2024-01-01T00:01:00.000Z"
  }
}
```

#### 3. Get Completed Jobs
```http
GET /completed-jobs
```

#### 4. Health Check
```http
GET /health
```

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `PORT` | 3000 | Server port |
| `MAX_CONCURRENT_WORKERS` | 3 | Maximum concurrent jobs |
| `WORKER_BATCH_SIZE` | 3 | Jobs processed per batch |
| `RATE_LIMIT_DELAY` | 500 | Delay between batch cycles (ms) |
| `MAX_DEPTH` | 2 | Maximum crawl depth |
| `SUPABASE_URL` | - | Supabase project URL |
| `SUPABASE_ANON_KEY` | - | Supabase anonymous key |

## Job Statuses

- `queued`: Job is waiting to be processed
- `processing`: Job is currently being processed
- `done`: Job completed successfully
- `error`: Job failed with an error

## Architecture

### Internal Job Queue
- In-memory job storage for fast processing
- Concurrent job execution with worker pool
- Automatic job status management

### Supabase Integration
- Stores completed jobs for persistence
- Optional - works without Supabase
- Automatic job synchronization

### Crawling Strategy
1. **Cheerio First**: Fast HTTP requests for static content
2. **Playwright Fallback**: Full browser rendering for dynamic content
3. **Smart URL Discovery**: Focuses on navigation and common pages
4. **Rate Limiting**: Respects target websites with delays

## Error Handling

- Graceful error handling for network issues
- Automatic retry logic for failed requests
- Detailed error logging and reporting
- Job status tracking for debugging

## Rate Limiting

- Random delays between requests (200-800ms)
- Configurable batch processing delays
- Respectful crawling to avoid being blocked

## License

ISC

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## Support

For issues and questions, please open an issue on GitHub.
