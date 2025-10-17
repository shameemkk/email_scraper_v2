import 'dotenv/config';
import express from 'express';
import cors from 'cors';
// REMOVE: import { AdaptivePlaywrightCrawler } from 'crawlee'; // Remove Crawlee dependency
import { chromium } from 'playwright'; // Import Playwright directly
import * as cheerio from 'cheerio'; // Import Cheerio
import axios from 'axios'; // Import Axios for light HTTP requests
import { createClient } from '@supabase/supabase-js';
import { v4 as uuidv4 } from 'uuid';

// Disable Crawlee persistent storage - NOW UNNECESSARY but harmless
// process.env.CRAWLEE_PERSIST_STORAGE = 'false';

const app = express();
const PORT = process.env.PORT || 3000;

// Configuration
// MAX_CONCURRENT_WORKERS now limits the number of SIMULTANEOUS Playwright/Cheerio jobs
const MAX_CONCURRENT_WORKERS = parseInt(process.env.MAX_CONCURRENT_WORKERS) || 6; 
const WORKER_BATCH_SIZE = parseInt(process.env.WORKER_BATCH_SIZE) || 6;
const RATE_LIMIT_DELAY = parseInt(process.env.RATE_LIMIT_DELAY) || 500; // Shorter delay since we do fewer requests per job
const MAX_DEPTH = parseInt(process.env.MAX_DEPTH) || 2;
// This limit is less relevant now, as the job should only focus on finding the email
// const PER_INSTANCE_REQUEST_LIMIT = parseInt(process.env.PER_INSTANCE_REQUEST_LIMIT) || 30; 
const REQUEST_TIMEOUT_MS = 10000; // 10 seconds for initial HTTP request

// Supabase configuration (for storing completed jobs only)
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_ANON_KEY;

let supabase = null;
if (supabaseUrl && supabaseKey) {
  supabase = createClient(supabaseUrl, supabaseKey);
  console.log('Supabase connected for completed job storage');
} else {
  console.log('Supabase not configured - completed jobs will only be stored in memory');
}

// =========================================================================
// INTERNAL JOB QUEUE SYSTEM
// =========================================================================

class InternalJobQueue {
  constructor() {
    this.jobs = new Map(); // jobId -> job data
    this.queue = []; // Array of job IDs in processing order
    this.processing = new Set(); // Set of job IDs currently being processed
  }

  // Create a new job
  createJob(jobId, url) {
    const job = {
      job_id: jobId,
      url: url,
      status: 'queued',
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
      emails: [],
      facebook_urls: [],
      crawled_urls: [],
      pages_crawled: 0,
      error: null,
      started_at: null,
      completed_at: null
    };

    this.jobs.set(jobId, job);
    this.queue.push(jobId);
    
    console.log(`Job ${jobId} created and queued for URL: ${url}`);
    return job;
  }

  // Update job status and data
  async updateJobStatus(jobId, status, updates = {}) {
    const job = this.jobs.get(jobId);
    if (!job) {
      throw new Error(`Job ${jobId} not found`);
    }

    // Update job data
    Object.assign(job, updates);
    job.status = status;
    job.updated_at = new Date().toISOString();

    if (status === 'processing' && !updates.started_at) {
      job.started_at = new Date().toISOString();
      this.processing.add(jobId);
    }

    if (status === 'done' || status === 'error') {
      job.completed_at = new Date().toISOString();
      this.processing.delete(jobId);
      
      // Save completed job to Supabase if available
      if (supabase) {
        try {
          await this.saveCompletedJobToSupabase(job);
        } catch (error) {
          console.error(`Failed to save completed job ${jobId} to Supabase:`, error);
        }
      }
    }

    console.log(`Job ${jobId} status updated to: ${status}`);
    return job;
  }

  // Save completed job to Supabase
  async saveCompletedJobToSupabase(job) {
    if (!supabase) return;

    try {
      const { error } = await supabase
        .from('email_scrap_jobs')
        .upsert({
          job_id: job.job_id,
          url: job.url,
          status: job.status,
          emails: job.emails || [],
          facebook_urls: job.facebook_urls || [],
          crawled_urls: job.crawled_urls || [],
          pages_crawled: job.pages_crawled || 0,
          error: job.error,
          created_at: job.created_at,
          updated_at: job.updated_at,
          started_at: job.started_at,
          completed_at: job.completed_at
        }, {
          onConflict: 'job_id'
        });

      if (error) {
        console.error('Error saving job to Supabase:', error);
        throw error;
      }

      console.log(`Job ${job.job_id} saved to Supabase`);
    } catch (error) {
      console.error('Failed to save job to Supabase:', error);
      throw error;
    }
  }

  // Get job by ID
  getJob(jobId) {
    return this.jobs.get(jobId) || null;
  }

  // Get queued jobs (for worker processing)
  getQueuedJobs(limit = WORKER_BATCH_SIZE) {
    const queuedJobIds = this.queue.filter(jobId => {
      const job = this.jobs.get(jobId);
      return job && job.status === 'queued';
    });

    return queuedJobIds.slice(0, limit).map(jobId => this.jobs.get(jobId));
  }

  // Remove job from queue when processing starts
  removeFromQueue(jobId) {
    const index = this.queue.indexOf(jobId);
    if (index > -1) {
      this.queue.splice(index, 1);
    }
  }

  // Get queue statistics
  getStats() {
    const stats = {
      total: this.jobs.size,
      queued: 0,
      processing: this.processing.size,
      done: 0,
      error: 0
    };

    for (const job of this.jobs.values()) {
      switch (job.status) {
        case 'queued':
          stats.queued++;
          break;
        case 'done':
          stats.done++;
          break;
        case 'error':
          stats.error++;
          break;
      }
    }

    return stats;
  }
}

// Initialize the internal job queue
const jobQueue = new InternalJobQueue();

// Middleware
app.use(cors());
app.use(express.json());

// =========================================================================
// EMAIL & URL EXTRACTION FUNCTIONS (Kept as is)
// =========================================================================

// Email extraction function - works with HTML content
function extractEmails(html) {
  const emails = [];
  
  // Extract emails from mailto: href attributes
  const mailtoRegex = /href=["']mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})["']/gi;
  const mailtoMatches = html.matchAll(mailtoRegex);
  for (const match of mailtoMatches) {
    emails.push(match[1]);
  }
  
  // Extract emails wrapped in HTML tags (like font, b, span, etc.)
  const htmlEmailRegex = /<[^>]*>([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})<\/[^>]*>/gi;
  const htmlEmailMatches = html.matchAll(htmlEmailRegex);
  for (const match of htmlEmailMatches) {
    emails.push(match[1]);
  }
  
  // Extract emails from plain text (after removing HTML tags)
  const textContent = html.replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
  const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
  const textEmails = textContent.match(emailRegex) || [];
  emails.push(...textEmails);
  
  return [...new Set(emails)]; // Remove duplicates
}

// Facebook URL extraction function - improved to avoid false links
function extractFacebookUrls(text) {
  // More precise Facebook URL regex that avoids false matches
  const facebookRegex = /(?:https?:\/\/)?(?:www\.)?(?:facebook\.com|fb\.com)\/(?:profile\.php\?id=\d+|pages\/[a-zA-Z0-9._-]+|groups\/[a-zA-Z0-9._-]+|[a-zA-Z0-9._-]{2,})(?:\/[a-zA-Z0-9._-]+)*/gi;
  const facebookUrls = text.match(facebookRegex) || [];
  
  // Filter out common false positives and clean URLs
  const filteredUrls = facebookUrls.filter(url => {
    // Clean the URL and remove any trailing slashes or unwanted characters
    let cleanUrl = url.replace(/^https?:\/\//, '').replace(/^www\./, '');
    cleanUrl = cleanUrl.replace(/\/+$/, ''); // Remove trailing slashes
    cleanUrl = cleanUrl.replace(/\\+/g, ''); // Remove backslashes
    
    const pathPart = cleanUrl.split('/')[1] || '';
    
    // Skip if path is too short (likely not a real profile/page)
    if (pathPart.length < 2) return false;
    
    // Skip common non-profile paths
    const skipPatterns = ['home', 'login', 'register', 'help', 'privacy', 'terms', 'cookies', 'settings'];
    if (skipPatterns.includes(pathPart.toLowerCase())) return false;
    
    // Skip URLs with backslashes or other invalid characters
    if (cleanUrl.includes('\\') || cleanUrl.includes('//')) return false;
    
    return true;
  });
  
  // Clean up the final URLs
  const finalUrls = filteredUrls.map(url => {
    return url.replace(/\\+/g, '').replace(/\/+$/, '');
  });
  
  return [...new Set(finalUrls)]; // Remove duplicates
}

// URL cleaning function to remove hash fragments
function cleanUrl(url) {
  try {
    const urlObj = new URL(url);
    urlObj.hash = ''; // Remove the hash fragment
    return urlObj.href;
  } catch (error) {
    // If URL parsing fails, try simple string replacement
    return url.split('#')[0];
  }
}

// =========================================================================
// DATABASE HELPER FUNCTIONS (Replaced with internal queue operations)
// =========================================================================

async function createJob(jobId, url) {
  try {
    const job = jobQueue.createJob(jobId, url);
    return job;
  } catch (error) {
    console.error('Failed to create job:', error);
    throw error;
  }
}

async function updateJobStatus(jobId, status, updates = {}) {
  try {
    const job = await jobQueue.updateJobStatus(jobId, status, updates);
    return job;
  } catch (error) {
    console.error('Failed to update job:', error);
    throw error;
  }
}

async function getJob(jobId) {
  try {
    // First check internal queue
    let job = jobQueue.getJob(jobId);
    
    // If not found in internal queue and Supabase is available, check Supabase
    if (!job && supabase) {
      try {
        const { data, error } = await supabase
          .from('email_scrap_jobs')
          .select('*')
          .eq('job_id', jobId)
          .single();

        if (error) {
          if (error.code === 'PGRST116') {
            return null; // Job not found
          }
          console.error('Error getting job from Supabase:', error);
          throw error;
        }

        job = data;
      } catch (supabaseError) {
        console.error('Failed to get job from Supabase:', supabaseError);
        // Continue with null if Supabase fails
      }
    }
    
    return job;
  } catch (error) {
    console.error('Failed to get job:', error);
    throw error;
  }
}

// =========================================================================
// NEW: Playwright/Cheerio Core Functions
// =========================================================================

/**
 * Scrapes a single URL using the Cheerio-first, Playwright-fallback strategy.
 * @param {string} url - The URL to scrape.
 * @param {number} depth - The current crawl depth (for recursive calls).
 * @param {Set<string>} visitedUrls - Set of URLs already visited in this job.
 * @returns {Promise<{emails: string[], facebookUrls: string[], newUrls: string[]}>}
 */
async function scrapeUrl(url, depth, visitedUrls) {
  const result = {
    emails: [],
    facebookUrls: [],
    newUrls: [],
  };

  if (visitedUrls.has(url)) {
    return result;
  }
  visitedUrls.add(url);
  
  console.log(`[Depth ${depth}] Processing URL: ${url}`);

  // Add random delay to avoid being blocked (200-800ms)
  await new Promise(resolve => setTimeout(resolve, Math.random() * 600 + 200));

  let htmlContent = '';
  let isJsRendered = false;

  // --- 1. Cheerio-First (Light/Fast HTTP Request) ---
  try {
    const response = await axios.get(url, {
      timeout: REQUEST_TIMEOUT_MS,
      validateStatus: (status) => status >= 200 && status < 400, // Only proceed on success
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
      },
      maxRedirects: 5,
      decompress: true, // Automatically decompress gzipped responses
    });
    
    htmlContent = response.data;
    
    // Attempt extraction with Cheerio
    let emails = extractEmails(htmlContent);

    if (emails.length > 0) {
      result.emails.push(...emails);
      console.log(`[Cheerio] Found ${emails.length} emails. Job successful.`);
      
      // If emails found, only extract navigation links for quick domain coverage
      const $ = cheerio.load(htmlContent);
      const baseUrl = new URL(url);
      const navLinks = $('nav a, header a, .navbar a, .nav a, .navigation a, .menu a, .main-menu a, .primary-menu a, .top-menu a, [role="navigation"] a, .site-nav a, .main-nav a')
        .map((i, el) => {
          const href = $(el).attr('href');
          if (!href || href.startsWith('#') || href.startsWith('javascript:')) return null;
          try {
            // Convert to absolute URL and clean hash fragments
            const absoluteUrl = new URL(href, url).href;
            return cleanUrl(absoluteUrl);
          } catch {
            return null;
          }
        })
        .get()
        .filter(href => href !== null);
      
      result.newUrls.push(...navLinks);
      return result; // Early exit if successful with Cheerio
    }

  } catch (error) {
    console.log(`[Cheerio Fallback] HTTP request or initial Cheerio attempt failed for ${url}: ${error.message}`);
    isJsRendered = true; // Assume JS rendering or a temporary error
  }


  // --- 2. Playwright-Fallback (Full Browser Rendering) ---
  if (isJsRendered || result.emails.length === 0) {
    console.log(`[Playwright] Falling back to browser rendering for ${url}...`);
    let browser;
    try {
      // Launch Playwright for THIS specific job request (Playwright Launcher)
      browser = await chromium.launch({ headless: true });
      const page = await browser.newPage();
      
      // Set a strict timeout (e.g., 30 seconds for the full page load/render)
      await page.goto(url, { waitUntil: 'networkidle', timeout: 30000 }); 
      
      // Wait for page to be fully loaded and any dynamic content to render
      await page.waitForLoadState('networkidle');
      
      // Additional wait for any lazy-loaded content or dynamic elements
      await page.waitForTimeout(1000);
      
      // Try to wait for common email-related elements to be present
      try {
        await page.waitForSelector('a[href*="mailto:"] ', { timeout: 1000 });
      } catch (e) {
        console.log('[Playwright] No email-related selectors found, continuing...');
      } 

      htmlContent = await page.content();
      
      // Debug logging to see what content was loaded
      console.log(`[Playwright] HTML content length: ${htmlContent.length}`);
      console.log(`[Playwright] Contains mailto: ${htmlContent.includes('mailto:')}`);
      console.log(`[Playwright] Contains angelcityclean: ${htmlContent.includes('angelcityclean')}`);
      if (htmlContent.includes('mailto:')) {
        const mailtoMatches = htmlContent.match(/href=["']mailto:([^"']+)["']/gi);
        console.log(`[Playwright] Found mailto links:`, mailtoMatches);
      }
      
      let emails = extractEmails(htmlContent);
      result.emails.push(...emails);
      
      // Extract links if we need to crawl deeper (and if emails weren't found)
      if (depth < MAX_DEPTH) {
        const links = await page.evaluate(() => {
          const links = [];
          document.querySelectorAll('a[href]').forEach(el => {
            const href = el.getAttribute('href');
            if (href && !href.startsWith('#') && !href.startsWith('javascript:')) {
              links.push(href);
            }
          });
          return links;
        });
        
        // Filter links to stay on the same domain and convert to absolute URLs
        const baseUrl = new URL(url);
        const sameDomainLinks = [];
        
        for (const link of links) {
          try {
            const absoluteUrl = new URL(link, url).href; // Convert to absolute URL using the current page URL
            const cleanAbsoluteUrl = cleanUrl(absoluteUrl); // Remove hash fragments
            const linkUrl = new URL(cleanAbsoluteUrl);
            // Only include links from the same origin
            if (linkUrl.origin === baseUrl.origin) {
              sameDomainLinks.push(cleanAbsoluteUrl);
            }
          } catch {
            // Skip invalid URLs
          }
        }

        // Add specific common pages to crawl
        const commonPages = [ '/about', '/contact', '/about-us/', '/contact-us/'];
        for (const page of commonPages) {
          try {
            const fullUrl = new URL(page, url).href; // Use 'url' not 'baseUrl.origin'
            sameDomainLinks.push(fullUrl);
          } catch (e) { /* skip invalid URLs */ }
        }

        result.newUrls.push(...sameDomainLinks);
      }
      
      console.log(`[Playwright] Found ${result.emails.length} emails and ${result.newUrls.length} new URLs.`);

    } catch (error) {
      console.error(`[Playwright Error] Failed to process ${url}: ${error.message}`);
    } finally {
      if (browser) {
        await browser.close();
      }
    }
  }


  // --- 3. Final Check & Fallback Extraction (if no emails found) ---
  if (result.emails.length === 0) {
    // Only extract Facebook URLs if no emails were found on the primary page
    result.facebookUrls.push(...extractFacebookUrls(htmlContent));
  }
  
  return result;
}


// =========================================================================
// Worker function to process jobs (REPLACED)
// =========================================================================
async function processJob(jobId, url) {
  console.log(`Starting job ${jobId} for URL: ${url}`);
  
  try {
    await updateJobStatus(jobId, 'processing');

    const uniqueEmails = new Set();
    const uniqueFacebookUrls = new Set();
    const visitedUrls = new Set();
    
    // In-memory queue for this job's internal crawl
    const urlQueue = [url]; 
    let crawlCount = 0;
    
    // Process the queue iteratively, respecting MAX_DEPTH
    while (urlQueue.length > 0) {
      const currentUrl = urlQueue.shift();
      
      // NOTE: We only track depth on the main URL and common pages.
      const currentDepth = (currentUrl === url || currentUrl.includes('/contact') || currentUrl.includes('/about')) ? 0 : 1;
      
      // Stop crawling if MAX_DEPTH is reached
      if (currentDepth >= MAX_DEPTH) {
        continue;
      }
      
      // Ensure we don't exceed a reasonable request limit for a single job
      if (crawlCount > 15) { // Arbitrary limit, can be set in config
         console.log(`Job ${jobId} hit internal crawl limit of 15.`);
         break;
      }
      
      try {
        const scrapeResult = await scrapeUrl(currentUrl, currentDepth, visitedUrls);
        crawlCount++;
        
        // Add results
        scrapeResult.emails.forEach(e => uniqueEmails.add(e));
        scrapeResult.facebookUrls.forEach(f => uniqueFacebookUrls.add(f));

        // Enqueue new links
        scrapeResult.newUrls
        .filter(link => {
            // Simple same-domain check before adding to queue
            try {
                const linkUrl = new URL(link);
                const baseUrl = new URL(url);
                return linkUrl.origin === baseUrl.origin && !visitedUrls.has(link);
            } catch {
                return false;
            }
        })
        .forEach(link => {
            if (!urlQueue.includes(link)) { // Prevent duplicates in the queue
                urlQueue.push(link);
            }
        });
        
      } catch (e) {
        console.error(`Error during scraping ${currentUrl}: ${e.message}`);
      }
    }

    // Remove duplicates and prepare results
    const finalEmails = Array.from(uniqueEmails);
    const finalFacebookUrls = Array.from(uniqueFacebookUrls);

    // Update job with results
    await updateJobStatus(jobId, 'done', {
      emails: finalEmails,
      facebook_urls: finalFacebookUrls,
      crawled_urls: Array.from(visitedUrls),
      pages_crawled: visitedUrls.size
    });

    console.log(`Completed job ${jobId}: Found ${finalEmails.length} emails and ${finalFacebookUrls.length} Facebook URLs`);

  } catch (error) {
    console.error(`Job ${jobId} failed:`, error);
    
    // Update job with error
    await updateJobStatus(jobId, 'error', {
      error: error.message || 'Unknown error occurred'
    });
  }
}

// =========================================================================
// Background worker system (Kept as is - now uses the custom processJob)
// =========================================================================

class JobWorker {
  constructor() {
    this.isRunning = false;
    this.activeJobs = new Set();
  }

  async start() {
    if (this.isRunning) return;
    
    this.isRunning = true;
    console.log('Job worker started');
    
    // Process jobs continuously
    while (this.isRunning) {
      try {
        await this.processNextBatch();
        // Wait before checking for more jobs
        await new Promise(resolve => setTimeout(resolve, RATE_LIMIT_DELAY)); // Use RATE_LIMIT_DELAY
      } catch (error) {
        console.error('Worker error:', error);
        await new Promise(resolve => setTimeout(resolve, 5000));
      }
    }
  }

  async stop() {
    this.isRunning = false;
    console.log('Job worker stopped');
  }

  async processNextBatch() {
    try {
      // Get queued jobs from internal queue
      const queuedJobs = jobQueue.getQueuedJobs(WORKER_BATCH_SIZE);

      if (!queuedJobs || queuedJobs.length === 0) {
        return; // No jobs to process
      }

      // Process jobs concurrently (limited by MAX_CONCURRENT_WORKERS)
      // Only process up to the limit of workers that are currently free
      const availableWorkers = MAX_CONCURRENT_WORKERS - this.activeJobs.size;
      const jobsToProcess = queuedJobs.slice(0, availableWorkers);

      if (jobsToProcess.length === 0) return; // No available workers

      const jobPromises = jobsToProcess.map(async (job) => {
        if (this.activeJobs.has(job.job_id)) {
          return; // Double-check, should not happen if logic is correct
        }

        this.activeJobs.add(job.job_id);
        
        try {
          // Remove job from queue when starting processing
          jobQueue.removeFromQueue(job.job_id);
          await processJob(job.job_id, job.url);
        } finally {
          this.activeJobs.delete(job.job_id);
        }
      });

      await Promise.all(jobPromises);

    } catch (error) {
      console.error('Error in processNextBatch:', error);
    }
  }
}

// Initialize worker
const jobWorker = new JobWorker();

// =========================================================================
// ENDPOINTS (Kept as is)
// =========================================================================

// Modified /extract-emails endpoint - now adds jobs to queue
app.post('/extract-emails', async (req, res) => { /* ... (Endpoint implementation) ... */
  try {
    const { url } = req.body;
    
    if (!url) {
      return res.status(400).json({ 
        error: 'URL is required',
        message: 'Please provide a URL in the request body'
      });
    }

    // Validate URL format
    try {
      new URL(url);
    } catch (error) {
      return res.status(400).json({ 
        error: 'Invalid URL format',
        message: 'Please provide a valid URL'
      });
    }

    // Generate unique job ID
    const jobId = uuidv4();

    // Create job in database
    const job = await createJob(jobId, url);

    res.json({
      success: true,
      message: 'Job queued successfully',
      job_id: jobId,
      status: 'queued',
      url: url
    });

  } catch (error) {
    console.error('Error queuing job:', error);
    res.status(500).json({
      error: 'Internal server error',
      message: 'Failed to queue the job'
    });
  }
});

// New endpoint to check job status
app.get('/job/:jobId', async (req, res) => { /* ... (Endpoint implementation) ... */
  try {
    const { jobId } = req.params;
    
    const job = await getJob(jobId);
    
    if (!job) {
      return res.status(404).json({
        error: 'Job not found',
        message: 'The specified job ID does not exist'
      });
    }

    res.json({
      success: true,
      job: {
        job_id: job.job_id,
        url: job.url,
        status: job.status,
        emails: job.emails || [],
        facebook_urls: job.facebook_urls || [],
        crawled_urls: job.crawled_urls || [],
        pages_crawled: job.pages_crawled || 0,
        error: job.error,
        created_at: job.created_at,
        updated_at: job.updated_at,
        started_at: job.started_at,
        completed_at: job.completed_at
      }
    });

  } catch (error) {
    console.error('Error getting job:', error);
    res.status(500).json({
      error: 'Internal server error',
      message: 'Failed to retrieve job information'
    });
  }
});

// Get all completed jobs from Supabase
app.get('/completed-jobs', async (req, res) => {
  try {
    if (!supabase) {
      return res.status(503).json({
        error: 'Supabase not configured',
        message: 'Completed jobs storage is not available'
      });
    }

    const { data, error } = await supabase
      .from('email_scrap_jobs')
      .select('*')
      .in('status', ['done', 'error'])
      .order('completed_at', { ascending: false })
      .limit(100); // Limit to last 100 completed jobs

    if (error) {
      console.error('Error fetching completed jobs:', error);
      return res.status(500).json({
        error: 'Database error',
        message: 'Failed to retrieve completed jobs'
      });
    }

    res.json({
      success: true,
      jobs: data || [],
      count: data ? data.length : 0
    });

  } catch (error) {
    console.error('Error getting completed jobs:', error);
    res.status(500).json({
      error: 'Internal server error',
      message: 'Failed to retrieve completed jobs'
    });
  }
});

// Health check endpoint
app.get('/health', (req, res) => {
  const queueStats = jobQueue.getStats();
  res.json({ 
    status: 'OK', 
    message: 'Email extraction API is running',
    worker_status: jobWorker.isRunning ? 'running' : 'stopped',
    active_jobs: jobWorker.activeJobs.size,
    queue_stats: queueStats,
    supabase_connected: supabase !== null
  });
});

// Root endpoint
app.get('/', (req, res) => {
  res.json({
    message: 'Email and Facebook URL Extraction API (Internal Queue-based)',
    endpoints: {
      'POST /extract-emails': 'Queue a job to extract emails and Facebook URLs from a website',
      'GET /job/:jobId': 'Check the status and results of a specific job',
      'GET /completed-jobs': 'Get all completed jobs from Supabase storage',
      'GET /health': 'Health check with queue statistics'
    },
    usage: {
      method: 'POST',
      url: '/extract-emails',
      body: { url: 'https://example.com' }
    },
    features: [
      'Internal job queue system (no external dependencies)',
      'Extract email addresses',
      'Extract Facebook URLs',
      'Crawl multiple pages within same domain',
      'Handle JavaScript-rendered content',
      'Concurrent job processing with rate limiting',
      'Job status tracking',
      'Error handling and retry logic'
    ]
  });
});

// Initialize and start the server
async function startServer() {
  try {
    // Start the job worker (this will run in background)
    jobWorker.start().catch(error => {
      console.error('Worker failed to start:', error);
    });
    
    app.listen(PORT, () => {
      console.log(`Email extraction API running on port ${PORT}`);
      console.log(`Visit http://localhost:${PORT} for API documentation`);
      console.log(`Worker system: ${MAX_CONCURRENT_WORKERS} concurrent workers, batch size: ${WORKER_BATCH_SIZE}`);
    });
    
  } catch (error) {
    console.error('Failed to start server:', error);
    process.exit(1);
  }
}

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('Shutting down gracefully...');
  await jobWorker.stop();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('Shutting down gracefully...');
  await jobWorker.stop();
  process.exit(0);
});

startServer();