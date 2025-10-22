import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import { chromium } from 'playwright'; // Import Playwright directly
import * as cheerio from 'cheerio'; // Import Cheerio
import axios from 'axios'; // Import Axios for light HTTP requests
import { createClient } from '@supabase/supabase-js';
import { v4 as uuidv4 } from 'uuid';

const app = express();
const PORT = process.env.PORT || 3000;

// Configuration
// MAX_CONCURRENT_WORKERS now limits the number of SIMULTANEOUS Playwright/Cheerio jobs
const MAX_CONCURRENT_WORKERS = Math.max(1, parseInt(process.env.MAX_CONCURRENT_WORKERS, 10) || 180);
const rawWorkerBatchSize = parseInt(process.env.WORKER_BATCH_SIZE, 100);
const WORKER_BATCH_SIZE = Math.max(1, rawWorkerBatchSize || MAX_CONCURRENT_WORKERS);
const RATE_LIMIT_DELAY = Math.max(0, parseInt(process.env.RATE_LIMIT_DELAY, 10) || 150); // Shorter delay since we do fewer requests per job
const MAX_DEPTH = Math.max(1, parseInt(process.env.MAX_DEPTH, 10) || 2);
const SUBPAGE_CONCURRENCY = Math.max(1, parseInt(process.env.SUBPAGE_CONCURRENCY, 10) || 6); // Max secondary links in parallel per job
const PLAYWRIGHT_MAX_CONTEXTS = Math.max(
  75,
  parseInt(process.env.PLAYWRIGHT_MAX_CONTEXTS, 10) || MAX_CONCURRENT_WORKERS
);
const rawScrapeDelayMin = parseInt(process.env.SCRAPE_DELAY_MIN_MS, 75);
const rawScrapeDelayMax = parseInt(process.env.SCRAPE_DELAY_MAX_MS, 200);
const SCRAPE_DELAY_MIN_MS = Math.max(0, Number.isFinite(rawScrapeDelayMin) ? rawScrapeDelayMin : 200);
const SCRAPE_DELAY_MAX_MS = Math.max(
  SCRAPE_DELAY_MIN_MS,
  Number.isFinite(rawScrapeDelayMax) ? rawScrapeDelayMax : Math.max(SCRAPE_DELAY_MIN_MS, 800)
);
// This limit is less relevant now, as the job should only focus on finding the email
// const PER_INSTANCE_REQUEST_LIMIT = parseInt(process.env.PER_INSTANCE_REQUEST_LIMIT, 10) || 30; 
const REQUEST_TIMEOUT_MS = 10000; // 10 seconds for initial HTTP request

// Shared Playwright browser management for faster fallbacks
const PLAYWRIGHT_LAUNCH_ARGS = [
  '--disable-dev-shm-usage',
  '--disable-gpu',
  '--no-zygote',
  '--no-sandbox',
];

let sharedBrowserInstance = null;
let sharedBrowserPromise = null;

async function getSharedBrowser() {
  if (sharedBrowserInstance) {
    return sharedBrowserInstance;
  }

  if (!sharedBrowserPromise) {
    sharedBrowserPromise = chromium.launch({
      headless: true,
      args: PLAYWRIGHT_LAUNCH_ARGS,
    });
  }

  try {
    sharedBrowserInstance = await sharedBrowserPromise;
    sharedBrowserInstance.once('disconnected', () => {
      sharedBrowserInstance = null;
      sharedBrowserPromise = null;
    });
    return sharedBrowserInstance;
  } catch (error) {
    sharedBrowserPromise = null;
    throw error;
  }
}

async function resetSharedBrowser() {
  if (sharedBrowserInstance) {
    try {
      await sharedBrowserInstance.close();
    } catch (error) {
      console.error('[Playwright] Error closing shared browser:', error.message);
    }
  }
  sharedBrowserInstance = null;
  sharedBrowserPromise = null;
}

async function closeSharedBrowser() {
  if (sharedBrowserPromise || sharedBrowserInstance) {
    await resetSharedBrowser();
  }
}

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
// INTERNAL JOB QUEUE SYSTEM (Supabase-only implementation)
// =========================================================================

// --- NEW Supabase Table Name Constants ---
const COMPLETED_JOBS_TABLE = 'email_scrap_jobs'; // The table for completed jobs (unchanged)
const ACTIVE_JOBS_TABLE = 'email_queued_jobs'; // The new table for active/queued/processing jobs

class InternalJobQueue {
  constructor() {
    // In-memory storage (jobs, queue, processing set) removed as per user request.
    if (!supabase) {
        console.error("WARNING: Supabase is not configured. Job queue cannot operate without database storage.");
    }
  }

  // Helper to save a job to the new active jobs table (for queued/processing)
  async saveActiveJobToSupabase(job) {
    if (!supabase) throw new Error("Supabase not configured for active job storage.");

    try {
      const { error } = await supabase
        .from(ACTIVE_JOBS_TABLE) // <--- ACTIVE JOBS TABLE
        .upsert({
          job_id: job.job_id,
          url: job.url,
          status: job.status,
          created_at: job.created_at,
          updated_at: job.updated_at,
          started_at: job.started_at || null, 
          // Only store essential info for active jobs
        }, {
          onConflict: 'job_id'
        });

      if (error) {
        console.error(`Error saving active job ${job.job_id} to ${ACTIVE_JOBS_TABLE}:`, error);
        throw error;
      }

      console.log(`Active job ${job.job_id} saved/updated in ${ACTIVE_JOBS_TABLE}`);
    } catch (error) {
      console.error(`Failed to save active job to ${ACTIVE_JOBS_TABLE}:`, error);
      throw error;
    }
  }
  
  // Helper to delete a job from the new active jobs table
  async deleteActiveJobFromSupabase(jobId) {
    if (!supabase) return;

    try {
      const { error } = await supabase
        .from(ACTIVE_JOBS_TABLE) // <--- ACTIVE JOBS TABLE
        .delete()
        .eq('job_id', jobId);

      if (error) {
        console.error(`Error deleting active job ${jobId} from ${ACTIVE_JOBS_TABLE}:`, error);
        throw error;
      }

      console.log(`Active job ${jobId} deleted from ${ACTIVE_JOBS_TABLE}`);
    } catch (error) {
      console.error(`Failed to delete active job from ${ACTIVE_JOBS_TABLE}:`, error);
      throw error;
    }
  }


  // Create a new job - now only interacts with Supabase
  async createJob(jobId, url) {
    if (!supabase) throw new Error("Supabase is required for job creation.");
      
    const job = {
      job_id: jobId,
      url: url,
      status: 'queued',
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
      emails: [], // Not stored in ACTIVE_JOBS_TABLE, only in COMPLETED_JOBS_TABLE
      facebook_urls: [],
      crawled_urls: [],
      pages_crawled: 0,
      error: null,
      started_at: null,
      completed_at: null
    };

    // Save the initial job status to the ACTIVE_JOBS_TABLE
    await this.saveActiveJobToSupabase(job);
    
    console.log(`Job ${jobId} created and queued for URL: ${url}`);
    return job;
  }

  // Update job status and data - now fetches and saves directly to Supabase
  async updateJobStatus(jobId, status, updates = {}) {
    if (!supabase) throw new Error("Supabase is required for job status update.");
      
    // 1. Get the latest job record (needed to preserve all properties during an update)
    let job = await this.getJob(jobId);
    if (!job) {
      throw new Error(`Job ${jobId} not found in active or completed tables`);
    }

    // 2. Update job data in memory
    Object.assign(job, updates);
    job.status = status;
    job.updated_at = new Date().toISOString();

    if (status === 'processing' && !job.started_at) {
      job.started_at = new Date().toISOString();
      
      // Save status update to ACTIVE_JOBS_TABLE
      await this.saveActiveJobToSupabase(job);
    }
    
    if (status === 'done' || status === 'error') {
      job.completed_at = new Date().toISOString();
      
      // Save completed job to COMPLETED_JOBS_TABLE (email_scrap_jobs)
      await this.saveCompletedJobToSupabase(job);
      
      // Delete from ACTIVE_JOBS_TABLE after successful save to completed table
      await this.deleteActiveJobFromSupabase(jobId);
    }
    
    // For intermediate updates during processing, we update the ACTIVE_JOBS_TABLE
    if (status === 'processing') {
      await this.saveActiveJobToSupabase(job);
    }

    console.log(`Job ${jobId} status updated to: ${status}`);
    return job;
  }

  // Save completed job to Supabase (using the original table: email_scrap_jobs)
  async saveCompletedJobToSupabase(job) {
    if (!supabase) return;

    try {
      const { error } = await supabase
        .from(COMPLETED_JOBS_TABLE) // <--- ORIGINAL TABLE NAME (email_scrap_jobs)
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
        console.error(`Error saving job to ${COMPLETED_JOBS_TABLE}:`, error);
        throw error;
      }

      console.log(`Job ${job.job_id} saved to ${COMPLETED_JOBS_TABLE}`);
    } catch (error) {
      console.error(`Failed to save job to ${COMPLETED_JOBS_TABLE}:`, error);
      throw error;
    }
  }

  // Get job by ID - now searches both tables
  async getJob(jobId) {
    if (!supabase) return null;

    try {
      // 1. Check the completed jobs table (email_scrap_jobs) first
      let { data: completedJob, error: completedError } = await supabase
        .from(COMPLETED_JOBS_TABLE)
        .select('*')
        .eq('job_id', jobId)
        .single();

      if (completedError && completedError.code !== 'PGRST116') {
        console.error(`Error getting job from ${COMPLETED_JOBS_TABLE}:`, completedError);
      }
      
      if (completedJob) return completedJob;

      // 2. Check the active/queued jobs table (email_queued_jobs)
      let { data: activeJob, error: activeError } = await supabase
        .from(ACTIVE_JOBS_TABLE)
        .select('*')
        .eq('job_id', jobId)
        .single();
        
      if (activeError && activeError.code !== 'PGRST116') {
        console.error(`Error getting job from ${ACTIVE_JOBS_TABLE}:`, activeError);
      }

      return activeJob || null; // Job not found in either table

    } catch (supabaseError) {
      console.error('Failed to get job from Supabase:', supabaseError);
      return null; 
    }
  }

  // Get queued jobs (for worker processing) - now queries Supabase
  async getQueuedJobs(limit = WORKER_BATCH_SIZE) {
    if (!supabase) return [];
    
    try {
      const { data, error } = await supabase
        .from(ACTIVE_JOBS_TABLE)
        .select('*')
        .eq('status', 'queued')
        .order('created_at', { ascending: true }) // FIFO order
        .limit(limit);

      if (error) {
        console.error(`Error fetching queued jobs from ${ACTIVE_JOBS_TABLE}:`, error);
        return [];
      }
      
      return data || [];
    } catch (error) {
      console.error('Failed to fetch queued jobs:', error);
      return [];
    }
  }

  // No-op method: Queue is managed by DB status.
  removeFromQueue(jobId) {
    // console.log(`[JobQueue] removeFromQueue is a no-op for job ${jobId}`);
  }

  // Get queue statistics - now queries Supabase
  async getStats() {
    if (!supabase) {
      return { total: 0, queued: 0, processing: 0, done: 0, error: 0, message: 'Supabase not available' };
    }
    
    try {
      // Get detailed counts from active table
      const { data: activeData } = await supabase
        .from(ACTIVE_JOBS_TABLE)
        .select('status');

      const queued = activeData ? activeData.filter(j => j.status === 'queued').length : 0;
      const processing = activeData ? activeData.filter(j => j.status === 'processing').length : 0;
      
      // Get detailed counts from completed table
      const { data: completedData } = await supabase
        .from(COMPLETED_JOBS_TABLE)
        .select('status');

      const done = completedData ? completedData.filter(j => j.status === 'done').length : 0;
      const error = completedData ? completedData.filter(j => j.status === 'error').length : 0;
      
      return {
        total: (activeData?.length || 0) + (completedData?.length || 0),
        queued: queued,
        processing: processing,
        done: done,
        error: error
      };

    } catch (error) {
      console.error('Error fetching job statistics from Supabase:', error);
      return { total: 0, queued: 0, processing: 0, done: 0, error: 0, message: 'Database query failed' };
    }
  }
}

// Initialize the internal job queue
const jobQueue = new InternalJobQueue();

// Middleware
app.use(cors());
app.use(express.json());

// =========================================================================
// EMAIL & URL EXTRACTION FUNCTIONS (Kept as is - DO NOT CHANGE)
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

/**
 * Utility to run async tasks with a fixed concurrency limit.
 * @param {Array<any>} items
 * @param {number} limit
 * @param {(item: any, index: number) => Promise<void>} iteratee
 */
async function runWithConcurrency(items, limit, iteratee) {
  if (!Array.isArray(items) || items.length === 0) {
    return;
  }
  
  const normalizedLimit = Math.max(1, Number.isFinite(limit) ? limit : 1);
  let currentIndex = 0;
  
  const worker = async () => {
    while (currentIndex < items.length) {
      const index = currentIndex++;
      await iteratee(items[index], index);
    }
  };
  
  const workers = Array.from(
    { length: Math.min(normalizedLimit, items.length) },
    () => worker()
  );
  
  await Promise.all(workers);
}

class AsyncSemaphore {
  constructor(limit) {
    this.limit = Math.max(1, Number.isFinite(limit) ? limit : 1);
    this.active = 0;
    this.queue = [];
  }

  async acquire() {
    if (this.active < this.limit) {
      this.active++;
      return;
    }

    await new Promise(resolve => this.queue.push(resolve));
    this.active++;
  }

  release() {
    if (this.active > 0) {
      this.active--;
    }

    if (this.queue.length > 0 && this.active < this.limit) {
      const resolve = this.queue.shift();
      resolve();
    }
  }
}

const playwrightContextSemaphore = new AsyncSemaphore(PLAYWRIGHT_MAX_CONTEXTS);

// =========================================================================
// DATABASE HELPER FUNCTIONS (Updated to use the new async JobQueue methods)
// =========================================================================

async function createJob(jobId, url) {
  try {
    const job = await jobQueue.createJob(jobId, url);
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
    const job = await jobQueue.getJob(jobId);
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
 * @param {string} jobId - The parent job identifier for error reporting.
 * @returns {Promise<{emails: string[], facebookUrls: string[], newUrls: string[]}>}
 */
async function scrapeUrl(url, depth, visitedUrls, jobId) {
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

  // Apply configurable jitter to spread requests when crawling aggressively
  const scrapeDelay = SCRAPE_DELAY_MAX_MS > 0
    ? SCRAPE_DELAY_MIN_MS + Math.random() * (SCRAPE_DELAY_MAX_MS - SCRAPE_DELAY_MIN_MS)
    : 0;
  if (scrapeDelay > 0) {
    await new Promise(resolve => setTimeout(resolve, scrapeDelay));
  }

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
    let context;
    let page;
    let semaphoreAcquired = false;
    try {
      // Reuse a shared browser instance for faster startup
      browser = await getSharedBrowser();
      await playwrightContextSemaphore.acquire();
      semaphoreAcquired = true;
      // Create explicit context and page
      context = await browser.newContext();
      page = await context.newPage();
      
      // Set a strict timeout (e.g., 60 seconds for the full page load/render)
      await page.goto(url, { waitUntil: 'networkidle', timeout: 60000 }); 
      
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
      
      let emails = extractEmails(htmlContent);
      result.emails.push(...emails);
      
      // Extract links if we need to crawl deeper (and if emails weren't found)
      if (depth < MAX_DEPTH) {
        // ... (link extraction logic remains the same, assuming it doesn't need the page/context anymore) ...
        // NOTE: We rely on Cheerio/URL parsing of the retrieved `htmlContent` for link extraction if emails were not found.
        
        if (result.emails.length === 0) {
            const $ = cheerio.load(htmlContent);
            const links = $('a[href]')
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
                
            // Filter links to stay on the same domain and convert to absolute URLs
            const baseUrl = new URL(url);
            const sameDomainLinks = [];
            
            for (const link of links) {
                try {
                    const linkUrl = new URL(link);
                    // Only include links from the same origin
                    if (linkUrl.origin === baseUrl.origin) {
                      sameDomainLinks.push(link);
                    }
                } catch {
                    // Skip invalid URLs
                }
            }

            // Add specific common pages to crawl
            const commonPages = [ '/about', '/contact', '/about-us/', '/contact-us/', '/privacy', '/terms'];
            for (const pagePath of commonPages) {
                try {
                    const fullUrl = new URL(pagePath, url).href;
                    sameDomainLinks.push(fullUrl);
                } catch (e) { /* skip invalid URLs */ }
            }

            result.newUrls.push(...sameDomainLinks);
        }
      }
      
      console.log(`[Playwright] Found ${result.emails.length} emails and ${result.newUrls.length} new URLs.`);

    } catch (error) {
      console.error(`[Playwright Error] Failed to process ${url}:`, error);
      if (!browser || !browser.isConnected()) {
        await resetSharedBrowser();
      }
      // Update job in Supabase/internal queue with error status to avoid stuck processing
      if (jobId && typeof updateJobStatus === 'function') {
        try {
          await updateJobStatus(jobId, 'error', {
            error: `[Playwright Error] ${error && error.message ? error.message : 'Unknown error'}`
          });
        } catch (statusErr) {
          console.error('Failed to update job status after Playwright error:', statusErr);
        }
      }
      // Re-throw to let the caller fail the job and stop further processing
      throw error;
    } finally {
      if (page) {
        try {
          await page.close();
        } catch (closeErr) {
          console.error(`[Playwright] Failed to close page for ${url}: ${closeErr.message}`);
        }
      }
      if (context) {
        try {
          await context.close();
        } catch (closeErr) {
          console.error(`[Playwright] Failed to close context for ${url}: ${closeErr.message}`);
        }
      }
      if (semaphoreAcquired) {
        playwrightContextSemaphore.release();
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
// Worker function to process jobs (Unchanged)
// =========================================================================
async function processJob(jobId, url) {
  console.log(`Starting job ${jobId} for URL: ${url}`);
  
  try {
    // The worker already sets status to 'processing' before calling this function, 
    // but we can ensure it's recorded again if needed, or if this function is called directly.
    // await updateJobStatus(jobId, 'processing'); // Removing duplicate call here

    const uniqueEmails = new Set();
    const uniqueFacebookUrls = new Set();
    const visitedUrls = new Set();

    // Step 1: Scrape the primary URL (depth 0)
    const primaryResult = await scrapeUrl(url, 0, visitedUrls, jobId);
    primaryResult.emails.forEach(e => uniqueEmails.add(e));
    primaryResult.facebookUrls.forEach(f => uniqueFacebookUrls.add(f));

    // Step 2: Optionally scrape a limited set of same-domain links without using an in-memory queue
    if (MAX_DEPTH > 1) {
      const baseOrigin = new URL(url).origin;
      const candidateLinks = (primaryResult.newUrls || [])
        .filter(link => {
          try {
            const linkUrl = new URL(link);
            return linkUrl.origin === baseOrigin && !visitedUrls.has(link);
          } catch {
            return false;
          }
        })
        .slice(0, 15); // Respect previous crawl limit without a queue

      await runWithConcurrency(candidateLinks, SUBPAGE_CONCURRENCY, async (link) => {
        try {
          const subResult = await scrapeUrl(link, 1, visitedUrls, jobId);
          subResult.emails.forEach(e => uniqueEmails.add(e));
          subResult.facebookUrls.forEach(f => uniqueFacebookUrls.add(f));
        } catch (e) {
          console.error(`Error during scraping ${link}: ${e && e.message ? e.message : e}`);
          // Abort this job to avoid marking it as done after a fatal scraping failure
          throw e;
        }
      });
    }

    // Remove duplicates and prepare results
    const finalEmails = Array.from(uniqueEmails);
    const finalFacebookUrls = Array.from(uniqueFacebookUrls);

    // Update job with results and mark as done
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
// Background worker system (Unchanged)
// =========================================================================

class JobWorker {
  constructor() {
    this.isRunning = false;
    // activeJobs set is still useful to prevent multiple workers from picking up 
    // the same job *simultaneously* after the DB query.
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
      // Get queued jobs from internal queue (now an async Supabase query)
      const queuedJobs = await jobQueue.getQueuedJobs(WORKER_BATCH_SIZE);

      if (!queuedJobs || queuedJobs.length === 0) {
        return; // No jobs to process
      }

      // Filter out jobs currently being processed by *this* instance of the worker
      const jobsToProcess = queuedJobs.filter(job => !this.activeJobs.has(job.job_id));
      
      // Process jobs concurrently (limited by MAX_CONCURRENT_WORKERS)
      const availableWorkers = Math.max(0, MAX_CONCURRENT_WORKERS - this.activeJobs.size);
      if (availableWorkers <= 0) {
        return;
      }
      const finalJobsToProcess = jobsToProcess.slice(0, availableWorkers);

      if (finalJobsToProcess.length === 0) return; // No jobs or no available workers

      const jobPromises = finalJobsToProcess.map(async (job) => {
        this.activeJobs.add(job.job_id);
        
        try {
          // 1. Immediately update status to 'processing' in DB
          // This is the critical step to prevent other workers/batches from picking it up
          await updateJobStatus(job.job_id, 'processing', {
              started_at: new Date().toISOString()
          });
          
          // 2. Execute the heavy lifting
          await processJob(job.job_id, job.url);
          
        } catch(error) {
          console.error(`Error during Job ${job.job_id} in worker loop:`, error.message);
          // processJob already handles error status update inside it
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
// ENDPOINTS (Unchanged)
// =========================================================================

// Modified /extract-emails endpoint - now adds jobs to queue
app.post('/extract-emails', async (req, res) => {
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

    // Create job in database (now an async call)
    const job = await createJob(jobId, url);

    // Ensure background worker is running (self-healing)
    if (!jobWorker.isRunning) {
      console.log('Worker was stopped, restarting...');
      jobWorker.start().catch(err => console.error('Failed to restart worker:', err));
    }

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
app.get('/job/:jobId', async (req, res) => {
  try {
    const { jobId } = req.params;
    
    // getJob is now async
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
      .from(COMPLETED_JOBS_TABLE) // <--- USING COMPLETED JOBS TABLE
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
app.get('/health', async (req, res) => {
  const queueStats = await jobQueue.getStats(); // AWAIT the new async method
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
    message: 'Email and Facebook URL Extraction API (Supabase Queue-based)',
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
      'Supabase-only job queue system (no internal memory state)',
      'Active jobs stored in: email_queued_jobs',
      'Completed jobs stored in: email_scrap_jobs',
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
      console.log(`Playwright contexts limit: ${PLAYWRIGHT_MAX_CONTEXTS}, subpage concurrency: ${SUBPAGE_CONCURRENCY}`);
      console.log(`Scrape delay window: ${SCRAPE_DELAY_MIN_MS}-${SCRAPE_DELAY_MAX_MS}ms`);
    });
    
  } catch (error) {
    console.error('Failed to start server:', error);
    await closeSharedBrowser();
    process.exit(1);
  }
}

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('Shutting down gracefully...');
  await jobWorker.stop();
  await closeSharedBrowser();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('Shutting down gracefully...');
  await jobWorker.stop();
  await closeSharedBrowser();
  process.exit(0);
});

startServer();
