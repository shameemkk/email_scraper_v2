-- Create email_scrap_jobs table
CREATE TABLE IF NOT EXISTS email_scrap_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id VARCHAR(255) UNIQUE NOT NULL,
    url TEXT NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'processing', 'done', 'error')),
    emails JSONB DEFAULT '[]'::jsonb,
    facebook_urls JSONB DEFAULT '[]'::jsonb,
    error TEXT,
    crawled_urls JSONB DEFAULT '[]'::jsonb,
    pages_crawled INTEGER DEFAULT 0,
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_email_scrap_jobs_status ON email_scrap_jobs(status);
CREATE INDEX IF NOT EXISTS idx_email_scrap_jobs_job_id ON email_scrap_jobs(job_id);
CREATE INDEX IF NOT EXISTS idx_email_scrap_jobs_created_at ON email_scrap_jobs(created_at);

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to automatically update updated_at
CREATE TRIGGER update_email_scrap_jobs_updated_at 
    BEFORE UPDATE ON email_scrap_jobs 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

