-- Add logo and website columns to the brands table
ALTER TABLE brands 
ADD COLUMN IF NOT EXISTS logo_url TEXT,
ADD COLUMN IF NOT EXISTS website_url TEXT;
