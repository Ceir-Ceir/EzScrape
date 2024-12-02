import asyncio
import httpx
import re
import json
from typing import List, Dict, Set
from urllib.parse import urlencode, quote
import logging
import aiofiles
import os
from datetime import datetime
from itertools import product
import math
import time
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f"scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ],
)

SCRAPINGANT_API_KEY = "c2c798dce3e8473ea03f48b10f7c2d87"
SCRAPINGANT_BASE_URL = "https://api.scrapingant.com/v2/general"
OUTPUT_FILE = "all_jobs.csv"
NUM_WORKERS = 5

class BatchJobScraper:
    def __init__(self):
        self.processed_combinations: Set[tuple] = set()
        self.lock = asyncio.Lock()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive"
        }

    def read_file_content(self, filename: str) -> List[str]:
        try:
            with open(filename, "r", encoding="utf-8") as file:
                lines = []
                for line in file:
                    if line.strip():
                        if filename.lower().endswith("cities.txt"):
                            parts = line.strip().split('\t')
                            if len(parts) > 1:
                                lines.append(parts[1].strip())
                            else:
                                parts = line.strip().split()
                                if len(parts) > 1:
                                    lines.append(' '.join(parts[1:]).strip())
                        else:
                            lines.append(line.strip())
                
                logging.info(f"Read {len(lines)} entries from {filename}")
                return lines
        except Exception as e:
            logging.error(f"Error reading {filename}: {str(e)}")
            return []

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError))
)
    async def fetch_with_retries(self, url: str, client: httpx.AsyncClient, max_retries=3) -> str:
        for attempt in range(max_retries):
            try:
                # Properly encode the URL for ScrapingAnt
                encoded_url = quote(url, safe=':/?=&')
                
                params = {
                    "url": encoded_url,
                    "api_key": SCRAPINGANT_API_KEY,
                    "browser": "true",
                    "proxy_country": "US"
                }
                
                async with self.lock:  # Rate limiting
                    await asyncio.sleep(1)
                    try:
                        response = await client.get(
                            SCRAPINGANT_BASE_URL,
                            params=params,
                            headers=self.headers,
                            timeout=30.0
                        )
                        
                        # Log the full response for debugging
                        logging.info(f"Response status: {response.status_code}")
                        if response.status_code != 200:
                            logging.error(f"Response content: {response.text}")
                        
                        if response.status_code == 200:
                            return response.text
                        elif response.status_code == 422:
                            logging.error(f"Invalid request format. URL: {encoded_url}")
                            return ""
                        elif response.status_code == 429:  # Rate limit
                            wait_time = (attempt + 1) * 5
                            logging.warning(f"Rate limit hit, waiting {wait_time} seconds...")
                            await asyncio.sleep(wait_time)
                        else:
                            logging.error(f"ScrapingAnt API error: {response.status_code}")
                            await asyncio.sleep(2)
                            
                    except httpx.ReadTimeout:
                        logging.warning(f"Timeout on attempt {attempt + 1}")
                        await asyncio.sleep(2)
                        
            except Exception as e:
                logging.error(f"Fetch error on attempt {attempt + 1}: {str(e)}")
                await asyncio.sleep(2)
                    
        return ""

    def parse_jobs(self, html: str, query: str, location: str) -> List[Dict]:
        try:
            if not html:
                return []
                
            data = re.findall(r'window.mosaic.providerData\["mosaic-provider-jobcards"\]=(\{.+?\});', html)
            if not data:
                return []
            
            parsed_data = json.loads(data[0])
            results = parsed_data["metaData"]["mosaicProviderJobCardsModel"]["results"]
            
            jobs = []
            for job in results[:15]:
                job_info = {
                    "searched_job_title": query,
                    "searched_city": location,
                    "job_title": job.get("title", "N/A"),
                    "company": job.get("company", "N/A"),
                    "location": job.get("formattedLocation", location),
                    "salary": job.get("salary", "N/A"),
                    "description": job.get("snippet", "N/A"),
                    "url": f"https://www.indeed.com/viewjob?jk={job.get('jobkey', '')}"
                }
                jobs.append(job_info)
            
            return jobs
        except Exception as e:
            logging.error(f"Parse error: {str(e)}")
            return []

    async def save_jobs(self, jobs: List[Dict]):
        if not jobs:
            return

        headers = [
            "searched_job_title", "searched_city", "job_title", "company",
            "location", "salary", "description", "url"
        ]

        async with self.lock:
            file_exists = os.path.exists(OUTPUT_FILE)
            async with aiofiles.open(OUTPUT_FILE, mode="a", encoding="utf-8", newline="") as file:
                if not file_exists:
                    await file.write(",".join(headers) + "\n")
                
                for job in jobs:
                    row = [
                        str(job.get(field, "")).replace(",", ";").replace("\n", " ").strip()
                        for field in headers
                    ]
                    await file.write(",".join(f'"{item}"' for item in row) + "\n")

    async def process_combination(self, job: str, city: str, client: httpx.AsyncClient):
        if (job, city) in self.processed_combinations:
            return

        logging.info(f"Scraping {job} jobs in {city} 0/15")
        
        try:
            # Create Indeed URL with proper encoding
            base_url = "https://www.indeed.com/jobs"
            params = {
                'q': job.replace(' ', '+'),
                'l': city.replace(' ', '+')
            }
            indeed_url = f"{base_url}?{urlencode(params)}"
            logging.info(f"Requesting URL: {indeed_url}")
            
            html = await self.fetch_with_retries(indeed_url, client)
            if html:
                jobs = self.parse_jobs(html, job, city)
                if jobs:
                    logging.info(f"Jobs found {len(jobs)}/15 for {job} in {city}")
                    await self.save_jobs(jobs)
                else:
                    logging.info(f"No jobs found for {job} in {city}")
            else:
                logging.warning(f"Failed to fetch data for {job} in {city}")
        
            async with self.lock:
                self.processed_combinations.add((job, city))
                
        except Exception as e:
            logging.error(f"Error processing {job} in {city}: {str(e)}")

    async def worker(self, worker_id: int, combinations: List[tuple]):
        async with httpx.AsyncClient(timeout=30.0) as client:
            for job, city in combinations:
                try:
                    await self.process_combination(job, city, client)
                    await asyncio.sleep(1)  # Add delay between requests
                except Exception as e:
                    logging.error(f"Worker {worker_id} error processing {job} in {city}: {str(e)}")
                    await asyncio.sleep(2)  # Add longer delay on error

    async def run(self, job_titles: List[str], cities: List[str]):
        all_combinations = list(product(job_titles, cities))
        batch_size = math.ceil(len(all_combinations) / NUM_WORKERS)
        
        tasks = []
        for i in range(NUM_WORKERS):
            start_idx = i * batch_size
            end_idx = start_idx + batch_size
            worker_combinations = all_combinations[start_idx:end_idx]
            tasks.append(self.worker(i + 1, worker_combinations))
        
        await asyncio.gather(*tasks)

async def main():
    try:
        if os.path.exists(OUTPUT_FILE):
            os.remove(OUTPUT_FILE)
            
        scraper = BatchJobScraper()
        job_titles = scraper.read_file_content("job_title.txt")
        cities = scraper.read_file_content("cities.txt")
        
        if not job_titles or not cities:
            logging.error("Empty input files")
            return
            
        logging.info(f"Starting scraper with {len(job_titles)} jobs * {len(cities)} cities")
        logging.info(f"Using {NUM_WORKERS} workers")
        
        await scraper.run(job_titles, cities)
        logging.info("Scraping completed")
        
    except Exception as e:
        logging.error(f"Main error: {str(e)}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Scraping stopped by user")
    except Exception as e:
        logging.error(f"Scraper failed with error: {str(e)}")
