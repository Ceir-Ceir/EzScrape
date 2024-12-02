import asyncio
import aiohttp
from bs4 import BeautifulSoup
import os
from datetime import datetime
import time
from urllib.parse import quote
from typing import List, Dict, Optional
import logging
import pandas as pd
import random

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler('job_scraper.log'),
        logging.StreamHandler()
    ]
)

class JobScraper:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.scrapingant.com/v2/general"
        self.output_dir = self._create_output_directory()
        self.sem = asyncio.Semaphore(10)  # Reduced concurrent requests
        self.batch_size = 25  # Smaller batch size
        self.results = []
        
        # Configure client settings
        self.timeout = aiohttp.ClientTimeout(total=60)  # Increased timeout
        self.connector = aiohttp.TCPConnector(
            limit=10,
            ssl=False,
            ttl_dns_cache=300
        )
        self.session = None

    @staticmethod
    def _create_output_directory() -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = f"jobs_{timestamp}"
        os.makedirs(output_dir, exist_ok=True)
        return output_dir

    async def get_page(self, url: str) -> Optional[str]:
        if not self.session or self.session.closed:
            return None

        async with self.sem:
            for retry in range(3):  # Add retries
                try:
                    params = {
                        "url": url,
                        "browser": "false",
                        "proxy_country": "US"
                    }
                    headers = {"x-api-key": self.api_key}
                    
                    async with self.session.get(
                        self.base_url,
                        params=params,
                        headers=headers,
                        timeout=self.timeout
                    ) as response:
                        if response.status == 200:
                            return await response.text()
                        elif response.status == 429:
                            wait_time = random.uniform(2, 5)
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            logging.warning(f"Failed request: {url} with status {response.status}")
                            await asyncio.sleep(1)
                except Exception as e:
                    logging.error(f"Error fetching {url}: {str(e)}")
                    if retry < 2:  # Don't sleep on last retry
                        await asyncio.sleep(1)
            return None

    def parse_job_listing(self, html: str, job_title: str, city: str) -> List[Dict]:
        if not html:
            return []
            
        try:
            soup = BeautifulSoup(html, 'lxml')
            jobs = []
            
            for card in soup.select(".job_seen_beacon")[:10]:
                try:
                    title = card.select_one("h2.jobTitle")
                    company = card.select_one("[data-company-name]")
                    salary = card.select_one(".salary-snippet") or card.select_one(".estimated-salary")
                    location = card.select_one(".companyLocation")
                    description = card.select_one(".job-snippet")
                    apply_link = card.select_one("h2.jobTitle a")
                    
                    if not title or not apply_link:
                        continue

                    job = {
                        'search_job': job_title,
                        'search_city': city,
                        'title': title.text.strip() if title else "N/A",
                        'company': company.text.strip() if company else "N/A",
                        'salary': salary.text.strip() if salary else "N/A",
                        'location': location.text.strip() if location else "N/A",
                        'description': description.text.strip() if description else "N/A",
                        'apply_link': f"https://www.indeed.com{apply_link['href']}" if apply_link else "N/A",
                        'timestamp': datetime.now().isoformat()
                    }
                    jobs.append(job)
                except Exception as e:
                    logging.error(f"Error parsing job card: {str(e)}")
                    continue
                    
            return jobs
        except Exception as e:
            logging.error(f"Error parsing HTML: {str(e)}")
            return []

    async def process_job_city_pair(self, job: str, city: str) -> List[Dict]:
        url = f"https://www.indeed.com/jobs?q={quote(job)}&l={quote(city)}"
        html = await self.get_page(url)
        return self.parse_job_listing(html, job, city) if html else []

    async def process_batch(self, combinations: List[tuple]) -> List[Dict]:
        tasks = []
        for job, city in combinations:
            task = asyncio.create_task(self.process_job_city_pair(job, city))
            tasks.append(task)
            
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
        valid_results = []
        for result in batch_results:
            if isinstance(result, list):
                valid_results.extend(result)
        return valid_results

    def save_results(self, results: List[Dict], batch_num: int):
        if not results:
            return
            
        df = pd.DataFrame(results)
        filename = os.path.join(self.output_dir, f'jobs_batch_{batch_num}.csv')
        df.to_csv(filename, index=False, encoding='utf-8')
        logging.info(f"Saved batch {batch_num} with {len(results)} jobs to {filename}")

    async def run(self, job_titles: List[str], cities: List[str]):
        combinations = [(job, city) for job in job_titles for city in cities]
        total_combinations = len(combinations)
        logging.info(f"Total combinations to process: {total_combinations}")
        
        start_time = time.time()
        batch_num = 1
        
        async with aiohttp.ClientSession(
            connector=self.connector,
            timeout=self.timeout
        ) as session:
            self.session = session
            
            try:
                # Process in batches
                for i in range(0, total_combinations, self.batch_size):
                    batch = combinations[i:i + self.batch_size]
                    logging.info(f"Processing batch {batch_num} ({i+1}-{min(i+self.batch_size, total_combinations)} of {total_combinations})")
                    
                    batch_results = await self.process_batch(batch)
                    self.save_results(batch_results, batch_num)
                    
                    batch_num += 1
                    await asyncio.sleep(2)  # Increased delay between batches
                    
            except Exception as e:
                logging.error(f"Error in run: {str(e)}")
            finally:
                self.session = None
                
        elapsed_time = time.time() - start_time
        logging.info(f"Scraping completed in {elapsed_time/3600:.2f} hours")

def read_input_files() -> tuple:
    try:
        with open('job_title.txt', 'r', encoding='utf-8') as f:
            job_titles = [line.strip() for line in f if line.strip()]
        
        with open('cities.txt', 'r', encoding='utf-8') as f:
            cities = [line.strip() for line in f if line.strip()]
            
        return job_titles, cities
    except Exception as e:
        logging.error(f"Error reading input files: {str(e)}")
        return [], []

async def main():
    API_KEY = "c2c798dce3e8473ea03f48b10f7c2d87"  # Replace with your API key
    
    job_titles, cities = read_input_files()
    if not job_titles or not cities:
        logging.error("Failed to read input files")
        return
        
    logging.info(f"Loaded {len(job_titles)} job titles and {len(cities)} cities")
    
    try:
        scraper = JobScraper(API_KEY)
        await scraper.run(job_titles, cities)
    except Exception as e:
        logging.error(f"Main error: {str(e)}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Scraping stopped by user")
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")