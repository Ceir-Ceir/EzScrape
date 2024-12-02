import subprocess
import sys
import os
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import logging
import queue

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

def install_required_packages():
    for package in ['undetected-chromedriver', 'selenium', 'pandas', 'beautifulsoup4']:
        try:
            __import__(package.replace('-', '_'))
        except ImportError:
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])

print("Installing packages...")
install_required_packages()

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys

class JobScraper:
    def __init__(self, output_dir: str, num_pages: int):
        self.output_dir = output_dir
        self.num_pages = num_pages
        self.job_queue = queue.Queue()
        self.result_queue = queue.Queue()
        self.drivers = {}

    def create_driver(self):
        options = uc.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--blink-settings=imagesEnabled=false')
        return uc.Chrome(options=options)

    def get_job_details(self, driver, job_url, job_title, city):
        try:
            driver.get(job_url)
            time.sleep(1)
            
            return {
                'Search_Job_Title': job_title,
                'Search_City': city,
                'Title': driver.find_element(By.CSS_SELECTOR, "h1.jobsearch-JobInfoHeader-title").text.strip(),
                'Company': driver.find_element(By.CSS_SELECTOR, "[data-company-name='true']").text.strip(),
                'Salary': driver.find_element(By.CSS_SELECTOR, "#salaryInfoAndJobType span.css-19j1a75").text.strip(),
                'Location': driver.find_element(By.CSS_SELECTOR, "[data-testid='inlineHeader-companyLocation']").text.strip(),
                'Description': driver.find_element(By.CSS_SELECTOR, "#jobDescriptionText").text.strip(),
                'URL': job_url
            }
        except Exception:
            return {
                'Search_Job_Title': job_title,
                'Search_City': city,
                'Title': "Not specified",
                'Company': "Not specified",
                'Salary': "Not specified",
                'Location': "Not specified",
                'Description': "Not specified",
                'URL': job_url
            }

    def worker(self, thread_id):
        driver = self.create_driver()
        while True:
            try:
                job_title, city = self.job_queue.get_nowait()
            except queue.Empty:
                break

            try:
                jobs_list = []
                for page in range(self.num_pages):
                    if page == 0:
                        driver.get("https://www.indeed.com")
                        what_input = WebDriverWait(driver, 5).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "#text-input-what"))
                        )
                        what_input.send_keys(job_title)
                        where_input = driver.find_element(By.CSS_SELECTOR, "#text-input-where")
                        where_input.clear()
                        where_input.send_keys(city + Keys.RETURN)
                    else:
                        url = f"https://www.indeed.com/jobs?q={job_title.replace(' ', '+')}&l={city.replace(' ', '+')}&start={page*10}"
                        driver.get(url)

                    time.sleep(2)
                    job_cards = WebDriverWait(driver, 5).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".job_seen_beacon"))
                    )

                    for card in job_cards[:10]:
                        try:
                            job_url = card.find_element(By.CSS_SELECTOR, "h2.jobTitle a").get_attribute('href')
                            job_details = self.get_job_details(driver, job_url, job_title, city)
                            jobs_list.append(job_details)
                            logging.info(f"Thread {thread_id}: Scraped {job_details['Title']} in {city}")
                        except Exception as e:
                            logging.error(f"Error scraping job card: {e}")
                            continue

                self.result_queue.put(jobs_list)
            except Exception as e:
                logging.error(f"Error in thread {thread_id} for {job_title} in {city}: {e}")
            finally:
                self.job_queue.task_done()

        driver.quit()

    def run(self, job_titles, cities):
        for job_title in job_titles:
            for city in cities:
                self.job_queue.put((job_title, city))

        threads = []
        num_threads = min(8, len(job_titles) * len(cities))  # Limit to 8 threads
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            for i in range(num_threads):
                threads.append(executor.submit(self.worker, i))

        all_jobs = []
        while not self.result_queue.empty():
            all_jobs.extend(self.result_queue.get())

        df = pd.DataFrame(all_jobs)
        output_file = os.path.join(self.output_dir, "indeed_jobs_all.csv")
        df.to_csv(output_file, index=False)
        return output_file

def main():
    try:
        with open('job_title.txt', 'r') as f:
            job_titles = [line.strip() for line in f if line.strip()]
        with open('cities.txt', 'r') as f:
            cities = [line.split(maxsplit=1)[1].strip() for line in f if line.strip()]

        num_pages = int(input("Enter number of pages to scrape per job and city: "))
        output_dir = f"indeed_jobs_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        os.makedirs(output_dir, exist_ok=True)

        scraper = JobScraper(output_dir, num_pages)
        output_file = scraper.run(job_titles, cities)
        logging.info(f"Scraping completed! Data saved to {output_file}")

    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()