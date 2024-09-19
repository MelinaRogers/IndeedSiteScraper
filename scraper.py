"""
Scraper Utils

This module contains utility functions for web scraping job data from Indeed.com
It includes functions for configuring the webdriver, searching for jobs,
and scraping job details from search results

"""

import logging
import time
from typing import Tuple, Optional

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from selenium_stealth import stealth

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def configure_webdriver() -> webdriver.Chrome:
    """
    Configure and return a chrome driver with settings for stealth

    Returns:
        webdriver.Chrome: configured crome webdriver
    """
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("start-maximized")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
    stealth(driver,
            languages=["en-US", "en"],
            vendor="Google Inc.",
            platform="Win32",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True,
            )
    return driver

def search_jobs(driver: webdriver.Chrome, country: str, job_position: str, job_location: str, days_since: int) -> Tuple[str, str]:
    """
    Search for jobs on Indeed.com and return search url and job count

    Args:
        driver (webdriver.Chrome): configured chrome driver 
        country (str): Base url
        job_position (str): Job position to check for
        job_location (str): area to search for (US, SF, Boston, whatever)
        days_since (int): Number of days to look back in job postings

    Returns:
        Tuple[str, str]: Full url and total number of jobs found 
    """
    full_url = f'{country}/jobs?q={"+".join(job_position.split())}&l={job_location}&fromage={days_since}'
    logger.info(f"Searching: {full_url}")
    driver.get(full_url)
    
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".jobsearch-JobCountAndSortPane-jobCount, .jobsearch-NoResult-messageHeader"))
        )
    except TimeoutException:
        logger.warning(f"Timeout after 20 seconds waiting for page to load. URL: {full_url}")
        return full_url, "Unknown"

    try:
        job_count_element = driver.find_element(By.CSS_SELECTOR, ".jobsearch-JobCountAndSortPane-jobCount")
        total_jobs = job_count_element.text.split()[0].replace(',', '').replace('+', '')
        logger.info(f"{total_jobs}+ jobs found")
    except NoSuchElementException:
        logger.warning("No job count found. The page structure might have changed or no jobs were found D:")
        total_jobs = "Unknown"

    return full_url, total_jobs

def scrape_job_data(driver: webdriver.Chrome, country: str, total_jobs: str) -> pd.DataFrame:
    """
    Scrape job data from Indeed search results

    Args:
        driver (webdriver.Chrome):chrome webdriver
        country (str): base url for indeed
        total_jobs (str): total number of jobs to scrape (or "Unknown")

    Returns:
        pd.DataFrame: DFe containing scraped job data
    """
    df = pd.DataFrame(columns=['Link', 'Job Title', 'Company', 'Date Posted', 'Location', 'Salary', 'Job Type'])
    job_count = 0
    page = 0
    max_retries = 3
    max_jobs = 15000  # Set a maximum number of jobs to scrape

    while True:
        for retry in range(max_retries):
            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "job_seen_beacon"))
                )
                break
            except TimeoutException:
                if retry == max_retries - 1:
                    logger.error("Failed to load job listings after multiple attempts")
                    return df
                logger.warning(f"Timeout on attempt {retry + 1}. Retrying...")
                driver.refresh()

        soup = BeautifulSoup(driver.page_source, 'lxml')
        boxes = soup.find_all('div', class_='job_seen_beacon')

        for box in boxes:
            job_data = extract_job_data(box, country)
            df = pd.concat([df, pd.DataFrame([job_data])], ignore_index=True)
            job_count += 1

        logger.info(f"Scraped {job_count} jobs so far")

        if job_count >= max_jobs or (total_jobs != "Unknown" and job_count >= int(total_jobs)):
            logger.info("Reached the maximum number of jobs or the total number of jobs found")
            break

        if not navigate_to_next_page(driver):
            break

    return df

def extract_job_data(box: BeautifulSoup, country: str) -> dict:
    """
    Extract job data from a single job posting box

    Args:
        box (BeautifulSoup): BeautifulSoup obj representing a single job listing
        country (str): base url for indeed

    Returns:
        dict: dictionary containing extracted job data
    """
    link = country + box.find('a').get('href')
    job_title = box.find('a', class_='jcs-JobTitle').text.strip() if box.find('a', class_='jcs-JobTitle') else 'N/A'
    company = box.find('span', {'data-testid': 'company-name'}).text.strip() if box.find('span', {'data-testid': 'company-name'}) else 'N/A'
    date_posted = extract_date_posted(box)
    location = box.find('div', {'data-testid': 'text-location'}).text.strip() if box.find('div', {'data-testid': 'text-location'}) else 'N/A'
    salary = box.find('div', class_='metadata salary-snippet-container').text.strip() if box.find('div', class_='metadata salary-snippet-container') else 'N/A'
    job_type = extract_job_type(box)

    return {
        'Link': link,
        'Job Title': job_title,
        'Company': company,
        'Date Posted': date_posted,
        'Location': location,
        'Salary': salary,
        'Job Type': job_type
    }

def extract_date_posted(box: BeautifulSoup) -> str:
    """
    Extract the date posted from a job posting box

    Args:
        box (BeautifulSoup): BeautifulSoup obj representing a single job listing

    Returns:
        str: date posted or 'N/A' if not found
    """
    date_elements = [
        box.find('span', class_='date'),
        box.find('span', {'data-testid': 'myJobsStateDate'}),
        box.find('span', {'data-testid': 'job-age'})
    ]
    for date_element in date_elements:
        if date_element:
            return date_element.text.strip()
    return 'N/A'

def extract_job_type(box: BeautifulSoup) -> str:
    """
    Extract the job type from a job posting box

    Args:
        box (BeautifulSoup): BeautifulSoup obj representing a single job listing

    Returns:
        str: job type or 'Unknown' if not found
    """
    job_type_element = box.find('div', class_='metadata')
    if job_type_element:
        job_type_text = job_type_element.text.strip().lower()
        if 'full-time' in job_type_text:
            return 'Full-time'
        elif 'part-time' in job_type_text:
            return 'Part-time'
    return 'Unknown'

def navigate_to_next_page(driver: webdriver.Chrome) -> bool:
    """
    Navigate to the next page of job listings

    Args:
        driver (webdriver.Chrome): configured chrome driver

    Returns:
        bool: True if successfully navigated to the next page, otherwise false
    """
    try:
        next_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "a[data-testid='pagination-page-next']"))
        )
        driver.execute_script("arguments[0].click();", next_button)
        time.sleep(2)  # Add a small delay to allow the page to load
        return True
    except (TimeoutException, NoSuchElementException):
        logger.info("No more pages to scrape")
        return False