"""
Main Pipeline

This script implements a comprehensive pipeline for scraping job data from Indeed,
processing it, and storing it in Google Cloud Storage and BigQuery. It includes
functions for web scraping, data cleaning, analysis, and cloud storage operations.

Usage:
    Ensure all required libraries are installed and Google Cloud credentials are set up.
    Run the script to start the job scraping and analysis process.

Note: This script is designed to run periodically to gather job data over time.
"""

import os
import logging
import sys
import re
from datetime import datetime, date

import pandas as pd
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from google.cloud import storage, bigquery
from google.api_core.exceptions import NotFound, BadRequest
from scraper_config import load_config

from scraper import scrape_job_data, configure_webdriver, search_jobs

# logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# load in configf
config = load_config()

# config vals
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config['GOOGLE_APPLICATION_CREDENTIALS']
bucket_name = config['BUCKET_NAME']
project_id = config['PROJECT_ID']
dataset_id = config['DATASET_ID']

def download_nltk_data():
    """Download required NLTK data for text processing"""
    nltk.download('punkt')
    nltk.download('stopwords')

def upload_to_gcs(df, bucket_name, blob_name):
    """
    Upload DF to GCS
    
    Args:
        df : The DF to upload
        bucket_name (str): The name of the GCS bucket
        blob_name (str): The name of the blob in GCS
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(df.to_csv(index=False), 'text/csv')
    logger.info(f"File uploaded to gs://{bucket_name}/{blob_name}")

def split_work_format_and_location(location_string):
    """
    Split the loc string into work type and location
    
    Args:
        location_string (str): The original loc string
    
    Returns:
        tuple: (work_format, location)
    """
    work_formats = ['Hybrid', 'Remote', 'In Person']
    work_format = 'Unknown'
    
    for format in work_formats:
        if format.lower() in location_string.lower():
            work_format = format
            location_string = re.sub(f'{format}.*?in', '', location_string, flags=re.IGNORECASE).strip()
            break
    
    location = re.sub(r'\s+', ' ', location_string).strip()
    
    return work_format, location

def ensure_dataset_exists(client, dataset_id):
    """
    Check for existing BigQuery dataset and create one if there is None
    
    Args:
        client (bigquery.Client): BigQuery client
        dataset_id (str): The ID of the dataset to check or create
    """
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset = client.create_dataset(dataset)
        logger.info(f"Dataset {dataset_id} created.")

def load_to_bigquery(bucket_name, blob_name, project_id, dataset_id, table_id):
    """
    Load data from GCS to BigQuery
    
    Args:
        bucket_name (str): The name of the GCS bucket
        blob_name (str): The name of the blob in GCS
        project_id (str): GC project ID
        dataset_id (str): BigQuery dataset ID
        table_id (str): BigQuery table ID
    """
    client = bigquery.Client()
    ensure_dataset_exists(client, dataset_id)
    
    dataset_ref = client.dataset(dataset_id, project=project_id)
    table_ref = dataset_ref.table(table_id)
    
    job_config = bigquery.LoadJobConfig(
        autodetect=False,
        source_format=bigquery.SourceFormat.CSV,
        schema=[
            bigquery.SchemaField("Link", "STRING"),
            bigquery.SchemaField("Job_Title", "STRING"),
            bigquery.SchemaField("Company", "STRING"),
            bigquery.SchemaField("Date_Posted", "STRING"),
            bigquery.SchemaField("Location", "STRING"),
            bigquery.SchemaField("Salary", "STRING"),
            bigquery.SchemaField("Job_Type", "STRING"),
            bigquery.SchemaField("Work_Format", "STRING"),
            bigquery.SchemaField("Processed_Location", "STRING")
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    uri = f"gs://{bucket_name}/{blob_name}"
    
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    
    try:
        load_job.result()
        logger.info(f"Loaded {load_job.output_rows} rows into {project_id}:{dataset_id}.{table_id}")
    except BadRequest as e:
        logger.error(f"Error loading data: {e}")

def basic_data_analysis(df):
    """
    Print basic Data analytics (could be improved future iterations)
    
    Args:
        df (pd.DataFrame): DF to analyze
    """
    logger.info("\nBasic Data Analysis:")
    logger.info(f"Total jobs scraped: {len(df)}")
    logger.info(f"Unique companies: {df['Company'].nunique()}")
    logger.info(f"\nTop 5 companies by job postings:")
    logger.info(df['Company'].value_counts().head())
    logger.info(f"\nMost common jfob titles:")
    logger.info(df['Job Title'].value_counts().head())
    logger.info(f"\nLocation distribution :")
    logger.info(df['Location'].value_counts().head())
    logger.info(f"\nJob Type distribution:")
    logger.info(df['Job Type'].value_counts())
    logger.info(f"\nSalary information available for {df['Salary'].ne('N/A').sum()} jobs")
    if df['Salary'].ne('N/A').sum() > 0:
        logger.info(f"\nSample of salaries:")
        logger.info(df[df['Salary'] != 'N/A']['Salary'].sample(min(5, df['Salary'].ne('N/A').sum())))

def identify_it_jobs(df):
    """
    Identify all IT-related jobs using nlp 
    
    Args:
        df (pd.DataFrame): The DF containing job dat
    
    Returns:
        pd.DataFrame: A DF containing only IT-related jobs
    """
    try:
        stop_words = set(stopwords.words('english'))
        df['processed_title'] = df['Job Title'].apply(
            lambda x: ' '.join([word.lower() for word in word_tokenize(x) 
                                if word.isalnum() and word.lower() not in stop_words])
        )

        vectorizer = TfidfVectorizer(max_features=1000)
        tfidf_matrix = vectorizer.fit_transform(df['processed_title'])

        kmeans = KMeans(n_clusters=10, random_state=42)
        df['cluster'] = kmeans.fit_predict(tfidf_matrix)

        it_keywords = ['software', 'developer', 'engineer', 'data', 'analyst', 'network', 'security', 'system', 'admin', 'cloud']
        it_clusters = [i for i in range(10) if any(keyword in ' '.join(df[df['cluster'] == i]['processed_title']) for keyword in it_keywords)]

        return df[df['cluster'].isin(it_clusters)]
    except Exception as e:
        logger.error(f"Error in identify_it_jobs: {e}")
        return df

def main():
    """function to run the job scraping and analysis pipeline"""
    driver = None
    try:
        driver = configure_webdriver()
        country = 'https://www.indeed.com'
        job_position = "IT"
        job_location = "all"

        target_date = date(2024, 9, 1)
        current_date = date.today()
        days_since = max(0, (current_date - target_date).days)

        logger.info(f"Searching for {job_position} jobs in {job_location} from the last {days_since} days")
        full_url, total_jobs = search_jobs(driver, country, job_position, job_location, days_since)
        
        if total_jobs == "Unknown":
            logger.warning("Could not determine the total number of jobs. Proceeding anyway")
        else:
            logger.info(f"Found {total_jobs} jobs to scrape")

        df = scrape_job_data(driver, country, total_jobs)

        if df.empty:
            logger.warning("No results found. Something went wrong D:")
            logger.info(f"Try a manual search with this link: {full_url}")
        else:
            logger.info(f"Successfully scraped {len(df)} jobs")
            
            logger.info("Processing location data...")
            df[['Work Format', 'Processed Location']] = df['Location'].apply(split_work_format_and_location).apply(pd.Series)
            
            try:
                df_it_jobs = identify_it_jobs(df)
                logger.info(f"Identified {len(df_it_jobs)} IT-related jobs")
            except Exception as e:
                logger.error(f"Error in identifying IT jobs: {e}")
                logger.info("Proceeding with all scraped jobs")
                df_it_jobs = df

            blob_name = f'indeed_it_jobs_{target_date.strftime("%Y%m%d")}.csv'
            upload_to_gcs(df_it_jobs, bucket_name, blob_name)

            table_id = 'indeed_it_jobs'

            try:
                load_to_bigquery(bucket_name, blob_name, project_id, dataset_id, table_id)
                logger.info("Data successfully loaded to BigQuery")
            except Exception as e:
                logger.error(f"Error loading data to BigQuery: {e}")

            basic_data_analysis(df_it_jobs)

            logger.info("Data pipeline completed successfully :D!")

    except Exception as e:
        logger.exception(f"An error occurred: {e}")
    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    download_nltk_data()
    main()