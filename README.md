# Indeed IT Job Scraper

This project implements a comprehensive pipeline for scraping IT job data from Indeed.com, processing it, and storing it in Google Cloud Storage and BigQuery. 

## Prerequisites
- Python 3.7+
- Google Cloud account with BigQuery and Cloud Storage set up
- Chrome browser installed (for Selenium WebDriver)

## Project Structure

- `main.py`: Main pipeline script
- `scraper.py`: Web scraping utility functions
- `scraper_config.py`: Configuration loader
- `.env`: Environment variables (you need to create this)
- `service-account-key.json`: Google Cloud service account key (you need to provide this)

## Setup

1. Clone this repository:
   ```
   git clone https://github.com/MelinaRogers/IndeedSiteScraper
   cd IndeedSiteScraper
   ```
2. Install required packages:
   `
   pip install -r requirements.txt
   `
3. Set up Google Cloud credentials:
   - Create a service account in your Google Cloud console
   - Download the JSON key file for this service account
   - Rename the JSON key file to 'service-account-key.json' and place it in the project root directory
4. Create a '.env' file in the project with the following content:
   
   ```
    GOOGLE_APPLICATION_CREDENTIALS='service-account-key.json'
    BUCKET_NAME='your-bucket-name'
    PROJECT_ID='your-project-id'
    DATASET_ID='your-dataset-id'
   ```
   
   Replace the values with your actual Google Cloud details

## Usage
Run the main script to start the job scraping and analyis process:
`
python main.py
`

## Configuration

You can modify the following in `main.py` to customize the scraping:
- `job_position`: The job title to search for (default is "IT")
- `job_location`: The location to search in (default is "all")
- `target_date`: The date from which to start searching for jobs

## Notes

- This script is designed to run periodically to gather job data over time
- Be mindful of Indeed.com's robots.txt and scraping policies
- Ensure your Google Cloud billing is set up correctly to avoid any issues with data storage and processing

## Troubleshooting

If you encounter any issues:
1. Check that all required files are present and correctly named
2. Verify that your Google Cloud credentials are correct and have necessary permissions
3. Ensure all environment variables in the `.env` file are set correctly
4. Check the `scraper.log` file for any error messages or warnings
