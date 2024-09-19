import os
from dotenv import load_dotenv

def load_config():
    load_dotenv()  
    
    return {
        "GOOGLE_APPLICATION_CREDENTIALS": os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        "BUCKET_NAME": os.getenv("BUCKET_NAME"),
        "PROJECT_ID": os.getenv("PROJECT_ID"),
        "DATASET_ID": os.getenv("DATASET_ID"),
    }