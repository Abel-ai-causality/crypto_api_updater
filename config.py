"""
Configuration file for the crypto API updater project.
"""
import os

# Try to load environment variables from .env file if python-dotenv is available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # python-dotenv is optional, so if it's not installed, just continue
    pass

# Define the base data directory
BASE_DATA_DIR = os.getenv("BASE_DATA_DIR", "/root/data")

# Define the path structure for storing parquet files
# Format: {market}/{freq}/{symbol}/{year}/data.parquet
DATA_PATH_FORMAT = os.path.join(BASE_DATA_DIR, "{market}", "{freq}", "{symbol}", "{year}", "data.parquet")