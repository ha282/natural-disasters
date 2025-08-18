import pandas as pd
from src.transform.clean_dataset9 import clean_dataset9
from src.utils.logging_utils import setup_logger

logger = setup_logger("transform_data", "transform_data.log")


def transform_data(data) -> pd.DataFrame:
    try:
        logger.info("Starting data transformation process...")
        # Clean natural disaster data
        logger.info("Cleaning transaction data...")
        cleaned_dataset = clean_dataset9(data)
        logger.info("Transaction data cleaned successfully.")
        
        return cleaned_dataset
    except Exception as e:
        logger.error(f"Data transformation failed: {str(e)}")
        raise