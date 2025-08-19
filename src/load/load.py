import pandas as pd
from src.load.create_clean_dataset9 import (
    create_clean_dataset9,
)
from src.utils.logging_utils import setup_logger

logger = setup_logger("load_data", "load_data.log")


def load_data(transformed_data: pd.DataFrame) -> None:
    try:
        # Load the transformed data into the target database
        logger.info("Starting data load process...")
        logger.info(transformed_data)
        create_clean_dataset9(transformed_data)
        logger.info("Data load process completed successfully.")
    except Exception as e:
        logger.error(f"Data load failed: {str(e)}")
        raise