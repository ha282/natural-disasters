import pandas as pd
from src.load.create_clean_dataset9 import (
    create_clean_dataset9,
)
from src.utils.logging_utils import setup_logger

logger = setup_logger("load_data", "load_data.log")


#def load_data(transformed_data: pd.DataFrame) -> None:
#    try:
#        # Load the transformed data into the target database
#        logger.info("Starting data load process...")
#        logger.info(transformed_data)
#        create_clean_dataset9(transformed_data)
#        logger.info("Data load process completed successfully.")
#    except Exception as e:
#        logger.error(f"Data load failed: {str(e)}")
#        raise

def load_data(transformed_data: pd.DataFrame | tuple) -> None:
    try:
        # Load the transformed data into the target database
        logger.info("Starting data load process...")
        logger.info(transformed_data)
        if isinstance(transformed_data, pd.DataFrame):
            datasets = (transformed_data,)
        else:
            datasets = transformed_data

        # Loop through each cleaned dataset
        for i, df in enumerate(datasets, start=1):
            if df.empty:
                logger.warning(f"Dataset {i} is empty. Skipping load.")
                continue

            create_clean_dataset9(df, table_name=f"cleaned_dataset_{i}")
            logger.info(f"Dataset {i} loaded successfully.")

        logger.info("Data load process completed successfully.")

    except Exception as e:
        logger.error(f"Data load failed: {str(e)}")
        raise
