import pandas as pd
from src.transform.clean_dataset9 import clean_dataset9
from src.utils.logging_utils import setup_logger

logger = setup_logger("transform_data", "transform_data.log")


#def transform_data(data) -> pd.DataFrame:
#    try:
#        logger.info("Starting data transformation process...")
#        # Clean natural disaster data
#        logger.info("Cleaning transaction data...")
#        cleaned_dataset = clean_dataset9(data)
#        logger.info("Transaction data cleaned successfully.")
        
#        return cleaned_dataset
#    except Exception as e:
#        logger.error(f"Data transformation failed: {str(e)}")
#        raise

def transform_data(data) -> pd.DataFrame:
    try:
        logger.info("Starting data transformation process...")
        # Clean natural disaster data
        
        cleaned_datasets = []
    
        #loop through extracted data
        for i, dataset in enumerate(data, start=1):
            logger.info(f"Cleaning dataset {i}...")
            cleaned_dataset = clean_dataset9(dataset, name=f"cleaned_dataset_{i}")
            logger.info(f"Dataset {i} cleaned successfully.")
            cleaned_datasets.append(cleaned_dataset)

        logger.info("All datasets cleaned successfully.")
  
        # Return cleaned datasets as a tuple
        return tuple(cleaned_datasets)
    except Exception as e:
        logger.error(f"Data transformation failed: {str(e)}")
        raise