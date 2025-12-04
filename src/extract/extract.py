import pandas as pd
from src.extract.extract_dataset9 import extract_dataset9
from src.extract.extract_basic_company_data import extract_basic_company_data
from src.utils.logging_utils import setup_logger

logger = setup_logger("extract_data", "extract_data.log")


def extract_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    try:
        logger.info("Starting data extraction process")

        dataset9 = extract_dataset9()

        logger.info(
            f"Data extraction completed successfully - "
            f"Natural Disasters: {dataset9.shape}"
        )
        
        basic_company_data = extract_basic_company_data()
        logger.info(
            f"Data extraction completed successfully - "
            f"Basic company data: {basic_company_data.shape}"
        )

        #return (dataset9)
        return (dataset9, basic_company_data)

    except Exception as e:
        logger.error(f"Data extraction failed: {str(e)}")
        raise
