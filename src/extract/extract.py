import pandas as pd
from src.extract.extract_dataset9 import extract_dataset9
from src.utils.logging_utils import setup_logger

logger = setup_logger("extract_data", "extract_data.log")


def extract_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    try:
        logger.info("Starting data extraction process")

        dataset9 = extract_dataset9()

        logger.info(
            f"Data extraction completed successfully - "
            f"Country: {dataset9.shape}"
        )

        return (dataset9)

    except Exception as e:
        logger.error(f"Data extraction failed: {str(e)}")
        raise
