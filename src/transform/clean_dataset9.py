import pandas as pd
from src.utils.logging_utils import setup_logger

logger = setup_logger("transform_data", "transform_data.log")


#def clean_dataset9(dataset9: pd.DataFrame) -> pd.DataFrame:
#    logger.info(f"Before cleaning: {dataset9.shape}")
#    # Task 2 - Remove rows with missing values
#    dataset9 = remove_missing_values(dataset9)
#    logger.info(f"After cleaning: {dataset9.shape}")
#    # Save the dataframe as a CSV for logging purposes
#    dataset9.to_csv("data/processed/cleaned_dataset9.csv", index=False)
#    return dataset9


def clean_dataset9(dataset9: pd.DataFrame, name=None):
    logger.info(f"Before cleaning: {dataset9.shape}")
    # Task 2 - Remove rows with missing values
    dataset9 = remove_missing_values(dataset9)
    logger.info(f"After cleaning: {dataset9.shape}")
    # Save the dataframe as a CSV for logging purposes
    if name:
        dataset9.to_csv(f"data/processed/{name}.csv", index=False)
    return dataset9


def remove_missing_values(dataset9: pd.DataFrame) -> pd.DataFrame:
    dataset = dataset9.dropna(axis=1, thresh=10000).copy()
    #dataset9 = dataset.dropna(subset=["location", "start_month", "end_month", 
    #                                  "cpi"]).copy()
    df3 = dataset.dropna()
    return df3