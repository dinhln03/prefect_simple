from prefect import flow, task
from prefect.states import Completed, Failed
import pandas as pd
from loguru import logger
import random

from src.cfg import Config
from src.utils import generate_fake_weather_data

@task
def task__generate_fake_weather_data():
    """
    Task to generate fake weather data.
    """
    generate_fake_weather_data()
    
@task
def task__load_data(config: Config = Config()):
    """
    Task to load data from the specified path.
    """
    # This task can be expanded to load data from a database or file system
    # For now, it just returns the path where the data is saved
    logger.info(f"Loading data from {config.mock_data_path}")
    df = pd.read_parquet(config.mock_data_path)
    return df

@task
def task__clean_data(df: pd.DataFrame):
    """
    Task to clean the data.
    This can include handling missing values, duplicates, etc.
    """
    # Example cleaning step: drop duplicates
    logger.info("Cleaning data by dropping duplicates.")
    logger.info(f"Initial data shape: {df.shape}")
    df_cleaned = df.drop_duplicates()
    logger.info(f"Cleaned data shape: {df_cleaned.shape}")
    return df_cleaned

@task
def task__is_failure():
    """
    Task to simulate a random failure.
    This is for testing purposes and can be removed in production.
    """
    num = random.randint(0, 10)
    if num < 5:
        logger.success("SUCCESS")
        return False  
    logger.error("FAIL")
    return True

@task
def save_data(df: pd.DataFrame, config: Config = Config()):
    """
    Task to save the cleaned data to a specified path.
    """
    output_path = config.mock_data_path.replace(".parquet", "_cleaned.parquet")
    df.to_parquet(output_path, index=False)
    logger.info(f"Cleaned data saved to {output_path}")

@flow(name="ELT pipeline")
def flow__etl():
    # Generate fake weather data
    task__generate_fake_weather_data()
    
    df = task__load_data()

    if task__is_failure():
        logger.error("Task failed, stopping the flow.")
        return Failed(message="Task failed due to random failure condition.")
    df_cleaned = task__clean_data(df)
    save_data(df_cleaned)
    return Completed(message="Flow completed successfully.")

if __name__ == "__main__":
    """
    Main entry point for the application.
    Generates fake weather data and saves it to a specified path.
    """
    flow__etl()