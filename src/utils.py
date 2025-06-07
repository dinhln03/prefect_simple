import pandas as pd
import numpy as np
from datetime import datetime
from src.cfg import Config
from loguru import logger

def generate_fake_weather_data(cfg: Config = Config()) -> None:
    """Generate fake weather dataset with timestamp, temperature, and humidity."""
    
    # Generate timestamp range from 2025-1-1 to 2025-6-1
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 6, 1)
    
    # Create hourly timestamps
    timestamps = pd.date_range(start=start_date, end=end_date, freq='H')
    
    # Generate random data
    np.random.seed(42)  # For reproducible results
    n_records = len(timestamps)
    
    data = {
        'timestamp': timestamps,
        'temperature': np.random.randint(-10, 51, size=n_records),  # -10 to 50
        'humidity': np.random.randint(20, 101, size=n_records)      # 20% to 100%
    }
    
    df = pd.DataFrame(data)
    
    # Save as parquet
    df.to_parquet(cfg.mock_data_path, index=False)
    
    logger.info(f"Fake weather data generated with {n_records} records.")
    return
