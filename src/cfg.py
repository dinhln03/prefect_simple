from pydantic import BaseModel

class Config(BaseModel):
    """
    Configuration class for the application.
    """
    mock_data_path: str = "data/fake_stream_data.parquet"
    random_seed: int = 42
    