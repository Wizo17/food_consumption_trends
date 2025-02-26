import os
from dotenv import load_dotenv
from typing import Any, Dict

class Configuration:
    _instance = None
    _config: Dict[str, Dict[str, Any]] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Configuration, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance
    
    def _initialize(self) -> None:
        load_dotenv()
        
        self._config = {
            "GENERAL": {
                "ENV": os.getenv("ENV"),
                "APP_NAME": os.getenv("APP_NAME"),
                "SPARK_JDBC_PATH": os.getenv("SPARK_JDBC_PATH"),
                "SPARK_HOST": os.getenv("SPARK_HOST")
            },
            "POSTGRES": {
                "DB_POSTGRES_HOST": os.getenv("DB_POSTGRES_HOST"),
                "DB_POSTGRES_PORT": os.getenv("DB_POSTGRES_PORT"),
                "DB_POSTGRES_NAME": os.getenv("DB_POSTGRES_NAME"),
                "DB_POSTGRES_USER": os.getenv("DB_POSTGRES_USER"),
                "DB_POSTGRES_PASSWORD": os.getenv("DB_POSTGRES_PASSWORD"),
                "DB_POSTGRES_DEFAULT_SCHEMA": os.getenv("DB_POSTGRES_DEFAULT_SCHEMA")
            },
            "GCP": {
                "GCP_PROJECT_ID": os.getenv("GCP_PROJECT_ID"),
                "GCP_BIGQUERY_DATASET": os.getenv("GCP_BIGQUERY_DATASET")
            },
            "DATASET": {
                "DEFAULT_RAW_TABLE": os.getenv("DEFAULT_RAW_TABLE")
            },
            "DEV": {
                "DEV_SPARK_MEMORY": os.getenv("DEV_SPARK_MEMORY"),
                "DEV_LIMIT_SIZE_DATA": os.getenv("DEV_LIMIT_SIZE_DATA")
            }
        }
    
    def get(self, path: str) -> Any:
        """
        Retrieves a configuration value using a path in the format "section.key"
        
        Args:
            path: Access path in the format "section.key" (ex: "POSTGRES.DB_HOST")
            
        Returns:
            The requested configuration value
        """
        try:
            section, key = path.split(".")
            return self._config[section][key]
        except (KeyError, ValueError) as e:
            #raise KeyError(f"Invalid configuration path: {path}") from e
            return ""

# Create a single instance for import
global_conf = Configuration()
