from pydantic_settings import BaseSettings, SettingsConfigDict
import os


class Settings(BaseSettings):
    # These names must match your .env keys (case-insensitive)
    collect_api:   str 
    mongo_url:     str 
    database_name: str = "energy_db"
    environment:   str = "development"
    debug:         bool = True

    model_config = SettingsConfigDict(
            env_file=os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"),
            env_file_encoding='utf-8'
        )



settings = Settings()
