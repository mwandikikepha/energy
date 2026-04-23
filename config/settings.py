from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # These names must match your .env keys (case-insensitive)
    collect_api:   str 
    mongo_url:     str 
    database_name: str = "energy_db"
    environment:   str = "development"
    debug:         bool = True

    model_config = SettingsConfigDict(
        env_file=".env", 
        case_sensitive=False,
        extra="ignore"  
    )



settings = Settings()