from pydantic import BaseSettings


class Config(BaseSettings):
    DATABASE_USER: str = "app_user"
    DATABASE_PASSWORD: str = "app_password"
    DATABASE_HOST: str = "172.20.0.10"
    DATABASE_NAME: str = "sensor"
    DATABASE_PORT: int = 5432
    DATABASE_POOL_SIZE: int = 5
    DATABASE_MAX_OVERFLOW: int = 50


settings = Config()