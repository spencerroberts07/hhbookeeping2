from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_env: str = "development"
    app_base_url: str = "http://localhost:8000"
    database_url: str

    qbo_client_id: str
    qbo_client_secret: str
    qbo_redirect_uri: str
    qbo_scope: str = "com.intuit.quickbooks.accounting"
    qbo_auth_url: str = "https://appcenter.intuit.com/connect/oauth2"
    qbo_token_url: str = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
    qbo_api_base_url: str = "https://quickbooks.api.intuit.com"
    qbo_minor_version: str = "75"
    default_entity_code: str = "1877-8"


settings = Settings()
