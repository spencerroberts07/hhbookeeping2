from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


# Sentinel to detect any leftover dev placeholder still floating around in
# someone's environment. We never want this to be the live signing key.
_REJECTED_JWT_SECRETS = {
    "",
    "dev_jwt_secret_replace_in_production_minimum_32_chars_long",
    "replace_me",
    "changeme",
}


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
    google_sheets_service_account_email: str = "replace_me"
    google_sheets_private_key: str = "replace_me"
    google_sheets_spreadsheet_id: str = "replace_me"
    cash_balancing_lookback_days: int = 56

    # JWT_SECRET must be set in the environment. We deliberately do NOT
    # provide a fallback default: a fallback caused issued-then-401 incidents
    # where the token was signed under one secret and verified under another
    # after a deploy that finally injected JWT_SECRET. Fail at boot instead.
    jwt_secret: str
    jwt_algorithm: str = "HS256"
    jwt_expiry_hours: int = 8

    # Optional. When set, services_claude_classifier uses it as Layer 3
    # of the bank-auto-journal classifier. Leave unset to skip the LLM
    # fallback — Layers 1 (rules) and 2 (vendor memory) still run.
    anthropic_api_key: str | None = None

    @field_validator("jwt_secret")
    @classmethod
    def _validate_jwt_secret(cls, value: str) -> str:
        if value in _REJECTED_JWT_SECRETS:
            raise ValueError(
                "JWT_SECRET is unset or matches a known placeholder. "
                "Set a real secret (>=32 chars) in the environment."
            )
        if len(value) < 32:
            raise ValueError(
                f"JWT_SECRET must be at least 32 characters (got {len(value)})."
            )
        return value


settings = Settings()
