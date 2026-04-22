from typing import List, Union
from pydantic import validator
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    PROJECT_NAME: str = "CMO Dashboard SaaS"
    API_V1_STR: str = "/api/v1"

    # Allow all origins in development
    BACKEND_CORS_ORIGINS: List[str] = ["*"]

    @validator("BACKEND_CORS_ORIGINS", pre=True)
    def assemble_cors_origins(cls, v: Union[str, List[str]]) -> List[str]:
        if isinstance(v, str):
            if v.startswith("["):
                import json
                return json.loads(v)
            return [i.strip() for i in v.split(",")]
        return v

    # Supabase Configuration
    SUPABASE_URL: str = ""
    SUPABASE_KEY: str = ""
    SUPABASE_SERVICE_ROLE_KEY: str = ""

    # Meta (Facebook) Configuration (Internal System User)
    META_SYSTEM_USER_TOKEN: str = ""
    META_APP_ID: str = ""
    META_APP_SECRET: str = ""
    META_WEBHOOK_VERIFY_TOKEN: str = "cmo_dashboard_verify"

    # Google Ads Configuration
    GOOGLE_DEVELOPER_TOKEN: str = ""
    GOOGLE_REDIRECT_URI: str = ""
    GOOGLE_CUSTOMER_ID: str = ""
    GOOGLE_CLIENT_ID: str = ""
    GOOGLE_CLIENT_SECRET: str = ""
    GOOGLE_REFRESH_TOKEN: str = ""

    # OpenAI / Gemini Configuration
    OPENAI_API_KEY: str = ""
    GEMINI_API_KEY: str = ""

    class Config:
        case_sensitive = True
        env_file = ".env"

settings = Settings()
