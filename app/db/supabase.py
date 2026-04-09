from supabase import create_client, Client
from app.core.config import settings

def get_supabase_client() -> Client:
    """Initialize and return a Supabase client."""
    if not settings.SUPABASE_URL or not settings.SUPABASE_KEY:
        raise ValueError("SUPABASE_URL or SUPABASE_KEY not set in environment.")
    return create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)

# Pre-initialized client
supabase: Client = get_supabase_client()
