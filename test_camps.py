import asyncio
from app.db.supabase import supabase
from app.services.ingest import IngestService

def test():
    # Let's see if the campaigns table exists first
    try:
        resp = supabase.table("campaigns").select("*").limit(1).execute()
        print("Campaigns table exists. Data:", resp.data)
    except Exception as e:
        print("Failed to query campaigns table. Did you execute the SQL in Supabase? Error:", e)

    # Note: we won't do a full pull unless we know a specific account_id mapped.
    try:
        accts_resp = supabase.table("brand_accounts").select("account_id").execute()
        if accts_resp.data:
            account_id = accts_resp.data[0]["account_id"]
            print(f"Testing status pull for mapped ad account: {account_id}")
            
            # just test the fetching part explicitly 
            from app.core.config import settings
            from facebook_business.api import FacebookAdsApi
            from facebook_business.adobjects.adaccount import AdAccount
            FacebookAdsApi.init(access_token=settings.META_SYSTEM_USER_TOKEN)
            norm_id  = account_id if account_id.startswith("act_") else f"act_{account_id}"
            
            camps = AdAccount(norm_id).get_campaigns(fields=["id", "name", "effective_status"], params={"limit": 10})
            print(f"✅ Meta API successfully returned {len(camps)} campaigns! Examples:")
            for c in camps[:3]:
                print(f" - {c.get('name')}: {c.get('effective_status')}")
                
        else:
            print("No mapped accounts found in DB to test with.")
    except Exception as e:
        print("Meta API call failed:", e)

if __name__ == "__main__":
    test()
