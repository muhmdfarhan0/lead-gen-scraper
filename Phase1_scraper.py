# phase1_apify_client.py

import os
import pandas as pd
from dotenv import load_dotenv
from apify_client import ApifyClient

# Load environment variables
load_dotenv()
API_TOKEN = os.getenv("APIFY_API_TOKEN")

if not API_TOKEN:
    raise RuntimeError("❌ APIFY_API_TOKEN not set in .env file.")

# Initialize Apify client
client = ApifyClient(API_TOKEN)

def run_actor_search(search_url: str, max_items: int = 100):
    """Triggers the Apify actor with the Facebook Ads Library search URL."""
    print("▶️ Starting Apify actor run...")
    run_input = {
        "urls": [{"url": search_url}],
        "count": max_items,
        "scrapePageAds.activeStatus": "active",
    }
    run = client.actor("curious_coder/facebook-ads-library-scraper").call(run_input=run_input)
    dataset_id = run.get("defaultDatasetId")
    if not dataset_id:
        raise Exception("❌ Dataset ID not returned from actor.")
    return dataset_id

def fetch_dataset(dataset_id):
    """Fetches all results from the Apify dataset."""
    print(f"📥 Fetching results from dataset: {dataset_id}")
    return list(client.dataset(dataset_id).iterate_items())

def clean_ads(data):
    """Deduplicates and cleans ad data for export."""
    seen = set()
    clean = []
    for ad in data:
        page = ad.get("pageName") or ad.get("page_name")
        url = ad.get("pageUrl")
        if not url and page:
            url = f"https://www.facebook.com/{page.replace(' ', '')}"

        # Extract ad text safely
        ad_text = ad.get("adText", "")
        if not ad_text:
            snapshot = ad.get("snapshot") or {}
            body = snapshot.get("body") or {}
            ad_text = body.get("text", "")

        if page and page not in seen:
            clean.append({
                "page_name": page,
                "page_url": url,
                "ad_text": ad_text.strip() or "N/A"
            })
            seen.add(page)
    return pd.DataFrame(clean)

def main():
    print("🟢 Meta Ads Scraper: Phase 1 (Extraction)")

    # Get user input
    keyword = input("🔤 Enter keyword (e.g., 'marketing'): ").strip()
    country = input("🌍 Enter 2-letter country code (e.g., 'US'): ").strip().upper()

    if not keyword or not country:
        print("❗ Both keyword and country are required.")
        return

    # Build Facebook Ads Library URL
    fb_url = (
        "https://www.facebook.com/ads/library/"
        f"?active_status=active&ad_type=all&country={country}"
        f"&q={keyword}&search_type=keyword_unordered&media_type=all"
    )

    print(f"🔗 Facebook Ads Library URL: {fb_url}")

    try:
        dataset_id = run_actor_search(fb_url)
        raw_ads = fetch_dataset(dataset_id)
        print(f"📊 Retrieved {len(raw_ads)} total ads")

        df = clean_ads(raw_ads)
        print(f"🧹 Cleaned to {len(df)} unique pages")

        os.makedirs("results", exist_ok=True)
        output_path = "results/ads_cleaned.csv"
        df.to_csv(output_path, index=False, encoding="utf-8")
        print(f"✅ Phase 1 complete — saved to: {output_path}")

    except Exception as e:
        print("❌ Error occurred:", e)

if __name__ == "__main__":
    main()
