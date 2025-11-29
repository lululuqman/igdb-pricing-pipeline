import requests
import json
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Load credentials from .env
CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")
# The access token is fetched dynamically
ACCESS_TOKEN = None

# --- CONSTANTS ---
TWITCH_AUTH_URL = "https://id.twitch.tv/oauth2/token"
IGDB_API_URL = "https://api.igdb.com/v4/games"
CURRENCY_API_URL = "https://api.fxratesapi.com/latest" # A simple, free exchange rate API

def get_access_token():
    """Step 1: Get temporary Access Token from Twitch."""
    print("-> Requesting Twitch Access Token...")
    params = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'grant_type': 'client_credentials'
    }
    
    response = requests.post(TWITCH_AUTH_URL, params=params)
    response.raise_for_status()
    
    token_data = response.json()
    token = token_data['access_token']
    print("-> Token received successfully.")
    return token

def get_usd_to_myr_rate():
    """Step 2: Get live exchange rate (USD to MYR) for transformation later."""
    print("-> Fetching live currency rate (USD/MYR)...")
    
    # This API is free and simple; returns rate against a base currency (default USD)
    response = requests.get(CURRENCY_API_URL, params={'base': 'USD', 'currencies': 'MYR'})
    response.raise_for_status()
    
    data = response.json()
    # The current rate is the value of 1 USD in MYR
    exchange_rate = data['rates']['MYR']
    print(f"-> Exchange Rate (USD/MYR): {exchange_rate}")
    return exchange_rate

def extract_game_metadata(token: str):
    """Step 3: Use the token to query IGDB for game metadata."""
    print("-> Fetching IGDB game data...")
    
    # IGDB uses POST requests with the query in the body (a unique DE skill to show)
    headers = {
        'Client-ID': CLIENT_ID,
        'Authorization': f'Bearer {token}'
    }
    
    # Query: Get the name, rating, and genres for the top 10 rated games.
    # Note: 'genres.name' tells the API to return the name from the nested genre object
    body = "fields name, total_rating, genres.name, first_release_date; sort total_rating desc; limit 10;"

    response = requests.post(IGDB_API_URL, headers=headers, data=body)
    response.raise_for_status()
    
    games_list = response.json()
    print(f"-> Successfully fetched {len(games_list)} games.")

    # 4. Save Raw Data (Staging)
    output_path = "data/raw_games_metadata.json"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save the data
    with open(output_path, "w") as f:
        json.dump(games_list, f, indent=4)
        
    print(f"✅ Data saved to {output_path}")
    return games_list


if __name__ == "__main__":
    try:
        # 1. Get Authentication Token
        access_token = get_access_token()
        
        # 2. Get Exchange Rate (for transformation step)
        exchange_rate = get_usd_to_myr_rate()
        
        # 3. Extract Game Data
        game_data = extract_game_metadata(access_token)
        
        # For the next step (Transformation), let's save the rate alongside the data
        with open("data/exchange_rate.json", "w") as f:
            json.dump({"USD_MYR_Rate": exchange_rate}, f)
            
        print("\nPipeline Extraction COMPLETE. Ready for Transformation.")
        
    except requests.exceptions.HTTPError as e:
        print(f"\n❌ FATAL API ERROR: Check your Client ID/Secret in .env or your API query syntax.")
        print(f"Details: {e}")