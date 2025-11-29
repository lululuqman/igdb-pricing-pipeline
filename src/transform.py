import json
import pandas as pd
from datetime import datetime
import numpy as np
import os

def transform_data():
    """
    1. Loads raw IGDB game metadata and the exchange rate.
    2. Flattens nested JSON fields (genres).
    3. Converts timestamps to dates.
    4. Calculates a simulated MYR price.
    5. Saves the clean data as a CSV file.
    """
    
    # --- 1. Load Data ---
    print("--- Starting Transformation ---")
    
    # Load raw game data
    with open("data/raw_games_metadata.json", "r") as f:
        raw_games = json.load(f)
        
    # Load exchange rate data
    with open("data/exchange_rate.json", "r") as f:
        rate_data = json.load(f)
        
    usd_myr_rate = rate_data.get("USD_MYR_Rate", 4.7) # Use 4.7 as a safe default fallback
    print(f"Loaded USD/MYR Exchange Rate: {usd_myr_rate}")

    # Convert the list of dicts to a DataFrame
    df = pd.DataFrame(raw_games)

    # --- 2. Data Cleaning and Flattening ---

    # 2a. Handle Nested Genres (Flattening/Normalization)
    # The genres field is a list of dicts: [{"id": 4, "name": "Fighting"}]
    def extract_genre_names(genre_list):
        if isinstance(genre_list, list):
            return ", ".join([g['name'] for g in genre_list])
        return np.nan # Use Pandas/Numpy standard NaN for missing values

    df['genres'] = df['genres'].apply(extract_genre_names)
    
    # 2b. Convert Unix Timestamp to Datetime
    # 'first_release_date' is in seconds since epoch.
    df['first_release_date'] = pd.to_datetime(
        df['first_release_date'], unit='s', errors='coerce'
    ).dt.strftime('%Y-%m-%d')
    
    # 2c. Rename/Clean Columns
    df.rename(columns={
        'name': 'game_name',
        'total_rating': 'avg_rating',
        'first_release_date': 'release_date'
    }, inplace=True)

    # --- 3. Data Enrichment (The Pricing Logic) ---
    
    # To simulate pricing, let's assume a correlation between rating and base price.
    # Base USD Price = (Rating / 100) * 80 + 10 (Resulting in prices roughly $10 to $74)
    df['base_price_usd'] = (df['avg_rating'] / 100) * 80 + 10
    
    # Calculate MYR Price (The main goal of the pipeline enrichment)
    df['base_price_myr'] = df['base_price_usd'] * usd_myr_rate
    
    # Round final prices to 2 decimal places for currency
    df['base_price_usd'] = df['base_price_usd'].round(2)
    df['base_price_myr'] = df['base_price_myr'].round(2)
    
    # --- 4. Final Cleanup and Output ---

    # Select and reorder final columns for the database schema
    final_columns = [
        'id', 'game_name', 'release_date', 'genres', 
        'avg_rating', 'base_price_usd', 'base_price_myr'
    ]
    df_clean = df[final_columns]
    
    # Save the cleaned data to a new staging file
    output_path = "data/clean_games_data.csv"
    df_clean.to_csv(output_path, index=False)
    
    print(f"Clean DataFrame saved to {output_path}")
    print(f"Final data shape: {df_clean.shape}")
    print("\nTransformation COMPLETE. Ready for Loading.")
    return df_clean.head()

if __name__ == "__main__":
    print(transform_data())