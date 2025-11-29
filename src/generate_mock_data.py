import json
import os
import time
import random

def generate_mock_data():
    """Generates fake game data and a mock exchange rate."""

    # --- 1. Mock Game Metadata (mimics IGDB output) ---
    titles = ["The Witcher 4", "Elden Ring 2", "Horizon Zero Dawn 3", "Cyberpunk 2077 DLC", "Diablo V"]
    mock_games = []

    for i, title in enumerate(titles):
        # Genres need to be nested, just like IGDB's raw JSON
        mock_games.append({
            "id": 1000 + i,
            "name": title,
            "total_rating": random.uniform(85.0, 98.0), 
            "first_release_date": int(time.time()) - (i * 86400 * 30), # Recent Unix timestamp
            "genres": [
                {"name": "RPG"}, 
                {"name": random.choice(["Adventure", "Action", "Strategy"])}
            ]
        })

    # --- 2. Mock Exchange Rate ---
    # Current RM rate is ~4.7. We'll generate a random rate near that value.
    mock_rate = {"USD_MYR_Rate": round(random.uniform(4.6, 4.8), 4)}

    # --- 3. Save Files ---
    os.makedirs("data", exist_ok=True)

    with open("data/raw_games_metadata.json", "w") as f:
        json.dump(mock_games, f, indent=4)

    with open("data/exchange_rate.json", "w") as f:
        json.dump(mock_rate, f, indent=4)

    print(f"✅ Generated 5 mock game records and exchange rate. Data saved to the 'data/' folder.")

if __name__ == "__main__":
    generate_mock_data()