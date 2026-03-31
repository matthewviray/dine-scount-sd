import os
import json
from datetime import datetime
from dotenv import load_dotenv
from ingestion.extractors.google_extractor import extract_restaurants
from ingestion.loaders.cdc_handler import run_cdc
import snowflake.connector

load_dotenv()

def get_snowflake_connection():
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role=os.getenv("SNOWFLAKE_ROLE")
    )
    return conn

def load_restaurants(restaurants): 
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        data = []
        for restaurant in restaurants:
            row = (
                restaurant["place_id"],
                restaurant["name"],
                restaurant["rating"],
                restaurant["review_count"],
                restaurant["price_level"],
                restaurant["website"],
                restaurant["photo_url"],
                restaurant["address"],
                restaurant["types"],
                restaurant["hours"],
                restaurant["lat"],
                restaurant["lng"],
                restaurant["neighborhood"],
                restaurant["ingested_at"],
                restaurant["place_id"],
                restaurant["name"],
                restaurant["rating"],
                restaurant["review_count"],
                restaurant["price_level"],
                restaurant["website"],
                restaurant["photo_url"],
                restaurant["address"],
                restaurant["types"],
                restaurant["hours"],
                restaurant["lat"],
                restaurant["lng"],
                restaurant["neighborhood"],
                restaurant["ingested_at"])
            data.append(row)
        cursor.executemany("""
                MERGE INTO DINE_SCOUT.RAW.RAW_RESTAURANTS AS target
                USING (SELECT %s AS place_id) AS source
                ON target.place_id = source.place_id
                WHEN MATCHED THEN UPDATE SET
                    name = %s,
                    rating = %s,
                    review_count = %s,
                    price_level = %s,
                    website = %s,
                    photo_url = %s,
                    address = %s,
                    types = PARSE_JSON(%s),
                    hours = PARSE_JSON(%s),
                    lat = %s,
                    lng = %s,
                    neighborhood = %s,
                    ingested_at = %s
                WHEN NOT MATCHED THEN INSERT (
                    place_id, name, rating, review_count,
                    price_level, website, photo_url, address,
                    types, hours, lat, lng, neighborhood, ingested_at
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    PARSE_JSON(%s), PARSE_JSON(%s), %s, %s, %s, %s
                )
            """, data)
                
        conn.commit()
        print(f"Successfully loaded {len(restaurants)} restaurants into snowflake")
    except Exception as e:
        print(f"Error loading restaurants into snowflake: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def load_photos(restaurants):
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        data = []
        for restaurant in restaurants:
            if restaurant["photo_url"]:
                data.append((
                    restaurant["place_id"],
                    restaurant["photo_url"],
                    restaurant["ingested_at"],
                    restaurant["place_id"],
                    restaurant["photo_url"],
                    restaurant["ingested_at"]
                ))
        cursor.executemany("""
            MERGE INTO DINE_SCOUT.RAW.RAW_RESTAURANT_PHOTOS AS target
            USING (SELECT %s AS place_id) AS source
                    ON target.place_id = source.place_id
                    WHEN MATCHED THEN UPDATE SET
                    photo_resource_name = %s,
                    ingested_at = %s
                    WHEN NOT MATCHED THEN INSERT (
                    place_id, photo_resource_name, ingested_at)
                    VALUES (%s, %s, %s)
                """, data)
        conn.commit()
        print(f"Successfully loaded {len(restaurants)} restaurants photos into snowflake")
    except Exception as e:
        print(f"Error loading restaurants photos into snowflake: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    restaurants = extract_restaurants()
    print(f"Extracted {len(restaurants)} restaurants")

    print(f"Starting to load Restaurants into Snowflake")
    load_restaurants(restaurants)

    print(f"Starting to load Photos into Snowflake")
    load_photos(restaurants)
    print("Data loading complete")

    print("Starting CDC")
    run_cdc(restaurants)
    print("CDC complete")