import hashlib
import json
from datetime import datetime, timezone

def compute_hash(restaurant):
    fields = {
        "name":         restaurant.get("name"),
        "rating":       restaurant.get("rating"),
        "review_count": restaurant.get("review_count"),
        "price_level":  restaurant.get("price_level"),
        "website":      restaurant.get("website"),
        "address":      restaurant.get("address"),
        "types":        restaurant.get("types"),
        "hours":        restaurant.get("hours"),
        "photo_url":    restaurant.get("photo_url"),
    }
    restaurant_data = json.dumps(fields, sort_keys=True)
    return hashlib.md5(restaurant_data.encode()).hexdigest()

def load_stored_hashes(cursor):
    cursor.execute("""
                   SELECT place_id, hash, name, rating, review_count,
                   price_level, website, address, types, hours, photo_url
                   FROM DINE_SCOUT.RAW.RESTAURANT_HASHES
    """)
    return {row[0]: {"hash": row[1], "data": {
        "name": row[2],
        "rating": row[3],
        "review_count": row[4],
        "price_level": row[5],
        "website": row[6],
        "address": row[7],
        "types": row[8],
        "hours": row[9],
        "photo_url": row[10]
    }} for row in cursor.fetchall()}

def detect_changes(restaurants, stored_hashes):

    cdc_events = []
    hash_updates = []
    time_updated = datetime.now(timezone.utc)


    for restaurant in restaurants:
        place_id = restaurant["place_id"]
        updated_hash = compute_hash(restaurant)
        new_data = json.dumps({
            "name": restaurant.get("name"),
            "rating": restaurant.get("rating"),
            "review_count": restaurant.get("review_count"),
            "price_level": restaurant.get("price_level"),
            "website": restaurant.get("website"),
            "address": restaurant.get("address"),
            "types": restaurant.get("types"),
            "hours": restaurant.get("hours"),
            "photo_url": restaurant.get("photo_url")
        }, sort_keys=True)
        
        

        if place_id not in stored_hashes:
            cdc_events.append((
                place_id,
                "INSERT",
                None,
                new_data,
                time_updated

            ))

            
        elif updated_hash != stored_hashes[place_id]["hash"]:
            cdc_events.append((
                place_id,
                "UPDATE",
                json.dumps(stored_hashes[place_id]["data"]),
                new_data,
                time_updated

            ))
        hash_updates.append((
            place_id,
            updated_hash,
            restaurant.get("name"),
            restaurant.get("rating"),
            restaurant.get("review_count"),
            restaurant.get("price_level"),
            restaurant.get("website"),
            restaurant.get("address"),
            restaurant.get("types"),
            restaurant.get("hours"),
            restaurant.get("photo_url"),
            time_updated
        ))

    return cdc_events, hash_updates

    
def save_hashes(cursor, hash_updates):
    if not hash_updates:
        return
    cursor.executemany("""
            MERGE INTO DINE_SCOUT.RAW.restaurant_hashes AS target
            USING (SELECT %s as place_id) AS source
            ON target.place_id = source.place_id
            WHEN MATCHED THEN UPDATE SET
            hash = %s,
            name = %s,
            rating = %s,
            review_count = %s,
            price_level = %s,
            website = %s,
            address = %s,
            types = %s,
            hours = %s,
            photo_url = %s,
            last_seen = %s
            WHEN NOT MATCHED THEN INSERT (
            place_id, hash, name, rating, review_count,
            price_level, website, address, types, hours, photo_url, last_seen
            ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s
            )
        """, [(
            update[0],  # place_id for USING clause
            update[1],  # hash for UPDATE
            update[2],  # name for UPDATE
            update[3],  # rating for UPDATE
            update[4],  # review_count for UPDATE
            update[5],  # price_level for UPDATE
            update[6],  # website for UPDATE
            update[7],  # address for UPDATE
            update[8],  # types for UPDATE
            update[9],  # hours for UPDATE
            update[10], # photo_url for UPDATE
            update[11], # last_seen for UPDATE
            update[0],  # place_id for INSERT
            update[1],  # hash for INSERT
            update[2],  # name for INSERT
            update[3],  # rating for INSERT
            update[4],  # review_count for INSERT
            update[5],  # price_level for INSERT
            update[6],  # website for INSERT
            update[7],  # address for INSERT
            update[8],  # types for INSERT
            update[9],  # hours for INSERT
            update[10], # photo_url for INSERT
            update[11] ) # last_seen for INSERT
         for update in hash_updates])
    
def write_cdc_events(cursor, cdc_events):
    if not cdc_events:
        return
    cursor.executemany("""
        INSERT INTO DINE_SCOUT.RAW.cdc_events (place_id, change_type, old_value, new_value, detected_at)
        VALUES (%s, %s, %s, %s, %s)
    """, cdc_events)

def run_cdc(restaurants):
    from ingestion.loaders.snowflake_loader import get_snowflake_connection
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        stored_hashes = load_stored_hashes(cursor)
        cdc_events, hash_updates = detect_changes(restaurants, stored_hashes)
        write_cdc_events(cursor, cdc_events)
        save_hashes(cursor, hash_updates)
        conn.commit()
        print(f"CDC completed: {len(cdc_events)} changes detected, {len(hash_updates)} hashes updated.")
    except Exception as e:
        print(f"Error running CDC: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


    
    