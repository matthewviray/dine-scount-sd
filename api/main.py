from fastapi import FastAPI, Query
from dotenv import load_dotenv
from typing import Optional 
import snowflake.connector
import os

load_dotenv()
app = FastAPI()

def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role =os.getenv("SNOWFLAKE_ROLE")
    )
@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.get("/neighborhoods")
def get_neighborhoodss():
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT DISTINCT neighborhood
            FROM DINE_SCOUT.MARTS.mart_restaurant_scores
            ORDER BY NEIGHBORHOOD
        """)
        rows = cursor.fetchall()
        return [row[0] for row in rows]
    except Exception as e:
        return {"error": str(e)}
    finally:
        cursor.close()
        conn.close()

@app.get("/recommendations")
def get_recommendations(
    category: Optional[str] = Query(None),
    neighborhood: Optional[str] = Query(None),
    cuisine: Optional[str] = Query(None),
    price: Optional[str] = Query(None),
    limit: int = Query(20)
):
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    try:
        query = """
            SELECT
                place_id,
                name,
                rating,
                review_count,
                price_level,
                neighborhood,
                address,
                website,
                photo_url,
                all_types,
                hours,
                rating_score,
                popularity_score,
                velocity_score,
                hidden_gem_score,
                newcomer_score,
                is_best_overall,
                is_hot_right_now,
                is_hidden_gem,
                is_new_spot,
                first_seen_at
            FROM DINE_SCOUT.MARTS.mart_restaurant_scores
            WHERE 1=1"""
        
        params = []

        if category == "best_overall":
            query += " AND is_best_overall = true"
        elif category == "hot_right_now":
            query += " AND is_hot_right_now = true"
        elif category == "hidden_gem":
            query += " AND is_hidden_gem = true"
        elif category == "new_spot":
            query += " AND is_new_spot = true"
        
        if neighborhood:
            query += " AND LOWER(neighborhood) = LOWER(%s)"
            params.append(neighborhood)

        if cuisine:
            query += " AND LOWER(all_types::string) LIKE LOWER(%s)"
            params.append(f"%{cuisine}%")
        
        if price:
            query += " AND price_level = %s"
            params.append(price)

        if category == "hot_right_now":
            query += " ORDER BY velocity_score DESC"
        elif category == "hidden_gem":
            query += " ORDER BY hidden_gem_score DESC"
        elif category == "new_spot":
            query += " ORDER BY rating_score DESC"
        else:
            query += " ORDER BY rating_score DESC"
        
        query += " LIMIT %s"
        params.append(limit)
        cursor.execute(query, params)
        rows = cursor.fetchall()
        columns = [desc[0].lower() for desc in cursor.description]
        results = []
        for row in rows:
            results.append(dict(zip(columns, row)))
        return results
    except Exception as e:
        return {"error": str(e)}
    finally:
        cursor.close()
        conn.close()