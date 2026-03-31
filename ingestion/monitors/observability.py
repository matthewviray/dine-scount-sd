import logging
from datetime import datetime
from ingestion.loaders.snowflake_loader import get_snowflake_connection

logger = logging.getLogger(__name__)

def check_row_count(**context):
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""SELECT COUNT(*) FROM
        DINE_SCOUT.RAW.raw_restaurants
        WHERE DATE(ingested_at) = CURRENT_DATE """)
        current_count = cursor.fetchone()[0]
        # Get 7 last averages of the row count
        cursor.execute("""SELECT AVG(daily_count) FROM (
            SELECT DATE(ingested_at), COUNT(*) AS daily_count
            FROM DINE_SCOUT.RAW.raw_restaurants
            GROUP BY DATE(ingested_at)
            ORDER BY DATE(ingested_at) DESC
            LIMIT 7)""")
        average_count = cursor.fetchone()[0]
        if not average_count:
            logger.info(f"No historical data to compare against. Today's row count: {current_count}.")
            return
        if current_count/average_count <= 0.8:
            logger.warning(f"Row count for today is {current_count}, which is significantly lower than the 7-day average of {average_count}.")
        else:
            logger.info(f"Row count for today is {current_count}, which is within the expected range compared to the 7-day average of {average_count}.")
    except Exception as e:
        logger.error(f"Error checking row count: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


            