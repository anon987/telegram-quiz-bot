import psycopg2
import os
from datetime import datetime, timedelta
from psycopg2.extras import RealDictCursor

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    return conn

def calculate_scores(time_frame):
    """Calculates scores for a given time frame and updates the corresponding summary table."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    now = datetime.utcnow()
    if time_frame == 'daily':
        start_date = now - timedelta(days=1)
        table_name = 'daily_scores'
    elif time_frame == 'weekly':
        start_date = now - timedelta(weeks=1)
        table_name = 'weekly_scores'
    elif time_frame == 'monthly':
        start_date = now - timedelta(days=30)
        table_name = 'monthly_scores'
    else:
        return

    # Clear the summary table
    cursor.execute(f"TRUNCATE TABLE {table_name}")
    
    # Calculate new scores
    cursor.execute(f"""
        INSERT INTO {table_name} (user_id, username, correct_answers, wrong_answers)
        SELECT 
            user_id, 
            username, 
            SUM(CASE WHEN is_correct THEN 1 ELSE 0 END),
            SUM(CASE WHEN NOT is_correct THEN 1 ELSE 0 END)
        FROM answer_log
        WHERE timestamp >= %s
        GROUP BY user_id, username
    """, (start_date,))
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Successfully calculated {time_frame} scores.")

if __name__ == "__main__":
    today = datetime.utcnow()
    
    # Always calculate daily scores
    calculate_scores('daily')
    
    # If it's Sunday, calculate weekly scores
    if today.weekday() == 6: # Sunday
        calculate_scores('weekly')
        
    # If it's the first day of the month, calculate monthly scores
    if today.day == 1:
        calculate_scores('monthly')
