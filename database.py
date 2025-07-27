import psycopg2
import os
from datetime import datetime, timedelta
from psycopg2.extras import RealDictCursor

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    return conn

def initialize_database():
    """Initializes the database and creates the necessary tables if they don't exist."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS answer_log (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            username TEXT,
            is_correct BOOLEAN NOT NULL,
            session_id TEXT NOT NULL,
            timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Add an index to the timestamp column for faster time-based queries
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_answer_log_timestamp ON answer_log (timestamp);')
    
    conn.commit()
    cursor.close()
    conn.close()

def log_answer(user_id, username, is_correct, session_id):
    """Logs a user's answer in the database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute(
        "INSERT INTO answer_log (user_id, username, is_correct, session_id) VALUES (%s, %s, %s, %s)",
        (user_id, username, is_correct, session_id)
    )
    
    conn.commit()
    cursor.close()
    conn.close()

def get_leaderboard(time_frame='all', session_id=None):
    """
    Retrieves the leaderboard data from the database based on the specified time frame or session_id.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    query = "SELECT user_id, username, SUM(CASE WHEN is_correct THEN 1 ELSE 0 END) as correct, SUM(CASE WHEN NOT is_correct THEN 1 ELSE 0 END) as wrong FROM answer_log"
    
    conditions = []
    params = []
    
    if session_id:
        conditions.append("session_id = %s")
        params.append(session_id)
    elif time_frame != 'all':
        now = datetime.utcnow()
        if time_frame == 'daily':
            start_date = now - timedelta(days=1)
        elif time_frame == 'weekly':
            start_date = now - timedelta(weeks=1)
        elif time_frame == 'monthly':
            start_date = now - timedelta(days=30)
        else: # yearly
            start_date = now - timedelta(days=365)
        
        conditions.append("timestamp >= %s")
        params.append(start_date)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " GROUP BY user_id, username ORDER BY correct DESC, wrong ASC LIMIT 100"
    
    cursor.execute(query, tuple(params))
    leaderboard = cursor.fetchall()
    cursor.close()
    conn.close()
    return leaderboard
