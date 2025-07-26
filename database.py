import sqlite3
import os
from datetime import datetime, timedelta

def get_db_connection():
    """Establishes a connection to the SQLite database."""
    # The persistent disk is mounted at /data. We'll create a subdirectory for our db.
    db_dir = '/data/database'
    os.makedirs(db_dir, exist_ok=True)
    
    db_path = os.path.join(db_dir, 'quiz_bot.db')
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def initialize_database():
    """Initializes the database and creates the necessary tables if they don't exist."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS answer_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT,
            is_correct BOOLEAN NOT NULL,
            session_id TEXT NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

def log_answer(user_id, username, is_correct, session_id):
    """Logs a user's answer in the database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO answer_log (user_id, username, is_correct, session_id) VALUES (?, ?, ?, ?)",
        (user_id, username, is_correct, session_id)
    )
    conn.commit()
    conn.close()

def get_leaderboard(time_frame='all', session_id=None):
    """
    Retrieves the leaderboard data from the database based on the specified time frame or session_id.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    query = "SELECT user_id, username, SUM(is_correct) as correct, COUNT(*) - SUM(is_correct) as wrong FROM answer_log"
    
    conditions = []
    if session_id:
        conditions.append(f"session_id = '{session_id}'")
    elif time_frame != 'all':
        now = datetime.utcnow()
        if time_frame == 'daily':
            start_date = now - timedelta(days=1)
        elif time_frame == 'weekly':
            start_date = now - timedelta(weeks=1)
        elif time_frame == 'monthly':
            start_date = now - timedelta(days=30)
        elif time_frame == 'yearly':
            start_date = now - timedelta(days=365)
        else:
            start_date = None
        
        if start_date:
            conditions.append(f"timestamp >= '{start_date.strftime('%Y-%m-%d %H:%M:%S')}'")

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " GROUP BY user_id, username ORDER BY correct DESC, wrong ASC LIMIT 100"
    
    cursor.execute(query)
    leaderboard = cursor.fetchall()
    conn.close()
    return leaderboard
