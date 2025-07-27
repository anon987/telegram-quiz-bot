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
    
    # Main table for all-time scores
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_scores (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            correct_answers INT DEFAULT 0,
            wrong_answers INT DEFAULT 0
        )
    ''')
    
    # Table for session-specific scores
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS session_scores (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL,
            username TEXT,
            session_id TEXT NOT NULL,
            correct_answers INT DEFAULT 0,
            wrong_answers INT DEFAULT 0,
            UNIQUE(user_id, session_id)
        )
    ''')
    
    # Tables for time-based scores
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_scores (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            correct_answers INT DEFAULT 0,
            wrong_answers INT DEFAULT 0
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS weekly_scores (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            correct_answers INT DEFAULT 0,
            wrong_answers INT DEFAULT 0
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS monthly_scores (
            user_id BIGINT PRIMARY KEY,
            username TEXT,
            correct_answers INT DEFAULT 0,
            wrong_answers INT DEFAULT 0
        )
    ''')
    
    # We will keep the answer log for detailed history, but it won't be used for leaderboards.
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
    
    conn.commit()
    cursor.close()
    conn.close()

def log_answer(user_id, username, is_correct, session_id):
    """Logs an answer and updates the summary tables."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Log the individual answer
    cursor.execute(
        "INSERT INTO answer_log (user_id, username, is_correct, session_id) VALUES (%s, %s, %s, %s)",
        (user_id, username, is_correct, session_id)
    )
    
    # Update all-time scores
    if is_correct:
        cursor.execute(
            "INSERT INTO user_scores (user_id, username, correct_answers) VALUES (%s, %s, 1) ON CONFLICT (user_id) DO UPDATE SET correct_answers = user_scores.correct_answers + 1, username = EXCLUDED.username",
            (user_id, username)
        )
    else:
        cursor.execute(
            "INSERT INTO user_scores (user_id, username, wrong_answers) VALUES (%s, %s, 1) ON CONFLICT (user_id) DO UPDATE SET wrong_answers = user_scores.wrong_answers + 1, username = EXCLUDED.username",
            (user_id, username)
        )
        
    # Update session scores
    if is_correct:
        cursor.execute(
            "INSERT INTO session_scores (user_id, username, session_id, correct_answers) VALUES (%s, %s, %s, 1) ON CONFLICT (user_id, session_id) DO UPDATE SET correct_answers = session_scores.correct_answers + 1, username = EXCLUDED.username",
            (user_id, username, session_id)
        )
    else:
        cursor.execute(
            "INSERT INTO session_scores (user_id, username, session_id, wrong_answers) VALUES (%s, %s, %s, 1) ON CONFLICT (user_id, session_id) DO UPDATE SET wrong_answers = session_scores.wrong_answers + 1, username = EXCLUDED.username",
            (user_id, username, session_id)
        )
        
    conn.commit()
    cursor.close()
    conn.close()

def get_leaderboard(time_frame='all', session_id=None):
    """
    Retrieves the leaderboard data from the appropriate summary table.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    if session_id:
        query = "SELECT username, correct_answers as correct, wrong_answers as wrong FROM session_scores WHERE session_id = %s ORDER BY correct DESC, wrong ASC LIMIT 100"
        cursor.execute(query, (session_id,))
    elif time_frame == 'daily':
        query = "SELECT username, correct_answers as correct, wrong_answers as wrong FROM daily_scores ORDER BY correct DESC, wrong ASC LIMIT 100"
        cursor.execute(query)
    elif time_frame == 'weekly':
        query = "SELECT username, correct_answers as correct, wrong_answers as wrong FROM weekly_scores ORDER BY correct DESC, wrong ASC LIMIT 100"
        cursor.execute(query)
    elif time_frame == 'monthly':
        query = "SELECT username, correct_answers as correct, wrong_answers as wrong FROM monthly_scores ORDER BY correct DESC, wrong ASC LIMIT 100"
        cursor.execute(query)
    else: # all-time
        query = "SELECT username, correct_answers as correct, wrong_answers as wrong FROM user_scores ORDER BY correct DESC, wrong ASC LIMIT 100"
        cursor.execute(query)
        
    leaderboard = cursor.fetchall()
    cursor.close()
    conn.close()
    return leaderboard
