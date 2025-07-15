import json
import sqlite3
from datetime import datetime
from typing import Dict, List

class FeedbackSystem:
    def __init__(self, db_path="feedback.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize feedback database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS feedback (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT,
                user_question TEXT,
                generated_sql TEXT,
                database_used TEXT,
                result_count INTEGER,
                user_rating INTEGER,
                feedback_text TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS query_patterns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pattern TEXT UNIQUE,
                successful_sql TEXT,
                success_count INTEGER DEFAULT 1,
                last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def log_query(self, session_id: str, question: str, sql: str, db_name: str, result_count: int):
        """Log query execution"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO feedback (session_id, user_question, generated_sql, database_used, result_count)
            VALUES (?, ?, ?, ?, ?)
        ''', (session_id, question, sql, db_name, result_count))
        
        conn.commit()
        conn.close()
    
    def record_feedback(self, session_id: str, rating: int, feedback_text: str = ""):
        """Record user feedback for last query"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE feedback 
            SET user_rating = ?, feedback_text = ?
            WHERE id = (
                SELECT id FROM feedback 
                WHERE session_id = ? AND user_rating IS NULL 
                ORDER BY timestamp DESC LIMIT 1
            )
        ''', (rating, feedback_text, session_id))
        
        # If positive feedback, save as successful pattern
        if rating >= 4:
            cursor.execute('''
                SELECT user_question, generated_sql FROM feedback 
                WHERE session_id = ? AND user_rating = ?
                ORDER BY timestamp DESC LIMIT 1
            ''', (session_id, rating))
            
            result = cursor.fetchone()
            if result:
                question, sql = result
                self._save_successful_pattern(question, sql, cursor)
        
        conn.commit()
        conn.close()
    
    def _save_successful_pattern(self, question: str, sql: str, cursor):
        """Save successful query pattern"""
        # Extract pattern from question (simplified)
        pattern = self._extract_pattern(question)
        
        cursor.execute('''
            INSERT OR REPLACE INTO query_patterns (pattern, successful_sql, success_count, last_updated)
            VALUES (?, ?, COALESCE((SELECT success_count FROM query_patterns WHERE pattern = ?) + 1, 1), CURRENT_TIMESTAMP)
        ''', (pattern, sql, pattern))
    
    def _extract_pattern(self, question: str) -> str:
        """Extract pattern from user question"""
        # Simple pattern extraction
        keywords = ['count', 'show', 'list', 'top', 'highest', 'department', 'salary', 'employee']
        found_keywords = [kw for kw in keywords if kw in question.lower()]
        return ' '.join(sorted(found_keywords))
    
    def get_similar_successful_queries(self, question: str) -> List[str]:
        """Get successful SQL queries for similar patterns"""
        pattern = self._extract_pattern(question)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT successful_sql, success_count FROM query_patterns 
            WHERE pattern = ? ORDER BY success_count DESC LIMIT 3
        ''', (pattern,))
        
        results = cursor.fetchall()
        conn.close()
        
        return [sql for sql, _ in results]
    
    def get_feedback_stats(self) -> Dict:
        """Get feedback statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT AVG(user_rating), COUNT(*) FROM feedback WHERE user_rating IS NOT NULL')
        avg_rating, total_feedback = cursor.fetchone()
        
        cursor.execute('SELECT COUNT(*) FROM feedback WHERE user_rating >= 4')
        positive_feedback = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            'average_rating': avg_rating or 0,
            'total_feedback': total_feedback or 0,
            'positive_feedback': positive_feedback or 0,
            'success_rate': (positive_feedback / total_feedback * 100) if total_feedback > 0 else 0
        }