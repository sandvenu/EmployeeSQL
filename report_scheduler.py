import sqlite3
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
import json

class ReportScheduler:
    def __init__(self, db_path="scheduled_reports.db"):
        self.db_path = db_path
        self.scheduler = BackgroundScheduler()
        self.init_database()
        self.scheduler.start()
    
    def init_database(self):
        """Initialize scheduler database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scheduled_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                report_name TEXT,
                sql_query TEXT,
                database_name TEXT,
                schedule_type TEXT,
                schedule_time TEXT,
                last_run DATETIME,
                next_run DATETIME,
                is_active BOOLEAN DEFAULT 1,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS report_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                report_id INTEGER,
                result_data TEXT,
                run_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (report_id) REFERENCES scheduled_reports (id)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def schedule_report(self, report_name: str, sql_query: str, db_name: str, 
                       schedule_type: str, schedule_time: str):
        """Schedule a new report"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Calculate next run time
        next_run = self._calculate_next_run(schedule_type, schedule_time)
        
        cursor.execute('''
            INSERT INTO scheduled_reports 
            (report_name, sql_query, database_name, schedule_type, schedule_time, next_run)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (report_name, sql_query, db_name, schedule_type, schedule_time, next_run))
        
        report_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        # Add to scheduler
        self._add_to_scheduler(report_id, schedule_type, schedule_time)
        
        return report_id
    
    def _calculate_next_run(self, schedule_type: str, schedule_time: str):
        """Calculate next run time"""
        now = datetime.now()
        
        if schedule_type == "daily":
            hour, minute = map(int, schedule_time.split(':'))
            next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if next_run <= now:
                next_run += timedelta(days=1)
                
        elif schedule_type == "weekly":
            # Format: "MON:14:30"
            day, time = schedule_time.split(':')[:2]
            hour, minute = int(schedule_time.split(':')[1]), int(schedule_time.split(':')[2])
            days_ahead = ['MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN'].index(day) - now.weekday()
            if days_ahead <= 0:
                days_ahead += 7
            next_run = now + timedelta(days=days_ahead)
            next_run = next_run.replace(hour=hour, minute=minute, second=0, microsecond=0)
            
        else:  # hourly
            next_run = now + timedelta(hours=1)
            
        return next_run
    
    def _add_to_scheduler(self, report_id: int, schedule_type: str, schedule_time: str):
        """Add job to APScheduler"""
        if schedule_type == "daily":
            hour, minute = map(int, schedule_time.split(':'))
            self.scheduler.add_job(
                self._run_report, 'cron', 
                hour=hour, minute=minute,
                args=[report_id], id=f"report_{report_id}"
            )
        elif schedule_type == "hourly":
            self.scheduler.add_job(
                self._run_report, 'interval', 
                hours=1, args=[report_id], id=f"report_{report_id}"
            )
    
    def _run_report(self, report_id: int):
        """Execute scheduled report"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get report details
        cursor.execute('SELECT * FROM scheduled_reports WHERE id = ?', (report_id,))
        report = cursor.fetchone()
        
        if not report:
            return
        
        # Execute query (simplified - would use your existing DB functions)
        result_data = f"Report executed at {datetime.now()}"
        
        # Save result
        cursor.execute('''
            INSERT INTO report_results (report_id, result_data)
            VALUES (?, ?)
        ''', (report_id, result_data))
        
        # Update last run
        cursor.execute('''
            UPDATE scheduled_reports 
            SET last_run = CURRENT_TIMESTAMP 
            WHERE id = ?
        ''', (report_id,))
        
        conn.commit()
        conn.close()
    
    def get_scheduled_reports(self):
        """Get all scheduled reports"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM scheduled_reports WHERE is_active = 1')
        reports = cursor.fetchall()
        conn.close()
        
        return reports
    
    def stop_scheduler(self):
        """Stop the scheduler"""
        self.scheduler.shutdown()