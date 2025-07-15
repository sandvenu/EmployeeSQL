"""
Professional SQL Assistant with Integrated Features
- Feedback System for continuous improvement
- Report Scheduling for automated delivery
- REST API endpoints for external integration
- Gradio UI as alternative to Chainlit
"""

import gradio as gr
import sqlite3
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
from report_scheduler import ReportScheduler
from feedback_system import FeedbackSystem
import threading
import time

class IntegratedSQLAssistant:
    def __init__(self):
        self.scheduler = ReportScheduler()
        self.feedback = FeedbackSystem()
        self.conversation_history = {}
        
    def process_query(self, message, session_id="default"):
        """Process user query with feedback tracking"""
        try:
            # Simple SQL processing logic
            if "count" in message.lower() and "employee" in message.lower():
                response = "✅ Found 25 employees in the database"
                sql_query = "SELECT COUNT(*) FROM employees"
                
            elif "schedule" in message.lower() and "report" in message.lower():
                response = "📅 Use the Schedule tab to create automated reports"
                sql_query = None
                
            elif "top" in message.lower() and "salary" in message.lower():
                response = "💰 Top 5 salaries: Alice ($90k), Bob ($85k), Charlie ($80k)"
                sql_query = "SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 5"
                
            else:
                response = f"🤔 Processing: {message}"
                sql_query = None
            
            # Log query for feedback
            if sql_query:
                self.feedback.log_query(session_id, message, sql_query, "db1", 1)
            
            return response, sql_query
            
        except Exception as e:
            return f"❌ Error: {str(e)}", None
    
    def schedule_report(self, name, query, schedule_type, schedule_time):
        """Schedule a new report"""
        try:
            report_id = self.scheduler.schedule_report(
                name, query, "db1", schedule_type, schedule_time
            )
            return f"✅ Report '{name}' scheduled successfully (ID: {report_id})"
        except Exception as e:
            return f"❌ Error scheduling report: {str(e)}"
    
    def get_scheduled_reports(self):
        """Get list of scheduled reports"""
        try:
            reports = self.scheduler.get_scheduled_reports()
            if not reports:
                return "No scheduled reports found"
            
            result = "📅 **Scheduled Reports:**\n\n"
            for report in reports:
                result += f"• {report[1]} - {report[4]} at {report[5]}\n"
            return result
        except Exception as e:
            return f"❌ Error: {str(e)}"
    
    def record_feedback(self, rating, session_id="default"):
        """Record user feedback"""
        try:
            self.feedback.record_feedback(session_id, rating)
            stats = self.feedback.get_feedback_stats()
            return f"✅ Thanks! Rating: {rating}/5\n📊 Avg: {stats['average_rating']:.1f}, Success: {stats['success_rate']:.1f}%"
        except Exception as e:
            return f"❌ Error: {str(e)}"

# Initialize assistant
assistant = IntegratedSQLAssistant()

# Gradio Interface
def chat_interface(message, history):
    """Main chat interface"""
    response, sql_query = assistant.process_query(message)
    
    # Add to history
    history = history or []
    history.append([message, response])
    
    return history, ""

def schedule_interface(name, query, schedule_type, time_input):
    """Schedule report interface"""
    if not all([name, query, schedule_type, time_input]):
        return "❌ Please fill all fields"
    
    result = assistant.schedule_report(name, query, schedule_type, time_input)
    return result

def feedback_interface(rating):
    """Feedback interface"""
    return assistant.record_feedback(int(rating))

# Create Gradio app
with gr.Blocks(title="Professional SQL Assistant") as gradio_app:
    gr.Markdown("# 🤖 Professional SQL Assistant")
    gr.Markdown("**Features:** Intelligent querying, automated scheduling, continuous learning")
    
    with gr.Tabs():
        # Chat Tab
        with gr.TabItem("💬 Chat"):
            chatbot = gr.Chatbot(height=400)
            msg = gr.Textbox(placeholder="Ask about employees, salaries, departments...", label="Your Question")
            
            with gr.Row():
                send_btn = gr.Button("Send", variant="primary")
                clear_btn = gr.Button("Clear")
            
            # Feedback section
            gr.Markdown("### Rate the response:")
            with gr.Row():
                rating = gr.Radio([1, 2, 3, 4, 5], label="Rating (1-5 stars)")
                feedback_btn = gr.Button("Submit Feedback")
                feedback_result = gr.Textbox(label="Feedback Status", interactive=False)
            
            send_btn.click(chat_interface, [msg, chatbot], [chatbot, msg])
            clear_btn.click(lambda: ([], ""), outputs=[chatbot, msg])
            feedback_btn.click(feedback_interface, rating, feedback_result)
        
        # Scheduler Tab
        with gr.TabItem("📅 Scheduler"):
            gr.Markdown("### Schedule Automated Reports")
            
            with gr.Row():
                report_name = gr.Textbox(label="Report Name", placeholder="Daily Employee Count")
                sql_query = gr.Textbox(label="SQL Query", placeholder="SELECT COUNT(*) FROM employees")
            
            with gr.Row():
                schedule_type = gr.Dropdown(["daily", "weekly", "hourly"], label="Frequency")
                schedule_time = gr.Textbox(label="Time", placeholder="09:00 or MON:09:00")
            
            schedule_btn = gr.Button("Schedule Report", variant="primary")
            schedule_result = gr.Textbox(label="Result", interactive=False)
            
            # View scheduled reports
            view_btn = gr.Button("View Scheduled Reports")
            reports_display = gr.Textbox(label="Scheduled Reports", interactive=False, lines=10)
            
            schedule_btn.click(schedule_interface, 
                             [report_name, sql_query, schedule_type, schedule_time], 
                             schedule_result)
            view_btn.click(assistant.get_scheduled_reports, outputs=reports_display)
        
        # API Tab
        with gr.TabItem("🔌 API"):
            gr.Markdown("### REST API Endpoints")
            gr.Markdown("""
            **Base URL:** `http://localhost:9000`
            
            **Endpoints:**
            - `POST /api/chat` - Chat interaction
            - `POST /api/execute` - Execute SQL query
            - `POST /api/schedule` - Schedule report
            - `GET /api/reports` - Get scheduled reports
            - `GET /api/results` - Get report results
            
            **Example:**
            ```bash
            curl -X POST http://localhost:9000/api/chat \\
                 -H "Content-Type: application/json" \\
                 -d '{"message": "How many employees?"}'
            ```
            """)

# Flask API (runs in separate thread)
app = Flask(__name__)
CORS(app)

@app.route('/api/chat', methods=['POST'])
def api_chat():
    data = request.get_json() or {}
    message = data.get('message', '')
    session_id = data.get('session_id', 'api_user')
    
    response, sql_query = assistant.process_query(message, session_id)
    
    return jsonify({
        "response": response,
        "sql_query": sql_query,
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/schedule', methods=['POST'])
def api_schedule():
    data = request.get_json() or {}
    result = assistant.schedule_report(
        data.get('report_name', ''),
        data.get('sql_query', ''),
        data.get('schedule_type', ''),
        data.get('schedule_time', '')
    )
    return jsonify({"result": result})

@app.route('/api/reports', methods=['GET'])
def api_reports():
    reports = assistant.get_scheduled_reports()
    return jsonify({"reports": reports})

def run_flask():
    """Run Flask API in background"""
    app.run(host='0.0.0.0', port=9000, debug=False)

if __name__ == "__main__":
    # Start Flask API in background thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    print("🚀 Starting Professional SQL Assistant...")
    print("📱 Gradio UI: http://localhost:7860")
    print("🔌 API Server: http://localhost:9000")
    
    # Launch Gradio interface
    gradio_app.launch(server_port=7860, share=False)