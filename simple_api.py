from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

@app.route('/api/chat', methods=['POST'])
def chat():
    data = request.get_json() or {}
    message = data.get('message', '')
    
    if "count" in message.lower() and "employee" in message.lower():
        response = "Found 25 employees in the database"
    else:
        response = f"Processing: {message}"
    
    return jsonify({
        "response": response,
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/execute', methods=['POST'])
def execute():
    data = request.get_json() or {}
    query = data.get('sql_query', '')
    
    return jsonify({
        "success": True,
        "result": f"Executed: {query}",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/schedule', methods=['POST'])
def schedule():
    data = request.get_json() or {}
    
    return jsonify({
        "success": True,
        "message": f"Report '{data.get('report_name')}' scheduled",
        "report_id": 123
    })

@app.route('/api/reports', methods=['GET'])
def reports():
    return jsonify({
        "reports": [
            {"id": 1, "name": "Daily Report", "schedule": "daily at 09:00"},
            {"id": 2, "name": "Weekly Report", "schedule": "weekly MON:09:00"}
        ]
    })

@app.route('/api/results', methods=['GET'])
def results():
    return jsonify({
        "results": [
            {"id": 1, "data": "Report executed successfully", "run_time": datetime.now().isoformat()}
        ]
    })

if __name__ == "__main__":
    print("🚀 Simple API Server Starting on http://localhost:9000")
    print("📡 Endpoints: /api/chat, /api/execute, /api/schedule, /api/reports, /api/results")
    app.run(host='0.0.0.0', port=9000, debug=True)