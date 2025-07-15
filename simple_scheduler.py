import chainlit as cl
from report_scheduler import ReportScheduler

scheduler = ReportScheduler()

@cl.on_chat_start
async def start_chat():
    await cl.Message(
        content="📊 **Report Scheduler**\n\n" +
                "**Commands:**\n" +
                "• `daily: Employee Count|09:00|SELECT COUNT(*) FROM employees`\n" +
                "• `weekly: Weekly Report|MON:09:00|SELECT COUNT(*) FROM employees`\n" +
                "• `hourly: System Check|SELECT COUNT(*) FROM employees`\n" +
                "• `show schedules` - View all scheduled reports\n" +
                "• `show results` - View recent report executions"
    ).send()

@cl.on_message
async def main(message: cl.Message):
    content = message.content.strip()
    
    if content.startswith("daily:"):
        # Parse: daily: name|time|query
        parts = content[6:].strip().split("|")
        if len(parts) == 3:
            name, time, query = [p.strip() for p in parts]
            report_id = scheduler.schedule_report(name, query, "db1", "daily", time)
            # Test run immediately
            scheduler._run_report(report_id)
            await cl.Message(content=f"✅ Daily report '{name}' scheduled for {time} (ID: {report_id})\n🧪 Test executed - check console").send()
        else:
            await cl.Message(content="❌ Format: `daily: name|HH:MM|query`").send()
            
    elif content.startswith("weekly:"):
        # Parse: weekly: name|DAY:HH:MM|query
        parts = content[7:].strip().split("|")
        if len(parts) == 3:
            name, day_time, query = [p.strip() for p in parts]
            report_id = scheduler.schedule_report(name, query, "db1", "weekly", day_time)
            await cl.Message(content=f"✅ Weekly report '{name}' scheduled for {day_time} (ID: {report_id})").send()
        else:
            await cl.Message(content="❌ Format: `weekly: name|DAY:HH:MM|query`").send()
            
    elif content.startswith("hourly:"):
        # Parse: hourly: name|query
        parts = content[7:].strip().split("|")
        if len(parts) == 2:
            name, query = [p.strip() for p in parts]
            report_id = scheduler.schedule_report(name, query, "db1", "hourly", "")
            await cl.Message(content=f"✅ Hourly report '{name}' scheduled (ID: {report_id})").send()
        else:
            await cl.Message(content="❌ Format: `hourly: name|query`").send()
            
    elif any(word in content.lower() for word in ["schedul", "sched"]) and any(w in content.lower() for w in ["show", "view", "list", "see"]):
        reports = scheduler.get_scheduled_reports()
        if reports:
            report_list = "📅 **Scheduled Reports:**\n\n"
            for report in reports:
                report_list += f"• {report[1]} - {report[4]} at {report[5]}\n"
            await cl.Message(content=report_list).send()
        else:
            await cl.Message(content="No scheduled reports found.").send()
            
    elif any(word in content.lower() for word in ["result", "reslt", "resl"]) and any(w in content.lower() for w in ["show", "view", "list", "see"]):
        import sqlite3
        conn = sqlite3.connect(scheduler.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM report_results ORDER BY run_time DESC LIMIT 5')
        results = cursor.fetchall()
        conn.close()
        
        if results:
            result_list = "📊 **Recent Report Results:**\n\n"
            for result in results:
                result_list += f"• {result[2]}\n"
            await cl.Message(content=result_list).send()
        else:
            await cl.Message(content="No report results found.").send()
            
    else:
        await cl.Message(content="❓ Use commands like: `daily: Employee Count|09:00|SELECT COUNT(*) FROM employees`").send()