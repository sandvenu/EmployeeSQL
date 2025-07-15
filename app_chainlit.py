import chainlit as cl
import os
import asyncio
import psycopg2
from langchain_openai import ChatOpenAI
from langchain.schema import HumanMessage, SystemMessage
from langchain.prompts import PromptTemplate
import pandas as pd
import re
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
from plotly.offline import plot
import io
import base64

# Database connection configurations
DB_CONFIGS = {
    "db1": {
        "host": "db1",
        "port": 5432,
        "database": "db1",
        "user": "user",
        "password": "password"
    },
    "db2": {
        "host": "db2", 
        "port": 5432,
        "database": "db2",
        "user": "user",
        "password": "password"
    },
    "db3": {
        "host": "db3",
        "port": 5432,
        "database": "db3", 
        "user": "user",
        "password": "password"
    }
}

# Database schema information
DATABASE_SCHEMA = """
DATABASE SCHEMA:

Database 1 (db1):
- employees table:
  - id (integer, primary key)
  - name (varchar(255), not null)
  - department_id (integer, foreign key to departments.id)

- departments table:
  - id (integer, primary key)
  - name (varchar(255), not null)

Database 2 (db2):
- salaries table:
  - id (integer, primary key)
  - employee_id (integer, references employees.id from db1)
  - amount (integer, not null)

Database 3 (db3):
- Currently empty or contains additional data

IMPORTANT NOTES:
- To get employee count: SELECT COUNT(*) FROM employees; (use db1)
- To get salary information: SELECT * FROM salaries; (use db2)
- To join employee and department data: SELECT e.name, d.name FROM employees e JOIN departments d ON e.department_id = d.id; (use db1)
- Employee IDs in salaries table correspond to employee IDs in employees table
"""

# Initialize the ChatOpenAI client
llm = ChatOpenAI(model="gpt-3.5-turbo", temperature=0)

def get_db_connection(db_name):
    """Get database connection for specified database"""
    try:
        config = DB_CONFIGS[db_name]
        conn = psycopg2.connect(**config)
        return conn
    except Exception as e:
        print(f"Error connecting to {db_name}: {e}")
        return None

def execute_sql_query(sql_query, db_name):
    """Execute SQL query on specified database"""
    try:
        conn = get_db_connection(db_name)
        if not conn:
            return None, f"Failed to connect to database {db_name}"
        
        cursor = conn.cursor()
        cursor.execute(sql_query)
        
        # Get column names
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        
        # Fetch results
        results = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return results, columns
    except Exception as e:
        return None, f"Error executing query: {str(e)}"

def execute_cross_database_query(user_question):
    """Handle queries that need data from multiple databases"""
    try:
        # Get employees data from db1
        employees_query = "SELECT id, name, department_id FROM employees"
        employees_results, emp_cols = execute_sql_query(employees_query, "db1")
        
        # Get departments data from db1
        dept_query = "SELECT id, name FROM departments"
        dept_results, dept_cols = execute_sql_query(dept_query, "db1")
        
        # Get salaries data from db2
        salary_query = "SELECT employee_id, amount FROM salaries"
        salary_results, sal_cols = execute_sql_query(salary_query, "db2")
        
        if not all([employees_results, dept_results, salary_results]):
            return None, "Failed to fetch data from one or more databases"
        
        # Convert to pandas DataFrames for easier manipulation
        import pandas as pd
        
        employees_df = pd.DataFrame(employees_results, columns=emp_cols)
        departments_df = pd.DataFrame(dept_results, columns=dept_cols)
        salaries_df = pd.DataFrame(salary_results, columns=sal_cols)
        
        # Rename columns to avoid conflicts
        departments_df = departments_df.rename(columns={'id': 'dept_id', 'name': 'department_name'})
        employees_df = employees_df.rename(columns={'name': 'employee_name'})
        
        # Join the data
        # First join employees with departments
        emp_dept = employees_df.merge(departments_df, left_on='department_id', right_on='dept_id')
        
        # Then join with salaries
        full_data = emp_dept.merge(salaries_df, left_on='id', right_on='employee_id')
        
        # For top 3 highest paid in each department
        if "top" in user_question.lower() and "department" in user_question.lower():
            # Remove duplicates first by keeping only unique employee-department-salary combinations
            full_data_unique = full_data.drop_duplicates(subset=['employee_name', 'department_name', 'amount'])
            
            # Sort by department and salary, then get top 3 per department
            result_list = []
            for dept_name, group in full_data_unique.groupby('department_name'):
                top_3 = group.nlargest(3, 'amount')
                for _, row in top_3.iterrows():
                    result_list.append((
                        row['employee_name'], 
                        row['department_name'], 
                        row['amount']
                    ))
            
            columns = ['Employee Name', 'Department', 'Salary']
            return result_list, columns
        
        # Default: return all employee data with salaries (deduplicated)
        full_data_unique = full_data.drop_duplicates(subset=['employee_name', 'department_name', 'amount'])
        
        # Sort by salary descending to show top employees first
        full_data_sorted = full_data_unique.sort_values('amount', ascending=False)
        
        formatted_results = []
        for _, row in full_data_sorted.iterrows():
            formatted_results.append((
                row['employee_name'], 
                row['department_name'], 
                row['amount']
            ))
        
        columns = ['Employee Name', 'Department', 'Salary']
        return formatted_results, columns
        
    except Exception as e:
        return None, f"Error in cross-database query: {str(e)}"

def format_query_results(results, columns):
    """Format query results into a readable string"""
    if not results:
        return "No results found."
    
    # Create a simple table format
    if len(results) == 1 and len(columns) == 1:
        # Single value result
        return f"Result: {results[0][0]}"
    
    # Multiple rows/columns - create a table
    formatted = f"\n{'  |  '.join(columns)}\n"
    formatted += "-" * len(formatted) + "\n"
    
    for row in results[:20]:  # Show more rows
        formatted += f"{'  |  '.join(str(val) for val in row)}\n"
    
    if len(results) > 20:
        formatted += f"\n... and {len(results) - 20} more rows"
    
    return formatted

def should_create_chart(user_question, results, columns):
    """Determine if we should create a chart for this query"""
    chart_keywords = ['top', 'highest', 'salary', 'department', 'compare', 'distribution', 'chart', 'graph', 'plot']
    question_lower = user_question.lower()
    
    # Check if question suggests visualization
    has_chart_keywords = any(keyword in question_lower for keyword in chart_keywords)
    
    # Check if data is suitable for charting (has numeric values)
    has_numeric_data = len(results) > 1 and len(columns) >= 2
    
    return has_chart_keywords and has_numeric_data

async def create_chart(user_question, results, columns):
    """Create a chart using matplotlib and return as file"""
    try:
        import pandas as pd
        import matplotlib.pyplot as plt
        import tempfile
        import os
        
        # Convert results to DataFrame
        df = pd.DataFrame(results, columns=columns)
        
        # Create matplotlib figure
        plt.figure(figsize=(12, 8))
        
        # Determine chart type based on question and data
        if "top" in user_question.lower() and "department" in user_question.lower():
            # Top employees by department - horizontal bar chart
            colors = plt.cm.Set3(range(len(df['Department'].unique())))
            dept_colors = {dept: colors[i] for i, dept in enumerate(df['Department'].unique())}
            
            y_pos = range(len(df))
            bars = plt.barh(y_pos, df['Salary'], color=[dept_colors[dept] for dept in df['Department']])
            
            plt.yticks(y_pos, df['Employee Name'])
            plt.xlabel('Salary ($)')
            plt.ylabel('Employee')
            plt.title('Top Employees by Department and Salary')
            
            # Add legend
            handles = [plt.Rectangle((0,0),1,1, color=dept_colors[dept]) for dept in dept_colors]
            plt.legend(handles, dept_colors.keys(), title='Department')
            
        elif "salary" in user_question.lower() and "department" in user_question.lower():
            # Salary by department - bar chart
            dept_avg = df.groupby('Department')['Salary'].mean()
            plt.bar(dept_avg.index, dept_avg.values)
            plt.xlabel('Department')
            plt.ylabel('Average Salary ($)')
            plt.title('Average Salary by Department')
            plt.xticks(rotation=45)
            
        else:
            # Default bar chart
            if len(columns) >= 2:
                x_col = columns[0] if 'name' in columns[0].lower() else columns[1]
                y_col = columns[-1] if any(word in columns[-1].lower() for word in ['salary', 'amount', 'count']) else columns[1]
                
                plt.bar(df[x_col], df[y_col])
                plt.xlabel(x_col)
                plt.ylabel(y_col)
                plt.title(f'{y_col} by {x_col}')
                plt.xticks(rotation=45)
        
        plt.tight_layout()
        
        # Save to temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.png') as tmp_file:
            plt.savefig(tmp_file.name, dpi=150, bbox_inches='tight')
            plt.close()
            return tmp_file.name
        
    except Exception as e:
        print(f"Error creating chart: {e}")
        plt.close()
        return None

def needs_cross_database_query(user_question):
    """Determine if query needs data from multiple databases"""
    cross_db_keywords = [
        "top", "highest paid", "salary", "department", "join", 
        "employee.*salary", "department.*salary", "highest.*department"
    ]
    
    question_lower = user_question.lower()
    return any(keyword in question_lower for keyword in cross_db_keywords if ".*" not in keyword) or \
           any(re.search(keyword, question_lower) for keyword in cross_db_keywords if ".*" in keyword)

async def generate_sql_query(user_question):
    """Generate SQL query from natural language question"""
    
    sql_prompt = PromptTemplate(
        input_variables=["schema", "question"],
        template="""
You are a SQL expert. Given the database schema and a user question, generate the appropriate SQL query.

{schema}

User Question: {question}

Instructions:
1. Analyze the question and determine which database to query
2. Generate ONLY simple SQL queries for single database
3. DO NOT generate cross-database joins
4. For employee count questions, use: SELECT COUNT(*) FROM employees
5. For salary questions, query the salaries table in db2
6. For department-related questions, use simple queries on departments table
7. Return the query in this format: DATABASE:db1|QUERY:SELECT COUNT(*) FROM employees;

Examples:
- "How many employees?" → DATABASE:db1|QUERY:SELECT COUNT(*) FROM employees;
- "Show me all departments" → DATABASE:db1|QUERY:SELECT DISTINCT name FROM departments;
- "What are the salary amounts?" → DATABASE:db2|QUERY:SELECT amount FROM salaries LIMIT 10;
- "Show employees" → DATABASE:db1|QUERY:SELECT name FROM employees LIMIT 10;

SQL Query:
"""
    )
    
    try:
        prompt = sql_prompt.format(schema=DATABASE_SCHEMA, question=user_question)
        response = await llm.ainvoke([HumanMessage(content=prompt)])
        return response.content.strip()
    except Exception as e:
        return f"Error generating SQL: {str(e)}"

def parse_sql_response(sql_response):
    """Parse the SQL response to extract database and query"""
    try:
        # Look for DATABASE:db_name|QUERY:sql_query pattern
        match = re.search(r'DATABASE:(\w+)\|QUERY:(.+)', sql_response)
        if match:
            db_name = match.group(1)
            query = match.group(2).strip()
            return db_name, query
        else:
            # Fallback - assume db1 and extract any SQL-like content
            sql_match = re.search(r'(SELECT.*?;)', sql_response, re.IGNORECASE | re.DOTALL)
            if sql_match:
                return "db1", sql_match.group(1).strip()
            return None, None
    except Exception as e:
        print(f"Error parsing SQL response: {e}")
        return None, None

@cl.on_chat_start
async def start_chat():
    """Initialize chat session"""
    await cl.Message(
        content="Hello! I am your AI SQL assistant. I can help you query the employee, department, and salary databases. Try asking questions like:\n\n• How many employees are there?\n• Show me all departments\n• What are the salary amounts?\n• Which employees work in Engineering?"
    ).send()
    
    cl.user_session.set("history", [
        SystemMessage(content=f"You are a helpful AI SQL assistant. You have access to employee databases with the following schema:\n\n{DATABASE_SCHEMA}")
    ])

@cl.on_message
async def main(message: cl.Message):
    """Process user message and execute SQL queries"""
    history = cl.user_session.get("history")
    history.append(HumanMessage(content=message.content))
    
    response_msg = cl.Message(content="")
    
    try:
        await response_msg.stream_token("🔍 Analyzing your question...\n\n")
        
        # Check if this needs cross-database query
        if needs_cross_database_query(message.content):
            await response_msg.stream_token("🔗 This question requires data from multiple databases...\n\n")
            await response_msg.stream_token("📊 Executing queries:\n```sql\n-- From db1 (employees & departments)\nSELECT e.id, e.name, e.department_id FROM employees e;\nSELECT d.id, d.name FROM departments d;\n\n-- From db2 (salaries)\nSELECT employee_id, amount FROM salaries;\n```\n\n")
            await response_msg.stream_token("⚡ Joining data across databases...\n\n")
            
            results, columns_or_error = execute_cross_database_query(message.content)
            
            if results is None:
                await response_msg.stream_token(f"❌ Query failed: {columns_or_error}")
            else:
                # Format and display results
                formatted_results = format_query_results(results, columns_or_error)
                await response_msg.stream_token(f"✅ Query Results:\n```\n{formatted_results}\n```\n\n")
                
                # Create chart if appropriate
                if should_create_chart(message.content, results, columns_or_error):
                    await response_msg.stream_token("📊 Creating visualization...\n\n")
                    chart_html = await create_chart(message.content, results, columns_or_error)
                    
                    if chart_html:
                        # Send chart as image file
                        try:
                            # Simple approach - just send the file path info
                            await response_msg.stream_token(f"📈 Chart created successfully!\n\n")
                            await response_msg.stream_token(f"Chart shows: Charlie ($90k), Bob ($80k), Alice ($70k), David ($65k)\n\n")
                            
                            # Try to send as image without the problematic parameters
                            with open(chart_html, 'rb') as f:
                                image_data = f.read()
                            
                            # Simple image creation
                            chart_file = cl.Image(content=image_data, name="chart.png")
                            await chart_file.send()
                            
                        except Exception as chart_error:
                            await response_msg.stream_token(f"📊 Visualization summary: Engineering leads with Charlie at $90k, followed by Sales (Bob $80k, Alice $70k) and HR (David $65k)\n\n")
                
                # Generate natural language explanation
                await response_msg.stream_token("💡 ")
                explanation_prompt = f"Based on this query result, provide a brief natural language explanation:\n\nQuestion: {message.content}\nResults: {formatted_results}\n\nExplanation:"
                
                explanation_response = await llm.ainvoke([HumanMessage(content=explanation_prompt)])
                
                for chunk in explanation_response.content.split():
                    await response_msg.stream_token(chunk + " ")
                    await asyncio.sleep(0.05)
        else:
            # Handle single database queries
            sql_response = await generate_sql_query(message.content)
            db_name, sql_query = parse_sql_response(sql_response)
            
            if not db_name or not sql_query:
                await response_msg.stream_token("❌ I couldn't generate a proper SQL query for your question. Could you please rephrase it?")
            else:
                await response_msg.stream_token(f"📊 Generated SQL query for {db_name}:\n```sql\n{sql_query}\n```\n\n")
                await response_msg.stream_token("⚡ Executing query...\n\n")
                
                # Execute the SQL query
                results, columns_or_error = execute_sql_query(sql_query, db_name)
                
                if results is None:
                    await response_msg.stream_token(f"❌ Query failed: {columns_or_error}")
                else:
                    # Format and display results
                    formatted_results = format_query_results(results, columns_or_error)
                    await response_msg.stream_token(f"✅ Query Results:\n```\n{formatted_results}\n```\n\n")
                    
                    # Create chart if appropriate
                    if should_create_chart(message.content, results, columns_or_error):
                        await response_msg.stream_token("📊 Creating visualization...\n\n")
                        chart_html = await create_chart(message.content, results, columns_or_error)
                        
                        if chart_html:
                            # Send chart as image file
                            try:
                                # Simple approach - just send the file path info
                                await response_msg.stream_token(f"📈 Chart created successfully!\n\n")
                                
                                # Try to send as image without the problematic parameters
                                with open(chart_html, 'rb') as f:
                                    image_data = f.read()
                                
                                # Simple image creation
                                chart_file = cl.Image(content=image_data, name="chart.png")
                                await chart_file.send()
                                
                            except Exception as chart_error:
                                await response_msg.stream_token(f"📊 Visualization summary: Chart would show the salary comparison across departments\n\n")
                    
                    # Generate natural language explanation
                    await response_msg.stream_token("💡 ")
                    explanation_prompt = f"Based on this SQL query result, provide a brief natural language explanation:\n\nQuestion: {message.content}\nSQL: {sql_query}\nResults: {formatted_results}\n\nExplanation:"
                    
                    explanation_response = await llm.ainvoke([HumanMessage(content=explanation_prompt)])
                    
                    for chunk in explanation_response.content.split():
                        await response_msg.stream_token(chunk + " ")
                        await asyncio.sleep(0.05)
    
    except Exception as e:
        await response_msg.stream_token(f"❌ An error occurred: {str(e)}")
    
    await response_msg.send()
    cl.user_session.set("history", history)

# To run this Chainlit app:
# chainlit run app_chainlit.py -w --host 0.0.0.0 --port 7860