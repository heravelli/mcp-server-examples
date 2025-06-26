import streamlit as st
import asyncio
import re
import requests
import os
from fastmcp import Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize FastMCP client
client = Client("toll_server.py")

# Initialize session state for chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Function to generate SQL query using custom NLP gateway
def generate_sql_query(natural_language: str) -> str:
    gateway_url = os.getenv("NLP_GATEWAY_URL")
    model_name = os.getenv("NLP_MODEL_NAME")
    api_key = os.getenv("NLP_API_KEY")  # Optional

    if not gateway_url or not model_name:
        raise ValueError("NLP_GATEWAY_URL and NLP_MODEL_NAME must be set")

    prompt = f"""
    Convert the following natural language request into a valid SQL query for a Databricks SQL warehouse.
    Assume tables are in a Unity Catalog schema (e.g., my_catalog.my_schema.table_name).
    Use standard SQL syntax and include a LIMIT 10 clause unless specified otherwise.
    If the schema or catalog is not mentioned, assume my_catalog.my_schema.
    Examples:
    - "Show all customers" -> "SELECT * FROM my_catalog.my_schema.customers LIMIT 10"
    - "Get total tolls for cars in January 2025" -> "SELECT SUM(toll_amount) FROM my_catalog.my_schema.tolls WHERE vehicle_type = 'car' AND date LIKE '2025-01%' LIMIT 10"
    Input: {natural_language}
    Output: Only the SQL query, no explanations.
    """

    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    payload = {
        "model": model_name,
        "prompt": prompt,
        "max_tokens": 200  # Adjust based on your model's requirements
    }

    try:
        response = requests.post(gateway_url, json=payload, headers=headers)
        response.raise_for_status()
        # Adjust parsing based on your gateway's response format
        # Example assumes OpenAI-like response
        return response.json().get("choices", [{}])[0].get("text", "").strip()
    except Exception as e:
        raise RuntimeError(f"Failed to generate SQL query: {str(e)}")

st.title("Toll Server Tools")

# Chat Interface
st.header("Chat with Toll Server")
chat_container = st.container()

# Display chat history
with chat_container:
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

# Chat input
user_input = st.chat_input("Type a command (e.g., 'Get secret word', 'Calculate toll for car, 10 miles, $0.2/mile', or 'Show all customers')")

if user_input:
    # Add user message to history
    st.session_state.messages.append({"role": "user", "content": user_input})
    with chat_container:
        with st.chat_message("user"):
            st.markdown(user_input)

    # Parse and process command
    try:
        async def process_command(command):
            async with client:
                command = command.lower().strip()
                # Handle secret_word
                if "secret word" in command:
                    return await client.call_tool("secret_word")
                # Handle calculate_toll
                elif "calculate toll" in command:
                    vehicle_match = re.search(r"for\s+(\w+)", command)
                    distance_match = re.search(r"(\d+\.?\d*)\s+miles", command)
                    rate_match = re.search(r"\$?(\d+\.?\d*)\s*/?\s*mile", command)

                    vehicle_type = vehicle_match.group(1) if vehicle_match else "car"
                    distance = float(distance_match.group(1)) if distance_match else 10.0
                    toll_rate = float(rate_match.group(1)) if rate_match else 0.25

                    return await client.call_tool(
                        "calculate_toll",
                        {"vehicle_type": vehicle_type, "distance": distance, "toll_rate": toll_rate}
                    )
                # Handle run_sql_query (direct SQL)
                elif "run sql query" in command:
                    query_match = re.search(r"run sql query\s+(.+)", command, re.IGNORECASE)
                    if not query_match:
                        return "Please provide a SQL query (e.g., 'Run SQL query SELECT * FROM my_table')."
                    query = query_match.group(1).strip()
                    results = await client.call_tool("run_sql_query", {"query": query})
                    if results:
                        return "\n".join([str(row) for row in results])
                    return "No results returned."
                # Handle NLP for SQL query
                else:
                    # Generate SQL query via custom gateway
                    query = generate_sql_query(command)
                    st.session_state.messages.append({"role": "assistant", "content": f"Generated SQL: {query}"})
                    with chat_container:
                        with st.chat_message("assistant"):
                            st.markdown(f"Generated SQL: {query}")
                    results = await client.call_tool("run_sql_query", {"query": query})
                    if results:
                        return "\n".join([str(row) for row in results])
                    return "No results returned."

        # Run async command
        response = asyncio.run(process_command(user_input))

        # Add assistant response to history
        st.session_state.messages.append({"role": "assistant", "content": str(response)})
        with chat_container:
            with st.chat_message("assistant"):
                st.markdown(str(response))

    except Exception as e:
        error_msg = f"Error: {e}"
        st.session_state.messages.append({"role": "assistant", "content": error_msg})
        with chat_container:
            with st.chat_message("assistant"):
                st.markdown(error_msg)

# Button-Based Interface
st.header("Manual Tool Access")
st.subheader("Get Secret Word")
if st.button("Get Secret Word"):
    try:
        async def call_secret_word():
            async with client:
                return await client.call_tool("secret_word")
        result = asyncio.run(call_secret_word())
        st.success(f"Secret Word: {result}")
    except Exception as e:
        st.error(f"Error: {e}")

st.subheader("Calculate Toll")
vehicle_type = st.selectbox("Vehicle Type", ["Car", "Truck", "Motorcycle"])
distance = st.number_input("Distance (miles)", min_value=0.0, step=0.1, value=10.0)
toll_rate = st.number_input("Toll Rate ($/mile)", min_value=0.0, step=0.01, value=0.25)

if st.button("Calculate Toll"):
    try:
        async def call_calculate_toll():
            async with client:
                return await client.call_tool(
                    "calculate_toll",
                    {"vehicle_type": vehicle_type.lower(), "distance": distance, "toll_rate": toll_rate}
                )
        result = asyncio.run(call_calculate_toll())
        st.success(f"Toll Cost: ${result}")
    except Exception as e:
        st.error(f"Error: {e}")

st.subheader("Run SQL Query")
sql_query = st.text_input("SQL Query (e.g., SELECT * FROM my_catalog.my_schema.my_table LIMIT 10)")
if st.button("Execute SQL Query"):
    try:
        async def call_sql_query():
            async with client:
                return await client.call_tool("run_sql_query", {"query": sql_query})
        results = asyncio.run(call_sql_query())
        if results:
            st.success("Query Results:")
            st.write(results)
        else:
            st.info("No results returned.")
    except Exception as e:
        st.error(f"Error: {e}")

# --- New Tool: Call Snowflake Agent ---
st.subheader("Call Snowflake Agent")
sf_query = st.text_input("Snowflake SQL Query (e.g., SELECT * FROM my_db.my_schema.my_table LIMIT 10)", key="sf_query")
if st.button("Execute Snowflake Query"):
    try:
        async def call_snowflake_agent():
            async with client:
                # Assumes you have a tool named 'run_snowflake_query' registered in your FastMCP server
                return await client.call_tool("run_snowflake_query", {"query": sf_query})
        sf_results = asyncio.run(call_snowflake_agent())
        if sf_results:
            st.success("Snowflake Query Results:")
            st.write(sf_results)
        else:
            st.info("No results returned from Snowflake.")
    except Exception as e:
        st.error(f"Error: {e}")