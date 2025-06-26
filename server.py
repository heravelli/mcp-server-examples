from fastmcp import FastMCP
import signal
import sys
import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import ExecuteStatementRequest
from typing import List, Dict, Any
import asyncio

mcp = FastMCP("TollServer")

workspace = WorkspaceClient(
    host=os.getenv("DATABRICKS_HOST"),
    token=os.getenv("DATABRICKS_TOKEN")
)

@mcp.tool()
def secret_word() -> str:
    """Returns a secret word."""
    return "ABRACADABRA"

@mcp.tool()
def calculate_toll(vehicle_type: str, distance: float, toll_rate: float = 0.25) -> float:
    """Calculates toll based on vehicle type, distance, and rate."""
    rates = {"car": 1.0, "truck": 1.5, "motorcycle": 0.8}
    multiplier = rates.get(vehicle_type.lower(), 1.0)
    return round(distance * toll_rate * multiplier, 2)

@mcp.tool()
async def run_sql_query(query: str) -> List[Dict[str, Any]]:
    """Runs a SQL query on Databricks SQL warehouse and returns results."""
    try:
        warehouse_id = os.getenv("DATABRICKS_SQL_WAREHOUSE_ID")
        if not warehouse_id:
            raise ValueError("DATABRICKS_SQL_WAREHOUSE_ID not set")

        statement = workspace.statement_execution.execute_statement(
            ExecuteStatementRequest(
                warehouse_id=warehouse_id,
                statement=query,
                wait_timeout="30s"
            )
        )

        async def poll_results():
            while statement.status.state in ["PENDING", "RUNNING"]:
                await asyncio.sleep(2)
                statement.refresh()
            if statement.status.state != "SUCCEEDED":
                raise RuntimeError(f"Query failed: {statement.status.error}")
            return statement.result.data_array or []

        results = await poll_results()

        if results and statement.result.schema:
            columns = [field.name for field in statement.result.schema.fields]
            return [dict(zip(columns, row)) for row in results]
        return []

    except Exception as e:
        raise RuntimeError(f"SQL query failed: {str(e)}")

@mcp.tool()
async def run_snowflake_query(query: str) -> List[Dict[str, Any]]:
    """
    Runs a SQL query on Snowflake and returns results.
    Requires SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE env vars.
    """
    try:
        import snowflake.connector
        import asyncio

        # Gather credentials from environment
        account = os.getenv("SNOWFLAKE_ACCOUNT")
        user = os.getenv("SNOWFLAKE_USER")
        password = os.getenv("SNOWFLAKE_PASSWORD")
        database = os.getenv("SNOWFLAKE_DATABASE")
        schema = os.getenv("SNOWFLAKE_SCHEMA")
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")

        if not all([account, user, password, database, schema, warehouse]):
            raise ValueError("Missing one or more Snowflake environment variables.")

        # Connect to Snowflake
        conn = snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            database=database,
            schema=schema,
            warehouse=warehouse,
            autocommit=True,
        )

        def run_query_sync(q):
            with conn.cursor() as cur:
                cur.execute(q)
                columns = [desc[0] for desc in cur.description] if cur.description else []
                rows = cur.fetchall()
                return [dict(zip(columns, row)) for row in rows] if columns else []

        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(None, run_query_sync, query)
        conn.close()
        return results

    except Exception as e:
        raise RuntimeError(f"Snowflake query failed: {str(e)}")

def signal_handler(sig, frame):
    print("Shutting down FastMCP server...")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

if __name__ == "__main__":
    print("Starting FastMCP Toll Server...")
    mcp.run()