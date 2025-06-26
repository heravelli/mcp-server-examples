# mcp-server

## Setup

```bash
cd mcp-server
python -m venv .venv
source .venv/bin/activate
pip install fastmcp streamlit databricks-sdk requests python-dotenv
```

## Usage

To run the server:

```bash
fastmcp run server.py
```

To run the server with verbose logging:

```bash
fastmcp run server.py --verbose
```

## Troubleshooting

### NLP Gateway Errors:

- Verify `NLP_GATEWAY_URL`, `NLP_MODEL_NAME`, `NLP_API_KEY`.
- Check response format and adjust parsing in `generate_sql_query`.

You can test the NLP gateway with the following `curl` command:

```bash
curl -H "Authorization: Bearer $NLP_API_KEY" -H "Content-Type: application/json" -d '{"model":"your-model-name","prompt":"test"}' $NLP_GATEWAY_URL
```