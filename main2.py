# main.py
import os
import logging
from agent import AlteryxToBigQueryAgent # Import the class from agent.py
from vertexai.agent_engines import server

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Get project ID and location from environment variables provided by Cloud Run
PROJECT_ID = os.environ.get('GCP_PROJECT')
LOCATION = os.environ.get('GCP_REGION')

if not PROJECT_ID or not LOCATION:
    logging.error("GCP_PROJECT or GCP_REGION environment variables not set. Please ensure they are configured.")
    # Fallback for local testing if not set, but will fail in Cloud Run without them
    PROJECT_ID = "your-gcp-project-id" # Placeholder, replace if testing locally without env vars
    LOCATION = "us-central1" # Placeholder

# Instantiate the AlteryxToBigQueryAgent
alteryx_converter_instance = AlteryxToBigQueryAgent(PROJECT_ID, LOCATION)

# Define the tool using the instance's method
convert_alteryx_tool = server.Tool(
    name="convert_alteryx_xml_to_bigquery_sql",
    description="Converts Alteryx XML code for Select and Filter tools into BigQuery SQL view code.",
    func=alteryx_converter_instance.convert_alteryx_to_sql,
    input_schema={"type": "object", "properties": {"alteryx_xml": {"type": "string"}}},
    output_schema={"type": "object", "properties": {"sql": {"type": "string"}, "message": {"type": "string"}}}
)

# Get port from environment variable, default to 8080 for local development
PORT = int(os.environ.get("PORT", 8080))

# Create the ADK server with your tool
app = server.create_app([convert_alteryx_tool])

if __name__ == "__main__":
    logging.info(f"Starting ADK server on port {PORT}")
    app.run(debug=True, host="0.0.0.0", port=PORT)
