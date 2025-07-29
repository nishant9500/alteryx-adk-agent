# main.py - Flask Web Server for Alteryx to BigQuery SQL Conversion

import os
import logging
from agent import AlteryxToBigQueryAgent # Import the class from agent.py
from flask import Flask, request, jsonify # Import Flask and related utilities

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

# Initialize the AlteryxToBigQueryAgent instance
# It's good practice to initialize this once globally for a Flask app
try:
    alteryx_converter_instance = AlteryxToBigQueryAgent(PROJECT_ID, LOCATION)
except Exception as e:
    logging.error(f"Failed to initialize AlteryxToBigQueryAgent: {e}")
    # Depending on your error handling strategy, you might want to exit or raise
    # For now, we'll let the app start but conversion calls will fail.

# Create the Flask app
app = Flask(__name__)

# Define the API endpoint for conversion
@app.route('/convert', methods=['POST'])
def convert_xml_to_sql_endpoint():
    # Ensure the request body is JSON
    if not request.is_json:
        return jsonify({"message": "Request must be JSON"}), 400

    data = request.get_json()
    alteryx_xml = data.get('alteryx_xml')

    # Validate input XML
    if not alteryx_xml:
        return jsonify({"message": "Missing 'alteryx_xml' in request body."}), 400

    try:
        # Call the conversion method from your agent instance
        result = alteryx_converter_instance.convert_alteryx_to_sql(alteryx_xml)
        return jsonify(result), 200
    except Exception as e:
        # Catch any errors during the conversion process
        logging.error(f"Error during Alteryx to BigQuery SQL conversion: {e}", exc_info=True)
        return jsonify({"message": f"An internal server error occurred during conversion: {str(e)}"}), 500

# Get port from environment variable, default to 8080 for local development
PORT = int(os.environ.get("PORT", 8080))

if __name__ == "__main__":
    logging.info(f"Starting Flask server on port {PORT}")
    # When running locally, debug=True provides helpful error messages
    # For production, set debug=False
    app.run(debug=True, host="0.0.0.0", port=PORT)
