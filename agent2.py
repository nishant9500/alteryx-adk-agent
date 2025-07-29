# agent.py - Standalone Alteryx XML to BigQuery SQL Conversion Logic

import os
import json
import logging
import re
from typing import Dict, Any, Tuple

from vertexai.preview.generative_models import GenerativeModel, Part
import vertexai

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class AlteryxToBigQueryAgent:
    def __init__(self, project_id: str, location: str, model_name: str = 'gemini-1.0-pro'):
        """
        Initializes the Alteryx to BigQuery Agent.
        Args:
            project_id: Your GCP project ID.
            location: The GCP region for Vertex AI (e.g., 'us-central1').
            model_name: The Gemini model to use (e.g., 'gemini-1.0-pro', 'gemini-1.5-flash').
        """
        logging.info(f"Initializing Vertex AI with project={project_id}, location={location}")
        vertexai.init(project=project_id, location=location)
        self.model = GenerativeModel(model_name=model_name)
        logging.info(f"Using Gemini model: {model_name}")

    def _parse_alteryx_xml(self, xml_string: str) -> Tuple[list, str]:
        """
        Parses a simplified Alteryx XML string to extract tool configurations.
        This is a basic regex-based parser for demonstration.
        For a robust production system, consider a dedicated XML parsing library.
        """
        tools = []
        message = "XML parsed successfully. Beginning conversion..."

        workflow_regex = r"<AlteryxWorkflow>(.*?)</AlteryxWorkflow>"
        workflow_content_match = re.search(workflow_regex, xml_string, re.DOTALL)

        if not workflow_content_match or not workflow_content_match.group(1):
            return [], "Error: Invalid Alteryx Workflow XML structure. Please ensure it's wrapped in <AlteryxWorkflow> tags."

        inner_xml = workflow_content_match.group(1)

        # Regex to find select nodes and extract attributes
        select_regex = r"<Node ToolID=\"(\d+)\" Type=\"Select\">(.*?)</Node>"
        for match in re.finditer(select_regex, inner_xml, re.DOTALL):
            tool_id = match.group(1)
            config_xml = match.group(2)
            fields = []
            field_regex = r"<Field Name=\"([^\"]+)\" Selected=\"([^\"]+)\"(?: Rename=\"([^\"]+)\")? />"
            for field_match in re.finditer(field_regex, config_xml):
                fields.append({
                    'name': field_match.group(1),
                    'selected': field_match.group(2) == 'True',
                    'rename': field_match.group(3) if field_match.group(3) else None
                })
            tools.append({'type': 'Select', 'toolId': tool_id, 'fields': fields, 'xml_snippet': match.group(0)})

        # Regex to find filter nodes and extract attributes
        filter_regex = r"<Node ToolID=\"(\d+)\" Type=\"Filter\">(.*?)</Node>"
        for match in re.finditer(filter_regex, inner_xml, re.DOTALL):
            tool_id = match.group(1)
            config_xml = match.group(2)
            expression_match = re.search(r"<Expression>(.*?)</Expression>", config_xml, re.DOTALL)
            expression = expression_match.group(1) if expression_match else None
            tools.append({'type': 'Filter', 'toolId': tool_id, 'expression': expression, 'xml_snippet': match.group(0)})

        # Sort tools by ToolID to maintain workflow order
        tools.sort(key=lambda x: int(x['toolId']))

        if not tools:
            message = "No recognizable 'Select' or 'Filter' tools found in the provided XML. I can only process these for now."

        return tools, message

    def _generate_sql_snippet(self, prompt: str) -> str:
        """Calls the Gemini model to generate a SQL snippet."""
        logging.info(f"Sending prompt to Gemini: {prompt[:100]}...")
        try:
            response = self.model.generate_content([Part.from_text(prompt)])
            return response.text.strip()
        except Exception as e:
            logging.error(f"Error generating content from Gemini: {e}")
            raise

    def convert_alteryx_to_sql(self, alteryx_xml: str) -> Dict[str, Any]:
        """
        Converts Alteryx XML to BigQuery SQL view code.
        Args:
            alteryx_xml: The Alteryx XML code as a string.
        Returns:
            A dictionary containing the generated SQL and a message from the agent.
        """
        tools, parse_message = self._parse_alteryx_xml(alteryx_xml)

        if not tools:
            return {"sql": "", "message": parse_message}

        current_schema = {
            "OrderID": "STRING",
            "CustomerName": "STRING",
            "ProductCategory": "STRING",
            "SalesAmount": "FLOAT"
        }

        sql_steps = []
        current_cte_name = "source_data"
        agent_messages = [parse_message]
        final_output_schema = current_schema.copy() # Track schema evolution

        for i, tool in enumerate(tools):
            prompt = ''
            step_output_schema = current_schema.copy() # Schema for this specific step's output

            if tool['type'] == 'Select':
                select_fields = [f for f in tool['fields'] if f['selected']]
                # Update step_output_schema based on selected and renamed fields
                step_output_schema = {} # Reset to only selected fields
                for f in select_fields:
                    output_name = f['rename'] if f['rename'] else f['name']
                    step_output_schema[output_name] = current_schema.get(f['name'], 'UNKNOWN') # Preserve type

                prompt = f"""
You are an expert Alteryx to BigQuery SQL converter.
Translate the following Alteryx Select tool logic into a BigQuery SQL SELECT statement.
The input data comes from a CTE named `{current_cte_name}` with the following schema:
{json.dumps(current_schema, indent=2)}

Alteryx Select Tool Configuration (XML snippet):
{tool['xml_snippet']}

Generate only the BigQuery SQL SELECT statement. Do not include any explanations or extra text.
Ensure all selected columns are present in the output.
"""
                agent_messages.append(f"Agent: Processing Select Tool (ID: {tool['toolId']})...")

            elif tool['type'] == 'Filter':
                # Filter tool doesn't change schema, so step_output_schema remains current_schema
                prompt = f"""
You are an expert Alteryx to BigQuery SQL converter.
Translate the following Alteryx Filter tool logic into a BigQuery SQL WHERE clause.
The input data comes from a CTE named `{current_cte_name}` with the following schema:
{json.dumps(current_schema, indent=2)}

Alteryx Filter Tool Configuration (XML snippet):
{tool['xml_snippet']}

Generate only the BigQuery SQL WHERE clause, including the 'WHERE' keyword. Do not include any explanations or extra text.
"""
                agent_messages.append(f"Agent: Processing Filter Tool (ID: {tool['toolId']})...")

            else:
                return {
                    "sql": "",
                    "message": f"Agent: I'm sorry, I don't recognize or support the Alteryx tool type: '{tool['type']}' (ToolID: {tool['toolId']}) yet. I can only convert 'Select' and 'Filter' tools."
                }

            try:
                generated_sql_snippet = self._generate_sql_snippet(prompt)
            except Exception as e:
                return {
                    "sql": "",
                    "message": f"Agent: Failed to generate SQL for ToolID {tool['toolId']}. Error: {str(e)}"
                }

            prev_cte_for_step = current_cte_name
            next_cte_name = f"cte_{i + 1}"

            if tool['type'] == 'Select':
                sql_steps.append(f"WITH {next_cte_name} AS (\n{generated_sql_snippet}\nFROM {prev_cte_for_step}\n)")
            elif tool['type'] == 'Filter':
                cols_to_select = ", ".join(step_output_schema.keys()) # Use keys from the schema *before* this filter
                sql_steps.append(f"WITH {next_cte_name} AS (\nSELECT\n    {cols_to_select}\nFROM {prev_cte_for_step}\n{generated_sql_snippet}\n)")

            current_cte_name = next_cte_name
            current_schema = step_output_schema # Update current schema for the next iteration
            final_output_schema = current_schema # Keep track of the final schema

        # Assemble the final BigQuery View SQL
        final_sql = ''
        if sql_steps:
            final_sql = "\n\n".join(sql_steps) # Add double newline for readability between CTEs
            final_select_cols = ", ".join(final_output_schema.keys())

            # Replace the initial source_data reference with a placeholder for the actual table
            final_sql = final_sql.replace("FROM source_data", "FROM `your_project.your_dataset.your_initial_table`")

            final_sql = f"""
CREATE OR REPLACE VIEW `your_project.your_dataset.your_view_name` AS
{final_sql}

SELECT
    {final_select_cols}
FROM
    {current_cte_name};
"""
            agent_messages.append("Agent: Conversion completed successfully! Please review the generated SQL.")
        else:
            agent_messages.append("Agent: I processed the XML but couldn't generate any SQL steps. Please check your XML content.")

        return {"sql": final_sql, "message": "\n".join(agent_messages)}

# Example of how to use this class as a standalone script
if __name__ == "__main__":
    # You would typically get these from environment variables or a config file
    # For local testing, replace with your actual GCP project ID and region
    PROJECT_ID = os.environ.get('GCP_PROJECT', 'your-gcp-project-id')
    LOCATION = os.environ.get('GCP_REGION', 'us-central1')

    # Example Alteryx XML
    example_alteryx_xml = """
<AlteryxWorkflow>
  <Node ToolID="1" Type="Select">
    <Name>Select Columns</Name>
    <Configuration>
      <Fields>
        <Field Name="OrderID" Selected="True" Rename="transaction_id" />
        <Field Name="CustomerName" Selected="True" />
        <Field Name="ProductCategory" Selected="False" />
        <Field Name="SalesAmount" Selected="True" Rename="total_sales" />
      </Fields>
    </Configuration>
  </Node>
  <Node ToolID="2" Type="Filter">
    <Name>Filter High Sales</Name>
    <Configuration>
      <Expression>[total_sales] > 1000 AND [CustomerName] = 'Alice'</Expression>
    </Configuration>
  </Node>
</AlteryxWorkflow>
"""
    # Initialize the agent
    converter_agent = AlteryxToBigQueryAgent(PROJECT_ID, LOCATION)

    # Perform the conversion
    result = converter_agent.convert_alteryx_to_sql(example_alteryx_xml)

    # Print the results
    print("--- Agent Message ---")
    print(result["message"])
    print("\n--- Generated SQL ---")
    print(result["sql"])
