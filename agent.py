# agent.py

import xml.etree.ElementTree as ET

def parse_alteryx_workflow(xml_string):
    root = ET.fromstring(xml_string)
    tools = []

    for node in root.findall(".//Node"):
        tool_id = node.get("ToolID")
        tool_name = node.get("Tool")
        config = node.find("Properties/Configuration")
        if config is not None:
            config_text = ET.tostring(config, encoding="unicode", method="xml")
        else:
            config_text = "No config"
        tools.append(f"Tool {tool_id}: {tool_name}\n{config_text.strip()}")

    return "\n\n".join(tools)

def build_prompt(parsed_tools):
    return f"""You are a data engineer. Given the following Alteryx tool descriptions, convert them to an equivalent BigQuery SQL query:

{parsed_tools}

Return only the SQL code.
"""
