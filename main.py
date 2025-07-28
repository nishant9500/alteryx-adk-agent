# main.py

import os
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from agent import parse_alteryx_workflow, build_prompt
from vertexai.preview.language_models import ChatModel
import vertexai

app = FastAPI()
templates = Jinja2Templates(directory="templates")

PROJECT_ID = os.getenv("PROJECT_ID")
LOCATION = os.getenv("LOCATION", "us-central1")

vertexai.init(project=PROJECT_ID, location=LOCATION)
chat_model = ChatModel.from_pretrained("chat-bison")

@app.get("/", response_class=HTMLResponse)
async def form_get(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "sql": None})

@app.post("/", response_class=HTMLResponse)
async def form_post(request: Request, xml_input: str = Form(...)):
    try:
        parsed = parse_alteryx_workflow(xml_input)
        prompt = build_prompt(parsed)

        chat = chat_model.start_chat()
        response = chat.send_message(prompt)
        sql_output = response.text
    except Exception as e:
        sql_output = f"Error: {str(e)}"

    return templates.TemplateResponse("index.html", {"request": request, "sql": sql_output, "xml_input": xml_input})
