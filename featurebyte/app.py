"""
FastAPI Application
"""
from fastapi import FastAPI

from featurebyte.routes import event_table

app = FastAPI()
app.include_router(event_table.router)
