"""
FastAPI Application
"""
# pylint: disable=too-few-public-methods
from bson.objectid import ObjectId
from fastapi import Depends, FastAPI, Request

from featurebyte.routes import event_table
from featurebyte.storage import MongoStorage

app = FastAPI()
storage = MongoStorage("mongodb://localhost:27017")


class User:
    """
    Skeleton user class to provide static user for API routes
    """

    # DO NOT CHANGE THIS VALUE
    id = ObjectId("62a6d9d023e7a8f2a0dc041a")


def inject_api_deps(request: Request) -> None:
    """
    Inject dependencies into the requests

    Parameters
    ----------
    request: Request
        Request object to be updated
    """
    request.state.storage = storage
    request.state.user = User()


app.include_router(event_table.router, dependencies=[Depends(inject_api_deps)])
