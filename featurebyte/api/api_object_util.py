"""
API Object Util
"""
from typing import Any, Dict, Optional

import ctypes
import threading

from rich.pretty import pretty_repr

from featurebyte.config import Configurations
from featurebyte.exception import RecordRetrievalException
from featurebyte.logging import get_logger

logger = get_logger(__name__)


class ProgressThread(threading.Thread):
    """
    Thread to get progress updates from websocket
    """

    def __init__(self, task_id: str, progress_bar: Any) -> None:
        self.task_id = task_id
        self.progress_bar = progress_bar
        threading.Thread.__init__(self)

    def run(self) -> None:
        """
        Check progress updates from websocket
        """
        # receive message from websocket
        with Configurations().get_websocket_client(task_id=self.task_id) as websocket_client:
            try:
                while True:
                    logger.debug("Waiting for websocket message")
                    message = websocket_client.receive_json()
                    # socket closed
                    if not message:
                        break
                    # update progress bar
                    description = message.get("message")
                    if description:
                        self.progress_bar.text(description)
                    percent = message.get("percent")
                    if percent:
                        # end of stream
                        if percent == -1:
                            break
                        self.progress_bar(percent / 100)  # pylint: disable=not-callable
            finally:
                logger.debug("Progress tracking ended.")

    def get_id(self) -> Optional[int]:
        """
        Returns id of the respective thread

        Returns
        -------
        Optional[int]
            thread id
        """
        # returns id of the respective thread
        if hasattr(self, "_thread_id"):
            return int(getattr(self, "_thread_id"))
        active_threads = getattr(threading, "_active", {})
        for thread_id, thread in active_threads.items():
            if thread is self:
                return int(thread_id)
        return None

    def raise_exception(self) -> None:
        """
        Raises SystemExit exception in the context of the given thread, which should
        cause the thread to exit silently (unless caught).
        """
        thread_id = self.get_id()
        if thread_id:
            res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_long(thread_id), ctypes.py_object(SystemExit)
            )
            if res > 1:
                ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, 0)
                logger.warning("Exception raise failure")


class PrettyDict(Dict[str, Any]):
    """
    Dict with prettified representation
    """

    def __repr__(self) -> str:
        return pretty_repr(dict(self), expand_all=True, indent_size=2)


class NameAttributeUpdatableMixin:
    """
    This mixin is used to handle the case when name of the api object is updatable.
    """

    def __getattribute__(self, item: str) -> Any:
        """
        Custom __getattribute__ method to handle the case when name of the model is updated.

        Parameters
        ----------
        item: str
            Attribute name.

        Returns
        -------
        Any
            Attribute value.
        """
        if item == "name":
            # Special handling for name attribute is required because name is a common attribute for all
            # FeaturebyteBaseDocumentModel objects. To override the parent name attribute, we need to use
            # __getattribute__ method as property has no effect to override the parent pydantic model attribute.
            try:
                # Retrieve the name from the cached model first. Cached model is used to store the latest
                # model retrieved from the server. If the model has not been saved to the server, name attribute
                # of this model will be used.
                return self.cached_model.name
            except RecordRetrievalException:
                pass
        return super().__getattribute__(item)
