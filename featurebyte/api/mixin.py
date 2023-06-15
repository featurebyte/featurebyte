"""
This module contains the mixin class used by api objects.
"""
from __future__ import annotations

from typing import Any, Optional

import time
from http import HTTPStatus

from alive_progress import alive_bar
from requests import Response

from featurebyte.api.api_object_util import ProgressThread
from featurebyte.common.env_util import get_alive_bar_additional_params
from featurebyte.config import Configurations
from featurebyte.exception import (
    RecordCreationException,
    RecordRetrievalException,
    RecordUpdateException,
)
from featurebyte.logging import get_logger
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.schema.task import TaskStatus

POLLING_INTERVAL = 3

logger = get_logger(__name__)


class AsyncMixin(FeatureByteBaseModel):
    """
    AsyncMixin class for async task
    """

    @classmethod
    def _poll_async_task(
        cls,
        task_response: Response,
        delay: float = POLLING_INTERVAL,
        retrieve_result: bool = True,
        has_output_url: bool = True,
    ) -> dict[str, Any]:
        response_dict = task_response.json()
        status = response_dict["status"]
        task_id = response_dict["id"]

        # poll the task route (if the task is still running)
        client = Configurations().get_client()
        task_get_response = None

        with alive_bar(
            manual=True,
            title="Working...",
            **get_alive_bar_additional_params(),
        ) as progress_bar:
            try:
                # create progress update thread
                thread = ProgressThread(task_id=task_id, progress_bar=progress_bar)
                thread.daemon = True
                thread.start()

                while status in [
                    TaskStatus.STARTED,
                    TaskStatus.PENDING,
                ]:  # retrieve task status
                    task_get_response = client.get(url=f"/task/{task_id}")
                    if task_get_response.status_code == HTTPStatus.OK:
                        status = task_get_response.json()["status"]
                        time.sleep(delay)
                    else:
                        raise RecordRetrievalException(task_get_response)

                if status == TaskStatus.SUCCESS:
                    progress_bar.title = "Done!"
                    progress_bar(1)  # pylint: disable=not-callable
            finally:
                thread.raise_exception()
                thread.join(timeout=0)

        # check the task status
        if status != TaskStatus.SUCCESS:
            raise RecordCreationException(response=task_get_response or task_response)

        # retrieve task result
        output_url = response_dict.get("output_path")
        if output_url is None and task_get_response:
            output_url = task_get_response.json().get("output_path")
        if output_url is None and has_output_url:
            raise RecordRetrievalException(response=task_get_response or task_response)

        if not retrieve_result:
            return {"output_url": output_url}

        logger.debug("Retrieving task result", extra={"output_url": output_url})
        result_response = client.get(url=output_url)
        if result_response.status_code == HTTPStatus.OK:
            return dict(result_response.json())
        raise RecordRetrievalException(response=result_response)

    @classmethod
    def post_async_task(
        cls,
        route: str,
        payload: dict[str, Any],
        delay: float = POLLING_INTERVAL,
        retrieve_result: bool = True,
        has_output_url: bool = True,
        is_payload_json: bool = True,
        files: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """
        Post async task to the worker & retrieve the results (blocking)

        Parameters
        ----------
        route: str
            Async task route
        payload: dict[str, Any]
            Task payload
        delay: float
            Delay used in polling the task
        retrieve_result: bool
            Whether to retrieve result from output_url
        has_output_url: bool
            Whether the task response has output_url
        is_payload_json: bool
            Whether the payload should be passed via the json parameter. If False, the payload will
            be passed via the data parameter. Set this to False for routes that expects
            multipart/form-data encoding.
        files: Optional[dict[str, Any]]
            Optional files to be passed to the request

        Returns
        -------
        dict[str, Any]
            Response data

        Raises
        ------
        RecordCreationException
            When unexpected creation failure
        """
        client = Configurations().get_client()
        post_kwargs = {"url": route, "files": files}
        if is_payload_json:
            post_kwargs["json"] = payload
        else:
            post_kwargs["data"] = payload
        create_response = client.post(**post_kwargs)  # type: ignore[arg-type]
        if create_response.status_code != HTTPStatus.CREATED:
            raise RecordCreationException(response=create_response)
        return cls._poll_async_task(
            task_response=create_response,
            delay=delay,
            retrieve_result=retrieve_result,
            has_output_url=has_output_url,
        )

    @classmethod
    def patch_async_task(
        cls, route: str, payload: dict[str, Any], delay: float = POLLING_INTERVAL
    ) -> None:
        """
        Patch async task to the worker & wait for the task to finish (blocking)

        Parameters
        ----------
        route: str
            Async task route
        payload: dict[str, Any]
            Task payload
        delay: float
            Delay used in polling the task

        Raises
        ------
        RecordUpdateException
            When unexpected update failure
        """
        client = Configurations().get_client()
        update_response = client.patch(url=route, json=payload)
        if update_response.status_code != HTTPStatus.OK:
            raise RecordUpdateException(response=update_response)
        if update_response.json():
            cls._poll_async_task(task_response=update_response, delay=delay, retrieve_result=False)
