"""
This module contains the mixin class used by api objects.
"""

from __future__ import annotations

import time
from datetime import datetime
from http import HTTPStatus
from typing import Any, Optional, Tuple, Union

import pandas as pd
from alive_progress import alive_bar
from requests import Response
from typeguard import typechecked

from featurebyte.api.api_object_util import ProgressThread
from featurebyte.common import get_active_catalog_id
from featurebyte.common.env_util import get_alive_bar_additional_params
from featurebyte.common.utils import dataframe_from_json, validate_datetime_input
from featurebyte.config import Configurations
from featurebyte.core.mixin import perf_logging
from featurebyte.exception import (
    RecordCreationException,
    RecordRetrievalException,
    RecordUpdateException,
    ResponseException,
)
from featurebyte.logging import get_logger
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.schema.feature_store import (
    FeatureStorePreview,
    FeatureStoreSample,
    FeatureStoreShape,
)
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
        task_failure_exception_class: type[ResponseException] = RecordCreationException,
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
                    progress_bar(1)
            except KeyboardInterrupt:
                # try to revoke task
                client.patch(f"/task/{task_id}", json={"revoke": True})
                raise
            finally:
                thread.raise_exception()
                thread.join(timeout=0)

        # check the task status
        if status != TaskStatus.SUCCESS:
            raise task_failure_exception_class(response=task_get_response or task_response)

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
        if update_response.status_code == HTTPStatus.OK:
            return
        if update_response.status_code == HTTPStatus.ACCEPTED:
            cls._poll_async_task(
                task_response=update_response,
                delay=delay,
                retrieve_result=False,
                task_failure_exception_class=RecordUpdateException,
            )
            return
        raise RecordUpdateException(response=update_response)


class SampleMixin(AsyncMixin):
    """
    Supports preview and sample functions
    """

    @perf_logging
    @typechecked
    def preview(self, limit: int = 10, **kwargs: Any) -> pd.DataFrame:
        """
        Retrieve a preview of the view / column.

        Parameters
        ----------
        limit: int
            Maximum number of return rows.
        **kwargs: Any
            Additional keyword parameters.

        Returns
        -------
        pd.DataFrame
            Preview rows of the data.

        Raises
        ------
        RecordRetrievalException
            Preview request failed.

        Examples
        --------
        Preview 3 rows of a view.
        >>> catalog.get_view("GROCERYPRODUCT").preview(3)
                             GroceryProductGuid ProductGroup
        0  10355516-5582-4358-b5f9-6e1ea7d5dc9f      Glaçons
        1  116c9284-2c41-446e-8eee-33901e0acdef      Glaçons
        2  3a45a5e8-1b71-42e8-b84e-43ddaf692375      Glaçons

        Preview 3 rows of a column.
        >>> catalog.get_view("GROCERYPRODUCT")["GroceryProductGuid"].preview(3)
                             GroceryProductGuid
        0  10355516-5582-4358-b5f9-6e1ea7d5dc9f
        1  116c9284-2c41-446e-8eee-33901e0acdef
        2  3a45a5e8-1b71-42e8-b84e-43ddaf692375

        See Also
        --------
        - [View.sample](/reference/featurebyte.api.view.View.sample/):
          Retrieve a sample of a view.
        - [View.describe](/reference/featurebyte.api.view.View.describe/):
          Retrieve a summary of a view.
        """
        pruned_graph, mapped_node = self.extract_pruned_graph_and_node(**kwargs)  # type: ignore
        payload = FeatureStorePreview(
            graph=QueryGraph(**pruned_graph.model_dump(by_alias=True)),
            node_name=mapped_node.name,
            feature_store_id=self.feature_store.id,  # type: ignore
        )
        client = Configurations().get_client()
        response = client.post(
            url=f"/feature_store/preview?limit={limit}", json=payload.json_dict()
        )
        if response.status_code != HTTPStatus.OK:
            raise RecordRetrievalException(response)
        return dataframe_from_json(response.json())

    @property
    def timestamp_column(self) -> Optional[str]:
        """
        Timestamp column to be used for datetime filtering during sampling

        Returns
        -------
        Optional[str]
        """
        return None

    def _get_sample_payload(
        self,
        from_timestamp: Optional[Union[datetime, str]] = None,
        to_timestamp: Optional[Union[datetime, str]] = None,
        **kwargs: Any,
    ) -> FeatureStoreSample:
        # construct sample payload
        from_timestamp = validate_datetime_input(from_timestamp) if from_timestamp else None
        to_timestamp = validate_datetime_input(to_timestamp) if to_timestamp else None

        pruned_graph, mapped_node = self.extract_pruned_graph_and_node(**kwargs)  # type: ignore
        return FeatureStoreSample(
            graph=QueryGraph(**pruned_graph.model_dump(by_alias=True)),
            node_name=mapped_node.name,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            timestamp_column=self.timestamp_column,
            feature_store_id=self.feature_store.id,  # type: ignore
        )

    @perf_logging
    @typechecked
    def shape(self, **kwargs: Any) -> Tuple[int, int]:
        """
        Return the shape of the view / column.

        Parameters
        ----------
        **kwargs: Any
            Additional keyword parameters.

        Returns
        -------
        Tuple[int, int]

        Raises
        ------
        RecordRetrievalException
            Shape request failed.

        Examples
        --------
        Get the shape of a view.
        >>> catalog.get_view("INVOICEITEMS").shape()
        (300450, 10)
        """
        pruned_graph, mapped_node = self.extract_pruned_graph_and_node(**kwargs)  # type: ignore
        payload = FeatureStorePreview(
            graph=QueryGraph(**pruned_graph.model_dump(by_alias=True)),
            node_name=mapped_node.name,
            feature_store_id=self.feature_store.id,  # type: ignore
        )
        client = Configurations().get_client()
        response = client.post(url="/feature_store/shape", json=payload.json_dict())
        if response.status_code != HTTPStatus.OK:
            raise RecordRetrievalException(response)
        shape = FeatureStoreShape(**response.json())
        return shape.num_rows, shape.num_cols

    @perf_logging
    @typechecked
    def sample(
        self,
        size: int = 10,
        seed: int = 1234,
        from_timestamp: Optional[Union[datetime, str]] = None,
        to_timestamp: Optional[Union[datetime, str]] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Retrieve a random sample of the view / column.

        Parameters
        ----------
        size: int
            Maximum number of rows to sample, with an upper bound of 10,000 rows.
        seed: int
            Seed to use for random sampling.
        from_timestamp: Optional[datetime]
            Start of date range to sample from.
        to_timestamp: Optional[datetime]
            End of date range to sample from.
        **kwargs: Any
            Additional keyword parameters.

        Returns
        -------
        pd.DataFrame
            Sampled rows of the data.

        Raises
        ------
        RecordRetrievalException
            Sample request failed.

        Examples
        --------
        Sample rows of a view.
        >>> catalog.get_view("GROCERYPRODUCT").sample(3)
                             GroceryProductGuid ProductGroup
        0  e890c5cb-689b-4caf-8e49-6b97bb9420c0       Épices
        1  5720e4df-2996-4443-a1bc-3d896bf98140         Chat
        2  96fc4d80-8cb0-4f1b-af01-e71ad7e7104a        Pains

        Sample 3 rows of a column.
        >>> catalog.get_view("GROCERYPRODUCT")["ProductGroup"].sample(3)
          ProductGroup
        0       Épices
        1         Chat
        2        Pains

        See Also
        --------
        - [View.preview](/reference/featurebyte.api.view.View.preview/):
          Retrieve a preview of a view.
        - [View.sample](/reference/featurebyte.api.view.View.sample/):
          Retrieve a sample of a view.
        """
        payload = self._get_sample_payload(from_timestamp, to_timestamp, **kwargs)
        client = Configurations().get_client()
        response = client.post(
            url=f"/feature_store/sample?size={size}&seed={seed}", json=payload.json_dict()
        )
        if response.status_code != HTTPStatus.OK:
            raise RecordRetrievalException(response)
        return dataframe_from_json(response.json())

    @perf_logging
    @typechecked
    def describe(
        self,
        size: int = 0,
        seed: int = 1234,
        from_timestamp: Optional[Union[datetime, str]] = None,
        to_timestamp: Optional[Union[datetime, str]] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        """
        Retrieve a summary of the view / column.

        Parameters
        ----------
        size: int
            Maximum number of rows to sample.
        seed: int
            Seed to use for random sampling.
        from_timestamp: Optional[datetime]
            Start of date range to sample from.
        to_timestamp: Optional[datetime]
            End of date range to sample from.
        **kwargs: Any
            Additional keyword parameters.

        Returns
        -------
        pd.DataFrame
            Summary of the view.

        Examples
        --------
        Get summary of a view.
        >>> catalog.get_view("GROCERYPRODUCT").describe()
                                    GroceryProductGuid        ProductGroup
        dtype                                  VARCHAR             VARCHAR
        unique                                   29099                  87
        %missing                                   0.0                 0.0
        %empty                                       0                   0
        entropy                               6.214608             4.13031
        top       017fe5ed-80a2-4e70-ae48-78aabfdee856  Chips et Tortillas
        freq                                       1.0              1319.0

        Get summary of a column.
        >>> catalog.get_view("GROCERYPRODUCT")["ProductGroup"].describe()
                        ProductGroup
        dtype                VARCHAR
        unique                    87
        %missing                 0.0
        %empty                     0
        entropy              4.13031
        top       Chips et Tortillas
        freq                  1319.0

        See Also
        --------
        - [View.preview](/reference/featurebyte.api.view.View.preview/):
          Retrieve a preview of a view.
        - [View.sample](/reference/featurebyte.api.view.View.sample/):
          Retrieve a sample of a view.
        """
        from_timestamp = validate_datetime_input(from_timestamp) if from_timestamp else None
        to_timestamp = validate_datetime_input(to_timestamp) if to_timestamp else None

        pruned_graph, mapped_node = self.extract_pruned_graph_and_node(**kwargs)  # type: ignore
        payload = FeatureStoreSample(
            graph=QueryGraph(**pruned_graph.model_dump(by_alias=True)),
            node_name=mapped_node.name,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            timestamp_column=self.timestamp_column,
            feature_store_id=self.feature_store.id,  # type: ignore
        )
        catalog_id = get_active_catalog_id()
        data_description = AsyncMixin.post_async_task(
            route=f"/feature_store/data_description?size={size}&seed={seed}&catalog_id={catalog_id}",
            payload=payload.json_dict(),
        )
        return dataframe_from_json(data_description)
