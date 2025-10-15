"""
Request table processing for joining with snapshots tables
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from sqlglot import expressions

from featurebyte.enum import InternalName, SpecialColumnName
from featurebyte.query_graph.node.generic import SnapshotsLookupParameters
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.aggregator.base import CommonTable
from featurebyte.query_graph.sql.common import quoted_identifier
from featurebyte.query_graph.sql.cron import get_request_table_job_datetime_column_name
from featurebyte.query_graph.sql.timestamp_helper import apply_snapshot_adjustment


@dataclass
class ProcessedRequestTableEntry:
    """
    Represents an entry in the processed request table plan
    """

    distinct_adjusted_point_in_time_table: str
    distinct_point_in_time_table: str
    snapshots_parameters: SnapshotsLookupParameters
    serving_names: list[str]


class SnapshotsRequestTablePlan:
    """
    Responsible for generating a process request table ready to be joined with snapshots tables
    """

    def __init__(self, adapter: BaseAdapter):
        self.entries: dict[str, ProcessedRequestTableEntry] = {}
        self.adapter = adapter

    def add_snapshots_parameters(
        self, snapshots_parameters: SnapshotsLookupParameters, serving_names: list[str]
    ) -> None:
        """
        Update state given snapshots lookup parameters

        Parameters
        ----------
        snapshots_parameters: SnapshotsLookupParameters
            Snapshots lookup parameters
        serving_names: list[str]
            Serving names of the features
        """
        key = self.get_key(snapshots_parameters, serving_names)
        if key in self.entries:
            return
        entry = ProcessedRequestTableEntry(
            distinct_adjusted_point_in_time_table=f"SNAPSHOTS_REQUEST_TABLE_DISTINCT_ADJUSTED_POINT_IN_TIME_{key}",
            distinct_point_in_time_table=f"SNAPSHOTS_REQUEST_TABLE_DISTINCT_POINT_IN_TIME_{key}",
            snapshots_parameters=snapshots_parameters,
            serving_names=serving_names,
        )
        self.entries[key] = entry

    def get_entry(
        self, snapshots_parameters: SnapshotsLookupParameters, serving_names: list[str]
    ) -> ProcessedRequestTableEntry:
        """
        Get the processed request table information corresponding to a SnapshotsLookupParameters

        Parameters
        ----------
        snapshots_parameters: SnapshotsLookupParameters
            Snapshots lookup parameters
        serving_names: list[str]
            Serving names of the features

        Returns
        -------
        str
        """
        key = self.get_key(snapshots_parameters, serving_names=serving_names)
        return self.entries[key]

    @staticmethod
    def get_key(snapshots_parameters: SnapshotsLookupParameters, serving_names: list[str]) -> str:
        """
        Get an internal key used to determine request table sharing

        Parameters
        ----------
        snapshots_parameters: SnapshotsLookupParameters
            Snapshots lookup parameters
        serving_names: list[str]
            Serving names of the features

        Returns
        -------
        str
        """
        parameters_dict = snapshots_parameters.model_dump()
        parameters_dict["serving_names"] = sorted(serving_names)
        hasher = hashlib.shake_128()
        hasher.update(json.dumps(parameters_dict, sort_keys=True).encode("utf-8"))
        return hasher.hexdigest(8)

    def construct_request_table_ctes(
        self,
        request_table_name: str,
    ) -> list[CommonTable]:
        """
        Construct SQL statements that build the processed request tables

        Parameters
        ----------
        request_table_name : str
            Name of request table to use

        Returns
        -------
        list[tuple[str, expressions.Select]]
        """
        ctes = []
        for key, entry in self.entries.items():
            processed_tables = self.construct_processed_request_tables_for_entry(
                entry, request_table_name
            )
            ctes.extend(processed_tables)
        return ctes

    def construct_processed_request_tables_for_entry(
        self,
        entry: ProcessedRequestTableEntry,
        request_table_name: str,
    ) -> list[CommonTable]:
        """
        Construct SQL statements that build the processed request tables

        Parameters
        ----------
        entry: ProcessedRequestTableEntry
            Processed request table entry
        request_table_name: str
            Name of the original request table that is determined at runtime

        Returns
        -------
        expressions.Select
        """
        snapshots_parameters = entry.snapshots_parameters

        # Distinct point-in-time table: Map distinct POINT_IN_TIME values to snapshot adjusted
        # POINT_IN_TIME values
        if snapshots_parameters.feature_job_setting is None:
            job_datetime_column_name = None
            datetime_expr_to_adjust = quoted_identifier(SpecialColumnName.POINT_IN_TIME)
        else:
            job_datetime_column_name = get_request_table_job_datetime_column_name(
                snapshots_parameters.feature_job_setting, self.adapter.source_type
            )
            datetime_expr_to_adjust = quoted_identifier(job_datetime_column_name)
        adjusted_point_in_time_expr = apply_snapshot_adjustment(
            datetime_expr=datetime_expr_to_adjust,
            time_interval=snapshots_parameters.time_interval,
            feature_job_setting=snapshots_parameters.feature_job_setting,
            format_string=snapshots_parameters.snapshot_timestamp_format_string,
            offset_size=snapshots_parameters.offset_size,
            adapter=self.adapter,
        )
        distinct_point_in_time_to_adjusted_expr = expressions.select(
            quoted_identifier(SpecialColumnName.POINT_IN_TIME),
            *[quoted_identifier(serving_name) for serving_name in entry.serving_names],
            expressions.alias_(
                adjusted_point_in_time_expr,
                InternalName.SNAPSHOTS_ADJUSTED_POINT_IN_TIME,
                quoted=True,
            ),
        ).from_(
            expressions.select(
                quoted_identifier(SpecialColumnName.POINT_IN_TIME),
                *(
                    [quoted_identifier(job_datetime_column_name)]
                    if job_datetime_column_name
                    else []
                ),
                *[quoted_identifier(serving_name) for serving_name in entry.serving_names],
            )
            .distinct()
            .from_(expressions.Table(this=quoted_identifier(request_table_name)))
            .subquery()
        )

        # Distinct adjusted point-in-time table: Distinct snapshot adjusted POINT_IN_TIME values for joining
        # with source tables
        distinct_adjusted_point_in_time_expr = (
            expressions.select(
                quoted_identifier(InternalName.SNAPSHOTS_ADJUSTED_POINT_IN_TIME),
                *[quoted_identifier(serving_name) for serving_name in entry.serving_names],
            )
            .distinct()
            .from_(expressions.Table(this=quoted_identifier(entry.distinct_point_in_time_table)))
        )
        return [
            CommonTable(
                name=entry.distinct_point_in_time_table,
                expr=distinct_point_in_time_to_adjusted_expr,
            ),
            CommonTable(
                name=entry.distinct_adjusted_point_in_time_table,
                expr=distinct_adjusted_point_in_time_expr,
            ),
        ]
