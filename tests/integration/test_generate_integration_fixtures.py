"""
Generate integration fixtures
"""
import numpy as np
import pandas as pd
import pytest


@pytest.fixture(name="scd_dataframe", scope="session")
def scd_dataframe_fixture(transaction_data):
    """
    DataFrame fixture with slowly changing dimension
    """
    rng = np.random.RandomState(0)  # pylint: disable=no-member

    natural_key_values = sorted(transaction_data["üser id"].unique())
    dates = pd.to_datetime(transaction_data["ëvent_timestamp"], utc=True).dt.floor("d")
    effective_timestamp_values = pd.date_range(dates.min(), dates.max())
    # Add variations at hour level
    effective_timestamp_values += pd.to_timedelta(
        rng.randint(0, 24, len(effective_timestamp_values)), unit="h"
    )
    values = [f"STÀTUS_CODE_{i}" for i in range(50)]

    num_rows = 1000
    data = pd.DataFrame(
        {
            "Effective Timestamp": rng.choice(effective_timestamp_values, num_rows),
            "User ID": rng.choice(natural_key_values, num_rows),
            "User Status": rng.choice(values, num_rows),
            "ID": np.arange(num_rows),
        }
    )
    # Ensure there is only one active record per natural key as at any point in time
    data = (
        data.drop_duplicates(["User ID", "Effective Timestamp"])
        .sort_values(["User ID", "Effective Timestamp"])
        .reset_index(drop=True)
    )
    index = data.index
    current_flag = data.groupby("User ID", as_index=False).agg({"Effective Timestamp": "max"})
    current_flag["Current Flag"] = True
    data = pd.merge(data, current_flag, on=["User ID", "Effective Timestamp"], how="left")
    data["Current Flag"] = data["Current Flag"].fillna(False)
    data.index = index
    return data


@pytest.fixture(name="csv_to_df", scope="session")
def csv_to_df_fixture(scd_dataframe):
    """
    Fixture that is a dictionary of CSV file names to the dataframe that is written to it.
    """
    return {
        "scd_data.csv": scd_dataframe,
    }


def test_save_payload_fixtures(update_fixtures, csv_to_df):
    """
    Save payload fixtures.
    """
    if update_fixtures:
        directory = "tests/fixtures/dataframe"
        for csv, df in csv_to_df.items():
            df.to_csv(f"{directory}/{csv}")
