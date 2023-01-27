from typing import Any, Dict

import pandas as pd
import requests


def request_features(entity_serving_names: Dict[str, Any]) -> pd.DataFrame:
    """
    Send POST request to online serving endpoint

    Parameters
    ----------
    entity_serving_names: Dict[str, Any]
        Entity serving name values to used for serving request

    Returns
    -------
    pd.DataFrame
    """
    response = requests.post(
        url="{{serving_url}}",
        headers={{headers}},
        json={"entity_serving_names": entity_serving_names},
    )
    assert response.status_code == 200, response.json()
    return pd.DataFrame.from_dict(response.json()["features"])


request_features({{entity_serving_names}})
