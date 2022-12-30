from featurebyte.query_graph.sql.tile_util import update_maximum_window_size_dict


def test_update_maximum_window_size_dict__none_considered_largest():
    """
    Test None handling when keeping track of maximum window size
    """
    max_windows = {}

    update_maximum_window_size_dict(max_windows, "a", 0)
    assert max_windows == {"a": 0}

    update_maximum_window_size_dict(max_windows, "a", 1)
    assert max_windows == {"a": 1}

    update_maximum_window_size_dict(max_windows, "a", None)
    assert max_windows == {"a": None}

    update_maximum_window_size_dict(max_windows, "a", 2)
    assert max_windows == {"a": None}

    update_maximum_window_size_dict(max_windows, "b", 10)
    assert max_windows == {"a": None, "b": 10}
