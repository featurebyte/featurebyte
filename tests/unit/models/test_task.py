"""Unit tests for the ProgressHistory model."""

from collections import defaultdict

from featurebyte.models.task import LogMessage, ProgressHistory


def test_compress_no_action_needed():
    """
    Test when the number of messages is less than or equal to max_messages.
    Compression should not occur.
    """
    data = [
        {"percent": 10, "message": "Message at 10%"},
        {"percent": 20, "message": "Message at 20%"},
        {"percent": 30, "message": "Message at 30%"},
    ]
    progress_history = ProgressHistory(data=data)
    progress_history.compress(max_messages=5)

    assert len(progress_history.data) == 3
    assert progress_history.compress_at == 0


def test_compress_basic():
    """
    Test basic compression where multiple messages exist for the same percent.
    """
    data = [
        {"percent": 10, "message": "First message at 10%"},
        {"percent": 10, "message": "Second message at 10%"},
        {"percent": 20, "message": "First message at 20%"},
        {"percent": 20, "message": "Second message at 20%"},
        {"percent": 30, "message": "Message at 30%"},
        {"percent": 40, "message": "Message at 40%"},
    ]
    progress_history = ProgressHistory(data=data)
    progress_history.compress(max_messages=4)

    assert len(progress_history.data) == 4
    assert progress_history.compress_at >= 20
    # Verify that only the last messages at 10% and 20% are kept
    percents = [msg.percent for msg in progress_history.data]
    messages = [msg.message for msg in progress_history.data]
    assert percents == [10, 20, 30, 40]
    assert "Second message at 10%" in messages
    assert "Second message at 20%" in messages
    assert "First message at 10%" not in messages
    assert "First message at 20%" not in messages


def test_compress_until_limit_reached():
    """
    Test compression continues until the total number of messages is less than or equal to max_messages.
    """
    data = []
    for i in range(1, 51):  # Create 50 messages at percent 1 to 50
        data.append({"percent": i, "message": f"Message at {i}% - first"})
        data.append({"percent": i, "message": f"Message at {i}% - second"})

    progress_history = ProgressHistory(data=data)
    progress_history.compress(max_messages=30)

    assert len(progress_history.data) == 30
    assert progress_history.compress_at == 50
    # Ensure that for compressed percents, only the last message is kept
    percent_counts = defaultdict(int)
    for msg in progress_history.data:
        percent_counts[msg.percent] += 1
    for count in percent_counts.values():
        assert count == 1  # Only one message per percent


def test_compress_with_existing_compress_at():
    """
    Test compression starting from an existing compress_at value.
    """
    data = [
        {"percent": 5, "message": "Message at 5%"},
        {"percent": 10, "message": "First message at 10%"},
        {"percent": 10, "message": "Second message at 10%"},
        {"percent": 15, "message": "First message at 15%"},
        {"percent": 15, "message": "Second message at 15%"},
        {"percent": 20, "message": "Message at 20%"},
    ]
    progress_history = ProgressHistory(data=data, compress_at=10)
    progress_history.compress(max_messages=4)

    assert len(progress_history.data) == 4
    assert progress_history.compress_at == 15
    percents = [msg.percent for msg in progress_history.data]
    assert 5 in percents  # Should not compress messages before compress_at
    assert 10 in percents  # compress_at is 10, so compression starts here
    assert 15 in percents
    assert 20 in percents


def test_compress_edge_case_zero_and_hundred_percent():
    """
    Test edge cases with messages at 0% and 100%.
    """
    data = [
        {"percent": 0, "message": "First message at 0%"},
        {"percent": 0, "message": "Second message at 0%"},
        {"percent": 50, "message": "Message at 50%"},
        {"percent": 100, "message": "First message at 100%"},
        {"percent": 100, "message": "Second message at 100%"},
    ]
    progress_history = ProgressHistory(data=data)
    progress_history.compress(max_messages=3)

    assert len(progress_history.data) == 3
    assert progress_history.compress_at >= 0
    # Ensure only the last messages at 0% and 100% are kept
    percents = [msg.percent for msg in progress_history.data]
    assert percents == [0, 50, 100]
    messages = [msg.message for msg in progress_history.data]
    assert "Second message at 0%" in messages
    assert "Second message at 100%" in messages


def test_no_messages_to_compress():
    """
    Test when there are no messages to compress (each percent has only one message).
    """
    data = [
        {"percent": 10, "message": "Message at 10%"},
        {"percent": 20, "message": "Message at 20%"},
        {"percent": 30, "message": "Message at 30%"},
    ]
    progress_history = ProgressHistory(data=data)
    progress_history.compress(max_messages=3)

    assert len(progress_history.data) == 3
    assert progress_history.compress_at == 0


def test_compress_with_single_percent():
    """
    Test data where all messages are at the same percent.
    """
    data = [
        {"percent": 50, "message": "Message 1 at 50%"},
        {"percent": 50, "message": "Message 2 at 50%"},
        {"percent": 50, "message": "Message 3 at 50%"},
    ]
    progress_history = ProgressHistory(data=data)
    progress_history.compress(max_messages=1)

    assert len(progress_history.data) == 1
    assert progress_history.data[0].message == "Message 3 at 50%"
    assert progress_history.compress_at == 50


def test_progress_history_with_missing_message():
    """Test progress history with missing messages."""
    data = [{"percent": 0}]
    progress_history = ProgressHistory(data=data)
    assert progress_history.data == [LogMessage(percent=0, message=None)]
