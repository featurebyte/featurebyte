"""Unit tests for the ProgressHistory model."""

from collections import defaultdict

from featurebyte.models.task import ProgressHistory


def test_compress_no_action_needed():
    """
    Test when the number of messages is less than or equal to max_messages.
    Compression should not occur.
    """
    data = [
        [10, "Message at 10%"],
        [20, "Message at 20%"],
        [30, "Message at 30%"],
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
        [10, "First message at 10%"],
        [10, "Second message at 10%"],
        [20, "First message at 20%"],
        [20, "Second message at 20%"],
        [30, "Message at 30%"],
        [40, "Message at 40%"],
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
        data.append([i, f"Message at {i}% - first"])
        data.append([i, f"Message at {i}% - second"])

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
        [5, "Message at 5%"],
        [10, "First message at 10%"],
        [10, "Second message at 10%"],
        [15, "First message at 15%"],
        [15, "Second message at 15%"],
        [20, "Message at 20%"],
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
        [0, "First message at 0%"],
        [0, "Second message at 0%"],
        [50, "Message at 50%"],
        [100, "First message at 100%"],
        [100, "Second message at 100%"],
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
        [10, "Message at 10%"],
        [20, "Message at 20%"],
        [30, "Message at 30%"],
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
        [50, "Message 1 at 50%"],
        [50, "Message 2 at 50%"],
        [50, "Message 3 at 50%"],
    ]
    progress_history = ProgressHistory(data=data)
    progress_history.compress(max_messages=1)

    assert len(progress_history.data) == 1
    assert progress_history.data[0].message == "Message 3 at 50%"
    assert progress_history.compress_at == 50
