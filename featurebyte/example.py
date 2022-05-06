"""Example of code."""


def hello(name: str) -> str:
    """
    Just an greetings example.

    Parameters
    ----------
    name : str
        Name to greet.

    Returns
    -------
    message : str
        Greeting message

    Examples
    --------
    >>> hello("Roman")
    'Hello Roman!'
    """
    return f"Hello {name}!"
