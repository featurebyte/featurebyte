"""
Setup for running doctests.
"""

import featurebyte as fb


def setup() -> None:
    """
    Setup featurebyte environment for running doctests.
    """
    # start playground
    fb.playground(local=True, datasets=["grocery"])

    # create catalog
    fb.Catalog.get_or_create("grocery")


if __name__ == "__main__":
    setup()
