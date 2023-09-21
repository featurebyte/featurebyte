"""
Test base router module
"""
from fastapi import APIRouter

from featurebyte.routes.base_router import BaseRouter


class RouterA(BaseRouter):
    """
    Router A for testing
    """

    def __init__(self) -> None:
        super().__init__(router=APIRouter(prefix="/a"))
        self.router.add_api_route(
            "/one",
            self.handler_one,
            methods=["GET"],
        )

    @staticmethod
    def handler_one() -> None:
        """
        Handler one function
        """


class RouterB(BaseRouter):
    """
    Router B for testing
    """

    def __init__(self) -> None:
        super().__init__(router=APIRouter(prefix="/b"))
        self.router.add_api_route(
            "/two",
            self.handler_two,
            methods=["GET"],
        )

    @staticmethod
    def handler_two() -> None:
        """
        Handler two function
        """


def test_base_router():
    """
    Test base router
    """
    router_a = RouterA()
    router_b = RouterB()
    assert router_a.router.routes[0].path == "/a/one"
    assert router_b.router.routes[0].path == "/b/two"

    # Test add_router
    router_a.add_router(router_b.router)
    assert len(router_a.router.routes) == 2
    assert router_a.router.routes[0].path == "/a/one"
    assert router_a.router.routes[1].path == "/b/two"

    # Test remove routes - noop here since there's no POST route matching /a/one
    router_a.remove_routes(
        {
            "/a/one": ["POST"],
        }
    )
    assert len(router_a.router.routes) == 2
    assert router_a.router.routes[0].path == "/a/one"
    assert router_a.router.routes[1].path == "/b/two"

    # Test remove route - should actually remove the matching GET route
    router_a.remove_routes(
        {
            "/a/one": ["GET"],
        }
    )
    assert len(router_a.router.routes) == 1
    assert router_a.router.routes[0].path == "/b/two"
