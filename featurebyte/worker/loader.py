"""
Custom Celery loader to implement no prefetch behavior for tasks.
"""

from typing import Any

from celery.loaders.app import AppLoader


class NoPrefetchTaskLoader(AppLoader):
    def on_worker_init(self) -> None:
        # called when the worker starts, before logging setup
        super().on_worker_init()

        import kombu.transport.virtual

        builtin_can_consume = kombu.transport.virtual.QoS.can_consume

        def can_consume(self: kombu.transport.virtual.QoS) -> Any:
            """
            Patch for kombu.transport.virtual.QoS.can_consume

            Parameters
            ----------
            self : kombu.transport.virtual.QoS
                The QoS instance to check.

            Returns
            -------
            Any
                Returns True if the worker can consume messages, otherwise False.
            """
            if delegate := getattr(self, "delegate_can_consume", False):
                return delegate()  # type: ignore
            else:
                return builtin_can_consume(self)

        kombu.transport.virtual.QoS.can_consume = can_consume

        from celery import bootsteps
        from celery.worker import state as worker_state

        class Set_QoS_Delegate(bootsteps.StartStopStep):
            requires = {"celery.worker.consumer.tasks:Tasks"}

            def start(self, c: Any) -> None:
                def can_consume() -> Any:
                    """
                    delegate for QoS.can_consume

                    Returns
                    -------
                    Any
                        Returns True if the worker can consume messages, otherwise False.
                    """
                    # only consume if capacity is available
                    return len(worker_state.reserved_requests) < c.controller.concurrency

                c.task_consumer.channel.qos.delegate_can_consume = can_consume

        # add bootstep to Consumer blueprint
        self.app.steps["consumer"].add(Set_QoS_Delegate)
