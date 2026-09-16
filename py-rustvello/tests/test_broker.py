"""Tests for RustMemBroker."""

from rustvello import RustMemBroker

UUID1 = "550e8400-e29b-41d4-a716-446655440001"
UUID2 = "550e8400-e29b-41d4-a716-446655440002"
UUID3 = "550e8400-e29b-41d4-a716-446655440003"


class TestRustMemBroker:
    def test_new_broker_is_empty(self):
        broker = RustMemBroker()
        assert broker.count_invocations() == 0

    def test_route_and_retrieve_single(self):
        broker = RustMemBroker()
        broker.route_invocation(UUID1)
        assert broker.count_invocations() == 1

        retrieved = broker.retrieve_invocation()
        assert retrieved == UUID1
        assert broker.count_invocations() == 0

    def test_fifo_ordering(self):
        broker = RustMemBroker()
        broker.route_invocation(UUID1)
        broker.route_invocation(UUID2)
        broker.route_invocation(UUID3)
        assert broker.count_invocations() == 3

        assert broker.retrieve_invocation() == UUID1
        assert broker.retrieve_invocation() == UUID2
        assert broker.retrieve_invocation() == UUID3
        assert broker.retrieve_invocation() is None

    def test_route_batch(self):
        broker = RustMemBroker()
        broker.route_invocations([UUID1, UUID2])
        assert broker.count_invocations() == 2

    def test_retrieve_from_empty(self):
        broker = RustMemBroker()
        assert broker.retrieve_invocation() is None

    def test_purge_clears_queue(self):
        broker = RustMemBroker()
        broker.route_invocation(UUID1)
        broker.route_invocation(UUID2)
        broker.purge()
        assert broker.count_invocations() == 0


class TestRustMemBrokerNamedQueues:
    """Queue-aware bindings used by pynenc named queues (pynenc >= 0.4)."""

    def test_route_to_queue_and_retrieve_by_queue(self):
        broker = RustMemBroker()
        broker.route_invocation_to_queue(UUID1, "payments", 0.0, task_module="m", task_name="pay")
        broker.route_invocation_to_queue(UUID2, "reports", 0.0, task_module="m", task_name="report")
        assert broker.count_invocations_in_queues(["payments"]) == 1
        assert broker.count_invocations_in_queues(["payments", "reports"]) == 2
        assert broker.retrieve_invocation_from_queue("reports", language="python") == UUID2
        assert broker.retrieve_invocation_from_queue("reports", language="python") is None
        assert broker.retrieve_invocation_from_queue("payments", language="python") == UUID1

    def test_priority_wins_then_fifo(self):
        broker = RustMemBroker()
        broker.route_invocation_to_queue(UUID1, "q", -100.0, task_module="m", task_name="t")
        broker.route_invocation_to_queue(UUID2, "q", 100.0, task_module="m", task_name="t")
        broker.route_invocation_to_queue(UUID3, "q", 100.0, task_module="m", task_name="t")
        assert broker.retrieve_invocation_from_queue("q", language="python") == UUID2
        assert broker.retrieve_invocation_from_queue("q", language="python") == UUID3
        assert broker.retrieve_invocation_from_queue("q", language="python") == UUID1

    def test_task_less_rows_stay_off_the_python_lane(self):
        broker = RustMemBroker()
        broker.route_invocation_to_queue(UUID1, "q", 0.0)
        assert broker.retrieve_invocation_from_queue("q", language="python") is None
        assert broker.retrieve_invocation_from_queue("q") == UUID1

    def test_batch_and_task_filter(self):
        broker = RustMemBroker()
        broker.route_invocations_to_queue([UUID1, UUID2], "q", 1.0, task_module="m", task_name="a")
        broker.route_invocation_to_queue(UUID3, "q", 1.0, task_module="m", task_name="b")
        assert broker.count_invocations_in_queues(["q"], task_module="m", task_name="a") == 2
        assert broker.retrieve_invocation_from_queue("q", task_module="m", task_name="b") == UUID3
        assert broker.count_invocations_in_queues(["q"]) == 2

    def test_module_and_name_come_together(self):
        import pytest

        broker = RustMemBroker()
        with pytest.raises(ValueError):
            broker.route_invocation_to_queue(UUID1, "q", 0.0, task_module="m")
