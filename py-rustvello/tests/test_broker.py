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
