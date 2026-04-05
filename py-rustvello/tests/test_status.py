"""Tests for InvocationStatus and ConcurrencyControlType."""

from rustvello import ConcurrencyControlType, InvocationStatus


class TestInvocationStatusConstructors:
    """All exposed status constructors produce distinct objects."""

    def test_registered(self):
        assert str(InvocationStatus.registered()) == "REGISTERED"

    def test_pending(self):
        assert str(InvocationStatus.pending()) == "PENDING"

    def test_running(self):
        assert str(InvocationStatus.running()) == "RUNNING"

    def test_success(self):
        assert str(InvocationStatus.success()) == "SUCCESS"

    def test_failed(self):
        assert str(InvocationStatus.failed()) == "FAILED"

    def test_retry(self):
        assert str(InvocationStatus.retry()) == "RETRY"

    def test_concurrency_controlled(self):
        assert str(InvocationStatus.concurrency_controlled()) == "CONCURRENCY_CONTROLLED"

    def test_concurrency_controlled_final(self):
        assert str(InvocationStatus.concurrency_controlled_final()) == "CONCURRENCY_CONTROLLED_FINAL"

    def test_rerouted(self):
        assert str(InvocationStatus.rerouted()) == "REROUTED"

    def test_pending_recovery(self):
        assert str(InvocationStatus.pending_recovery()) == "PENDING_RECOVERY"

    def test_running_recovery(self):
        assert str(InvocationStatus.running_recovery()) == "RUNNING_RECOVERY"


class TestInvocationStatusTerminal:
    def test_terminal_statuses(self):
        assert InvocationStatus.success().is_terminal()
        assert InvocationStatus.failed().is_terminal()
        assert InvocationStatus.concurrency_controlled_final().is_terminal()

    def test_non_terminal_statuses(self):
        non_terminal = [
            InvocationStatus.registered(),
            InvocationStatus.pending(),
            InvocationStatus.running(),
            InvocationStatus.retry(),
            InvocationStatus.concurrency_controlled(),
            InvocationStatus.rerouted(),
            InvocationStatus.pending_recovery(),
            InvocationStatus.running_recovery(),
        ]
        for s in non_terminal:
            assert not s.is_terminal(), f"{s} should not be terminal"


class TestInvocationStatusEquality:
    def test_same_status_equal(self):
        assert InvocationStatus.running() == InvocationStatus.running()

    def test_different_status_not_equal(self):
        assert InvocationStatus.running() != InvocationStatus.failed()

    def test_hash_consistency(self):
        a = InvocationStatus.pending()
        b = InvocationStatus.pending()
        assert hash(a) == hash(b)

    def test_repr_format(self):
        r = repr(InvocationStatus.success())
        assert "InvocationStatus" in r
        assert "SUCCESS" in r


class TestConcurrencyControlType:
    def test_unlimited(self):
        assert "Unlimited" in str(ConcurrencyControlType.unlimited())

    def test_task(self):
        assert "Task" in str(ConcurrencyControlType.task())

    def test_argument(self):
        assert "Argument" in str(ConcurrencyControlType.argument())

    def test_none(self):
        assert "None" in str(ConcurrencyControlType.none())

    def test_repr(self):
        r = repr(ConcurrencyControlType.unlimited())
        assert "ConcurrencyControlType" in r
