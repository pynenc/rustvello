"""Tests for RustMemTriggerStore."""

import json

import pytest

from rustvello import RustMemTriggerStore


class TestRustMemTriggerStore:
    def test_create(self):
        store = RustMemTriggerStore()
        assert store is not None

    def test_register_and_get_condition(self):
        store = RustMemTriggerStore()
        condition = {
            "Status": {
                "task_id": {"module": "mod", "name": "func"},
                "statuses": ["Success"],
            }
        }
        cid = store.register_condition(json.dumps(condition))
        assert isinstance(cid, str)
        assert len(cid) > 0

        # Retrieve it
        retrieved = store.get_condition(cid)
        assert retrieved is not None
        parsed = json.loads(retrieved)
        assert "Status" in parsed

    def test_get_nonexistent_condition(self):
        store = RustMemTriggerStore()
        result = store.get_condition("nonexistent-id")
        assert result is None

    def test_invalid_condition_json(self):
        store = RustMemTriggerStore()
        with pytest.raises(ValueError):
            store.register_condition("not valid json")

    def test_report_status_change(self):
        store = RustMemTriggerStore()

        # Register a status condition
        condition = {
            "Status": {
                "task_id": {"module": "mod", "name": "func"},
                "statuses": ["Success"],
            }
        }
        store.register_condition(json.dumps(condition))

        # Report a status change
        valid = store.report_status_change(
            "550e8400-e29b-41d4-a716-446655440001",
            "mod",
            "func",
            "SUCCESS",
        )
        assert isinstance(valid, list)

    def test_evaluate_cron_conditions_empty(self):
        store = RustMemTriggerStore()
        result = store.evaluate_cron_conditions()
        assert result == []

    def test_evaluate_triggers_empty(self):
        store = RustMemTriggerStore()
        result = store.evaluate_triggers()
        assert result == []
