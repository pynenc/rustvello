"""Tests for AppConfig and TaskConfig."""

import pytest

from rustvello import AppConfig, TaskConfig


class TestTaskConfig:
    def test_defaults(self):
        cfg = TaskConfig()
        assert cfg.max_retries == 0
        assert cfg.cache_results is False
        assert cfg.running_concurrency is None
        assert cfg.queue == "default"
        assert cfg.priority == 0.0

    def test_custom_values(self):
        cfg = TaskConfig(
            max_retries=3,
            concurrency_control="task",
            running_concurrency=5,
            cache_results=True,
            queue="critical",
            priority=12.5,
        )
        assert cfg.max_retries == 3
        assert cfg.cache_results is True
        assert cfg.running_concurrency == 5
        assert cfg.queue == "critical"
        assert cfg.priority == 12.5

    def test_all_concurrency_types(self):
        for cc in ["unlimited", "task", "argument", "none"]:
            cfg = TaskConfig(concurrency_control=cc)
            assert cfg is not None

    def test_invalid_concurrency_type_raises(self):
        with pytest.raises(ValueError):
            TaskConfig(concurrency_control="invalid")

    def test_repr(self):
        cfg = TaskConfig(max_retries=2, cache_results=True)
        r = repr(cfg)
        assert "max_retries=2" in r
        assert "cache_results=true" in r


class TestAppConfig:
    def test_defaults(self):
        cfg = AppConfig()
        assert cfg.app_id == "rustvello"
        assert cfg.dev_mode_force_sync is False

    def test_custom(self):
        cfg = AppConfig(
            app_id="my_app",
            dev_mode_force_sync=True,
            broker_queues=["default", "critical"],
            runner_queues=["critical"],
            queue_selection_strategy="ordered",
            priority_rules=[("billing.*", 50.0)],
        )
        assert cfg.app_id == "my_app"
        assert cfg.dev_mode_force_sync is True
        assert cfg.broker_queues == ["default", "critical"]
        assert cfg.runner_queues == ["critical"]
        assert cfg.queue_selection_strategy == "ordered"
        assert cfg.priority_rules == [("billing.*", 50.0)]

    def test_repr(self):
        cfg = AppConfig()
        r = repr(cfg)
        assert "rustvello" in r
