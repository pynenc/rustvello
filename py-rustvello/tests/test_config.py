"""Tests for AppConfig and TaskConfig."""

import pytest

from rustvello import AppConfig, TaskConfig


class TestTaskConfig:
    def test_defaults(self):
        cfg = TaskConfig()
        assert cfg.max_retries == 0
        assert cfg.cache_results is False
        assert cfg.running_concurrency is None

    def test_custom_values(self):
        cfg = TaskConfig(max_retries=3, concurrency_control="task", running_concurrency=5, cache_results=True)
        assert cfg.max_retries == 3
        assert cfg.cache_results is True
        assert cfg.running_concurrency == 5

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
        cfg = AppConfig(app_id="my_app", dev_mode_force_sync=True)
        assert cfg.app_id == "my_app"
        assert cfg.dev_mode_force_sync is True

    def test_repr(self):
        cfg = AppConfig()
        r = repr(cfg)
        assert "rustvello" in r
