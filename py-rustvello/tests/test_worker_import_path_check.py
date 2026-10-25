"""A worker process whose ``import_path`` registers fewer tasks than the runner is refused at start-up."""

import pytest

from rustvello import App


@pytest.fixture
def empty_import_path(tmp_path, monkeypatch: pytest.MonkeyPatch) -> str:
    """An importable module whose App registers no task (Celery's forgotten ``include``)."""
    db_path = tmp_path / "pool.db"
    monkeypatch.setenv("RUSTVELLO_TEST_EMPTY_POOL_DB", str(db_path))
    (tmp_path / "empty_pool_app.py").write_text(
        "import os\nfrom rustvello import App\n"
        'app = App(app_id="import_path_check", backend="sqlite", db_path=os.environ["RUSTVELLO_TEST_EMPTY_POOL_DB"], '
        'import_path="empty_pool_app:app")\n'
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    return "empty_pool_app:app"


def test_worker_registering_fewer_tasks_stops_the_runner(empty_import_path: str, tmp_path) -> None:
    app = App(app_id="import_path_check", backend="sqlite", db_path=str(tmp_path / "pool.db"))

    @app.task
    def work(x: int) -> int:
        return x

    with pytest.raises(Exception, match="empty_pool_app:app.*missing.*work") as info:
        app.run(num_processes=1, block=True, import_path=empty_import_path)
    assert "import every module" in str(info.value)
