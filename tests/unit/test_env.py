"""Unit: хелперы загрузки окружения (olist_ml.defs.env) — строгие, без
дефолтов.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from olist_ml.defs import env

pytestmark = pytest.mark.unit


def test_env_list_splits_and_strips(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("X_LIST", " a, b ,,c ")
    assert env.env_list("X_LIST") == ["a", "b", "c"]


def test_env_path_relative_becomes_absolute(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("X_PATH", "data/x.duckdb")
    path = env.env_path("X_PATH")
    assert path == env.PROJECT_ROOT / "data" / "x.duckdb"
    # записывается обратно — чтобы dbt в подпроцессе увидел тот же путь
    import os

    assert os.environ["X_PATH"] == str(path)


def test_env_path_absolute_is_kept(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("X_PATH", str(tmp_path / "db.duckdb"))
    assert env.env_path("X_PATH") == tmp_path / "db.duckdb"


def test_require_lists_all_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("X_MISSING_1", raising=False)
    monkeypatch.setenv("X_EMPTY", "")
    with pytest.raises(RuntimeError) as err:
        env.require("X_MISSING_1", "X_EMPTY")
    assert "X_MISSING_1" in str(err.value) and "X_EMPTY" in str(err.value)


def test_require_passes_when_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("X_SET", "1")
    env.require("X_SET")


def test_load_env_file_missing_points_to_template(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match=r"\.env\.template"):
        env.load_env_file(tmp_path / ".env")


def test_load_env_file_does_not_override(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text("X_KEEP=from_file\nX_NEW=from_file\n")
    monkeypatch.setenv("X_KEEP", "from_shell")
    monkeypatch.delenv("X_NEW", raising=False)
    env.load_env_file(dotenv)
    import os

    assert os.environ["X_KEEP"] == "from_shell"
    assert os.environ["X_NEW"] == "from_file"
    monkeypatch.delenv("X_NEW")


def test_required_vars_loaded_for_session() -> None:
    import os

    for name in env.REQUIRED:
        assert os.environ.get(name), name
    # conftest подменил DUCKDB_PATH на временный — .env его не перетёр
    assert "olist-ml-test-run-" in str(env.DUCKDB_PATH)
