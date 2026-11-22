import importlib
import os
from pathlib import Path

import pytest

from tio_spark import env


def test_env_should_override_shell_variable_with_dotenv_value(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    (tmp_path / ".env").write_text("TIO_SPARK_TEST_HOME=/opt/spark/from-dotenv\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TIO_SPARK_TEST_HOME", "/opt/spark/from-shell")

    # Act
    importlib.reload(env)

    # Assert
    actual = os.environ["TIO_SPARK_TEST_HOME"]
    expected = "/opt/spark/from-dotenv"
    assert actual == expected


def test_env_should_keep_shell_variable_absent_from_dotenv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    (tmp_path / ".env").write_text("TIO_SPARK_TEST_HOME=/opt/spark/from-dotenv\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TIO_SPARK_TEST_HOME", "/opt/spark/from-shell")
    monkeypatch.setenv("TIO_SPARK_TEST_IP", "10.0.0.1")

    # Act
    importlib.reload(env)

    # Assert
    actual = os.environ["TIO_SPARK_TEST_IP"]
    expected = "10.0.0.1"
    assert actual == expected


def test_env_should_load_dotenv_from_parent_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Arrange
    (tmp_path / ".env").write_text("TIO_SPARK_TEST_HOME=/opt/spark/from-dotenv\n")
    (tmp_path / "jobs").mkdir()
    monkeypatch.chdir(tmp_path / "jobs")
    monkeypatch.setenv("TIO_SPARK_TEST_HOME", "/opt/spark/from-shell")

    # Act
    importlib.reload(env)

    # Assert
    actual = os.environ["TIO_SPARK_TEST_HOME"]
    expected = "/opt/spark/from-dotenv"
    assert actual == expected
