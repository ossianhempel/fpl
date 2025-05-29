import pytest
from typing import Generator
from prefect.testing.utilities import prefect_test_harness
from src.prefect_files.tasks.fpl_source_extraction_tasks import (
    download_gws_task,
    download_teams_task,
    download_fixtures_task,
)


@pytest.fixture(scope="session")
def prefect_test_fixture() -> Generator[None, None, None]:
    with prefect_test_harness():
        yield  # turns prefect_test_harness into a generator (lazy eval, preserves state)


def test_fpl_pipeline_flow() -> None:
    assert download_gws_task, "Task 'download_gws' was not found in flow"
    assert download_teams_task, "Task 'download_teams' was not found in flow"
    assert download_fixtures_task, "Task 'download_fixtures' was not found in flow"
