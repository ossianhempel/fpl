import pytest
from typing import Generator
from prefect.testing.utilities import prefect_test_harness
from src.prefect_files.flows import fpl_pipeline_flow


@pytest.fixture(scope="session")
def prefect_test_fixture() -> Generator[None, None, None]:
    with prefect_test_harness():
        yield  # turns prefect_test_harness into a generator (lazy eval, preserves state)


def test_fpl_pipeline_flow() -> None:
    assert (
        fpl_pipeline_flow.download_gws_task
    ), "Task 'download_gws' was not found in flow"
