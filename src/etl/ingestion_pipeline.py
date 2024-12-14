

import sys
from pathlib import Path

# Add src/components directory to Python path to enable imports
components_path = str(Path(__file__).parent.parent / "components")
if components_path not in sys.path:
    sys.path.append(components_path)


# TODO: glue together the ingestion pipeline from the components

from components.fpl.ingest_fixtures import ingest_data as ingest_fixtures
from components.fpl.ingest_gameweeks import ingest_data as ingest_gameweeks

