"""Default values for starting the indexer."""

try:
    from typing import Final
except ImportError:
    from typing_extensions import Final  # type: ignore[misc]

_AGGREGATE_LATENCY_MINUTES: Final[int] = 30  # minutes

PATHS = None
PATHS_FILE = ""
BLACKLIST = None
BLACKLIST_FILE = ""
URL = "https://file-catalog.icecube.wisc.edu/"
TIMEOUT = 30  # seconds
RETRIES = int((60 / TIMEOUT) * _AGGREGATE_LATENCY_MINUTES)
BASIC_ONLY = False
PATCH = False
ICEPRODV2_RC_TOKEN = ""
ICEPRODV1_DB_PASS = ""
DRYRUN = False
NON_RECURSIVE = False
N_PROCESSES = 1
