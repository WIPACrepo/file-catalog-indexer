# defaults.py
"""Default values for starting the indexer."""

BASIC_ONLY = False
DENYLIST = None
DENYLIST_FILE = ""
DRYRUN = False
FILE_CATALOG_REST_URL = "https://file-catalog.icecube.wisc.edu/"
ICEPROD_REST_URL = "https://iceprod2-api.icecube.wisc.edu"
ICEPRODV1_DB_PASS = ""
LOG_LEVEL = "INFO"
N_PROCESSES = 1
NON_RECURSIVE = False
OAUTH_CLIENT_ID = 'file-catalog-indexer'
OAUTH_URL = "https://keycloak.icecube.wisc.edu/auth/realms/IceCube"
PATCH = False
PATHS = None
PATHS_FILE = ""
REST_RETRIES = 10
REST_TIMEOUT = 60  # seconds
