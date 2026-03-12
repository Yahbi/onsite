import os

# Core toggles
ENRICH_ENABLED = os.getenv("ENRICH_ENABLED", "0") == "1"
USE_MASTER_ENRICH = os.getenv("USE_MASTER_ENRICH", "0") == "1"
ENRICH_INTERVAL_SEC = float(os.getenv("ENRICH_INTERVAL_SEC", "600"))

# Database (support multiple aliases)
DB_PATH = (
    os.getenv("PERMITS_DB_PATH")
    or os.getenv("ENRICH_DB_PATH")
    or os.getenv("DB_PATH")
    or "leads.db"
)
ENRICH_BATCH_SIZE = int(os.getenv("ENRICH_BATCH_SIZE", "50"))

# Provider keys / URLs (all via environment only)
ONE_LOOKUP_API_KEY = os.getenv("ONE_LOOKUP_API_KEY") or os.getenv("ONELOOKUP_API_KEY") or ""
ONE_LOOKUP_URL = os.getenv("ONE_LOOKUP_URL", "https://api.1lookup.io/v1/person")

ALT_LOOKUP_KEY = os.getenv("ALT_LOOKUP_KEY", "")
ALT_LOOKUP_URL = os.getenv("ALT_LOOKUP_URL", "")

PROPERTY_API_KEY = os.getenv("PROPERTY_API_KEY", "")
PROPERTY_API_URL = os.getenv("PROPERTY_API_URL", "")

GEOCODE_API_KEY = os.getenv("GEOCODE_API_KEY", "")
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN", "")

# Networking
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "12"))

# Confidence thresholds (support CONTACT_CONF_THRESHOLD alias)
CONTACT_CONFIDENCE_THRESHOLD = float(
    os.getenv("CONTACT_CONFIDENCE_THRESHOLD")
    or os.getenv("CONTACT_CONF_THRESHOLD")
    or "0.75"
)

# Optional assessor overrides
LA_ASSESSOR_URL = os.getenv("LA_ASSESSOR_URL", "")
