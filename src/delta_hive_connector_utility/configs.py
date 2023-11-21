from pathlib import Path

from dotenv import load_dotenv

import delta_hive_connector_utility

PROJECT_BASE = Path(delta_hive_connector_utility.__file__).parent
DOTENV_FILE_PATH = PROJECT_BASE / '.env.prod'

load_dotenv(DOTENV_FILE_PATH)
