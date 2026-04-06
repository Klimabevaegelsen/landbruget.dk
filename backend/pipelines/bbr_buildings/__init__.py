"""BBR Buildings Pipeline Package."""

from dotenv import find_dotenv, load_dotenv

# Load environment variables early for all modules
dotenv_path = find_dotenv()
if dotenv_path:
    load_dotenv(dotenv_path)

from common.secrets import init_secrets  # noqa: E402

init_secrets()
