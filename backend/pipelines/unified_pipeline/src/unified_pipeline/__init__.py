from dotenv import find_dotenv, load_dotenv

# Load environment variables early for all modules
dotenv_path = find_dotenv()
if dotenv_path:
    load_dotenv(dotenv_path)
