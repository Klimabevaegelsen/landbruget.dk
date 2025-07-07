#!/usr/bin/env python3
"""
Debug script to test CHR pipeline locally and understand the 'int' object is not iterable error
"""

import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

# Add the backend directory to the path
backend_path = os.path.join(os.path.dirname(__file__), "..", "..")
sys.path.insert(0, backend_path)

# Set up logging to see what's happening
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def load_env_credentials():
    """Load credentials from .env file"""
    env_file = Path(__file__).parent.parent / ".env"  # Go up to pipelines directory
    if not env_file.exists():
        raise FileNotFoundError(f"No .env file found at {env_file}")

    credentials = {}
    with open(env_file, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                credentials[key.strip()] = value.strip().strip("\"'")

    username = credentials.get("FVM_USERNAME")
    password = credentials.get("FVM_PASSWORD")

    if not username or not password:
        raise ValueError(f"Missing FVM_USERNAME or FVM_PASSWORD in .env file. Found keys: {list(credentials.keys())}")

    return username, password


def test_chr_pipeline():
    """Test the CHR pipeline with problematic herds"""
    try:
        from bronze.load_chr_dyr import create_soap_client, load_animal_movements

        print("=== CHR Pipeline Debug Test ===")

        # Get credentials from .env file
        username, password = load_env_credentials()
        print(f"Got credentials for user: {username}")

        # Create CHR_dyr client
        chr_dyr_client = create_soap_client("https://webservice.fvm.dk/wsdl/CHR_dyr/CHR_dyr.wsdl", username, password)
        print("Created CHR_dyr client successfully")

        # Test with problematic herds from the logs
        test_herds = [46678, 45061]  # From the error logs
        start_date = date.today() - timedelta(days=7)  # Just 7 days to test quickly
        end_date = date.today()

        for test_herd in test_herds:
            print(f"\n--- Testing herd {test_herd} from {start_date} to {end_date} ---")

            try:
                # Call the function that's failing
                result = load_animal_movements(chr_dyr_client, username, test_herd, start_date, end_date)

                if result:
                    print("Success! Got result")
                    if isinstance(result, dict):
                        print(f"Result keys: {list(result.keys())}")
                        if "movements" in result:
                            print(f"Number of movements: {len(result['movements'])}")
                        if "summary_stats" in result:
                            print(f"Summary stats: {result['summary_stats']}")
                    else:
                        print(f"Result type: {type(result)}")
                else:
                    print("No result returned")

            except Exception as e:
                print(f"Error with herd {test_herd}: {e}")
                import traceback

                traceback.print_exc()

                # Let's also test the raw SOAP call to see what we get
                print(f"\n--- Testing raw SOAP call for herd {test_herd} ---")
                try:
                    from bronze.load_chr_dyr import _create_base_request

                    # Create the request
                    request = _create_base_request(username)
                    request.update(
                        {
                            "BesaetningsNummer": test_herd,
                            "PeriodeFra": start_date.strftime("%Y-%m-%d"),
                            "PeriodeTil": end_date.strftime("%Y-%m-%d"),
                        }
                    )

                    print(f"Making SOAP call with request: {request}")

                    # Make the raw SOAP call
                    response = chr_dyr_client.service.GetCHRDyrOplysninger(**request)
                    print(f"Raw response type: {type(response)}")
                    print(f"Raw response hasattr Response: {hasattr(response, 'Response')}")

                    if hasattr(response, "Response"):
                        resp_obj = response.Response[0] if isinstance(response.Response, list) else response.Response
                        print(f"Response object type: {type(resp_obj)}")
                        print(f"Response object attributes: {dir(resp_obj)}")

                        if hasattr(resp_obj, "Enkeltdyrsoplysninger"):
                            animals = getattr(resp_obj, "Enkeltdyrsoplysninger", None)
                            print(f"Enkeltdyrsoplysninger type: {type(animals)}")
                            print(f"Enkeltdyrsoplysninger value: {animals}")

                            # This is where the error probably happens
                            if isinstance(animals, int):
                                print(f"FOUND THE ISSUE: Enkeltdyrsoplysninger is an integer: {animals}")
                                print("This should be handled by the safety checks, but apparently it's not")
                            elif hasattr(animals, "__iter__"):
                                print(
                                    f"Enkeltdyrsoplysninger is iterable, length: {len(animals) if hasattr(animals, '__len__') else 'unknown'}"
                                )
                            else:
                                print(f"Enkeltdyrsoplysninger is not iterable: {type(animals)}")

                except Exception as raw_error:
                    print(f"Raw SOAP call error: {raw_error}")
                    import traceback

                    traceback.print_exc()

                print(f"--- End test for herd {test_herd} ---\n")

    except Exception as e:
        print(f"Setup error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    test_chr_pipeline()
