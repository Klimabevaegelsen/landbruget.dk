import subprocess
import sys
import os
import bronze.export
import silver.transform

PIPELINE_ROOT = os.path.dirname(os.path.abspath(__file__))
print("[DEBUG] DISPLAY =", os.environ.get("DISPLAY"))
print("[DEBUG] DOCKER_ENV =", os.environ.get("DOCKER_ENV"))
if __name__ == "__main__":
    # Run Bronze Layer
    print("[main.py] Running Bronze Layer: export.py ...")
    bronze.export.main()
    print("[main.py] Bronze Layer complete.")

    # Run Silver Layer
    print("[main.py] Running Silver Layer: transform.py ...")
    silver.transform.main()
    print("[main.py] Silver Layer complete.")

    print("[main.py] Pipeline finished successfully.")
