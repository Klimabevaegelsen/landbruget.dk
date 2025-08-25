#!/usr/bin/env python3
"""
Quick local test for the optimized CadastralSilver implementation.

This script runs the cadastral silver processing locally to test the bulk insert optimization.
Run this from the project root directory.
"""

import subprocess
import sys
import time
from pathlib import Path


def run_cadastral_silver_test():
    """Run the cadastral silver processing test."""
    
    print("🧪 Testing Cadastral Silver Optimization Locally")
    print("=" * 60)
    print()
    
    # Change to the unified pipeline directory
    pipeline_dir = Path("backend/pipelines/unified_pipeline")
    
    if not pipeline_dir.exists():
        print(f"❌ Pipeline directory not found: {pipeline_dir}")
        print("Make sure you're running this from the project root directory")
        return False
    
    print(f"📁 Working directory: {pipeline_dir.absolute()}")
    print()
    
    # Build the command
    cmd = [
        sys.executable, "-m", "unified_pipeline",
        "-s", "cadastral",
        "-j", "silver"
    ]
    
    print(f"🚀 Running command: {' '.join(cmd)}")
    print("   This will run ONLY the cadastral silver stage with the new bulk insert optimization")
    print()
    
    # Record start time
    start_time = time.time()
    
    try:
        # Run the command
        result = subprocess.run(
            cmd,
            cwd=pipeline_dir,
            capture_output=True,
            text=True,
            timeout=1800  # 30 minute timeout
        )
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        print(f"⏱️  Total processing time: {processing_time:.2f} seconds ({processing_time/60:.1f} minutes)")
        print()
        
        if result.returncode == 0:
            print("✅ Cadastral silver processing completed successfully!")
            print()
            print("📊 STDOUT:")
            print(result.stdout)
            
            if result.stderr:
                print("⚠️  STDERR:")
                print(result.stderr)
            
            # Performance assessment
            print("\n📈 Performance Assessment:")
            if processing_time < 300:  # Less than 5 minutes
                print("🚀 EXCELLENT: Processing completed in under 5 minutes!")
            elif processing_time < 600:  # Less than 10 minutes
                print("✅ GOOD: Processing completed in under 10 minutes")
            elif processing_time < 1200:  # Less than 20 minutes
                print("⚠️  ACCEPTABLE: Processing took under 20 minutes (better than before)")
            else:
                print("❌ SLOW: Processing took over 20 minutes - optimization may not be working")
            
            return True
            
        else:
            print(f"❌ Cadastral silver processing failed with exit code: {result.returncode}")
            print()
            print("📊 STDOUT:")
            print(result.stdout)
            print()
            print("📊 STDERR:")
            print(result.stderr)
            return False
            
    except subprocess.TimeoutExpired:
        end_time = time.time()
        processing_time = end_time - start_time
        print(f"⏰ Process timed out after {processing_time:.2f} seconds (30 minutes)")
        print("This suggests the optimization may not be working properly")
        return False
        
    except Exception as e:
        end_time = time.time()
        processing_time = end_time - start_time
        print(f"❌ Error running command after {processing_time:.2f} seconds: {e}")
        return False


if __name__ == "__main__":
    print("Cadastral Silver Optimization Test")
    print("This will test the new bulk insert optimization by running only the silver stage")
    print()
    
    # Check if we're in the right directory
    if not Path("backend/pipelines/unified_pipeline").exists():
        print("❌ Error: This script must be run from the project root directory")
        print("Current directory:", Path.cwd())
        sys.exit(1)
    
    # Run the test
    success = run_cadastral_silver_test()
    
    if success:
        print("\n🎉 Test completed successfully!")
        print("The optimization appears to be working correctly.")
        sys.exit(0)
    else:
        print("\n❌ Test failed!")
        print("Check the output above for details.")
        sys.exit(1)
