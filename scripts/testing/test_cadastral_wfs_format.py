#!/usr/bin/env python3
"""
Test script to check the actual WFS coordinate format from Datafordeler.
This will help us understand if coordinates are 2D or 3D and fix the parsing bug.
"""

import os
import sys
import xml.etree.ElementTree as ET

import requests


def test_wfs_coordinate_format():
    """Test the actual WFS response to understand coordinate format"""

    # Get credentials
    username = os.getenv("DATAFORDELER_USERNAME") or os.getenv("WFS_USERNAME")
    password = os.getenv("DATAFORDELER_PASSWORD") or os.getenv("WFS_PASSWORD")

    if not username or not password:
        print("❌ ERROR: No WFS credentials found")
        print("Set DATAFORDELER_USERNAME/PASSWORD or WFS_USERNAME/PASSWORD")
        return False

    print(f"🔑 Using credentials: {username}")

    # WFS request parameters
    url = "https://wfs.datafordeler.dk/MATRIKLEN2/MatGaeldendeOgForeloebigWFS/1.0.0/WFS"
    params = {
        "SERVICE": "WFS",
        "REQUEST": "GetFeature",
        "VERSION": "2.0.0",
        "TYPENAMES": "mat:SamletFastEjendom_Gaeldende",
        "SRSNAME": "EPSG:25832",
        "startIndex": "0",
        "count": "2",  # Just get 2 features for testing
    }

    print("🌐 Making WFS request...")
    try:
        response = requests.get(url, params=params, auth=(username, password), timeout=60)
        print(f"📡 Response status: {response.status_code}")

        if response.status_code != 200:
            print(f"❌ ERROR: {response.text[:500]}")
            return False

        # Parse XML
        root = ET.fromstring(response.text)
        namespaces = {
            "gml": "http://www.opengis.net/gml/3.2",
            "mat": "http://data.gov.dk/schemas/matrikel/1",
            "wfs": "http://www.opengis.net/wfs/2.0",
        }

        # Find all posList elements
        pos_lists = root.findall(".//gml:posList", namespaces)
        print(f"📍 Found {len(pos_lists)} posList elements")

        if not pos_lists:
            print("❌ No posList elements found in response")
            return False

        # Analyze first few posList elements
        for i, pos_list in enumerate(pos_lists[:3]):
            if not pos_list.text:
                continue

            coords_text = pos_list.text.strip()
            coords = coords_text.split()

            print(f"\n🔍 PosList {i + 1}:")
            print(f"   Raw text: {coords_text[:100]}{'...' if len(coords_text) > 100 else ''}")
            print(f"   Total coordinates: {len(coords)}")
            print(f"   First 12 coords: {coords[:12]}")

            # Test different parsing approaches
            print("\n   📊 Testing parsing approaches:")

            # Approach 1: Step by 2 (2D coordinates)
            print("   🔸 Step-by-2 (2D parsing):")
            pairs_2d = []
            for j in range(0, min(12, len(coords)), 2):
                if j + 1 < len(coords):
                    x, y = float(coords[j]), float(coords[j + 1])
                    pairs_2d.append((x, y))

                    # Check if coordinates are in valid UTM range
                    utm_x_valid = 400000 <= x <= 900000
                    utm_y_valid = 6000000 <= y <= 7000000
                    status = "✅ VALID UTM" if utm_x_valid and utm_y_valid else "❌ INVALID"

                    print(f"      Pair {len(pairs_2d)}: ({x}, {y}) - {status}")

            # Approach 2: Step by 3 (3D coordinates)
            print("\n   🔹 Step-by-3 (3D parsing):")
            pairs_3d = []
            for j in range(0, min(12, len(coords)), 3):
                if j + 2 < len(coords):
                    x, y, z = float(coords[j]), float(coords[j + 1]), float(coords[j + 2])
                    pairs_3d.append((x, y, z))

                    # Check if coordinates are in valid UTM range
                    utm_x_valid = 400000 <= x <= 900000
                    utm_y_valid = 6000000 <= y <= 7000000
                    status = "✅ VALID UTM" if utm_x_valid and utm_y_valid else "❌ INVALID"

                    print(f"      Triplet {len(pairs_3d)}: ({x}, {y}, {z}) - {status}")

            # Determine which approach gives more valid coordinates
            valid_2d = sum(1 for x, y in pairs_2d if 400000 <= x <= 900000 and 6000000 <= y <= 7000000)
            valid_3d = sum(1 for x, y, z in pairs_3d if 400000 <= x <= 900000 and 6000000 <= y <= 7000000)

            print("\n   📈 Results:")
            print(
                f"      2D parsing: {valid_2d}/{len(pairs_2d)} valid coordinates ({valid_2d / len(pairs_2d) * 100:.1f}%)"
            )
            print(
                f"      3D parsing: {valid_3d}/{len(pairs_3d)} valid coordinates ({valid_3d / len(pairs_3d) * 100:.1f}%)"
            )

            if valid_2d > valid_3d:
                print("   ✅ CONCLUSION: 2D parsing (step-by-2) produces more valid coordinates")
            elif valid_3d > valid_2d:
                print("   ✅ CONCLUSION: 3D parsing (step-by-3) produces more valid coordinates")
            else:
                print("   ❓ CONCLUSION: Both approaches produce similar results")

        return True

    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False


if __name__ == "__main__":
    success = test_wfs_coordinate_format()
    sys.exit(0 if success else 1)
