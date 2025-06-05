import argparse
import json
import logging
import sys  # Import sys for stdout
from pathlib import Path
from xml.etree import ElementTree as ET

# Configure logging to stdout
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", stream=sys.stdout)

# Updated namespace dictionary for parsing VetStat XML
NAMESPACES = {
    "soap": "http://schemas.xmlsoap.org/soap/envelope/",
    "eks": "http://vetstat.fvst.dk/ekstern",  # Updated to match actual namespace
}


def extract_data_from_xml_chunk(xml_chunk):
    """Parses a single SOAP XML response chunk and extracts data."""
    data_list = []
    try:
        # Add a dummy root element if needed (sometimes chunks might be fragments)
        if not xml_chunk.strip().startswith("<"):
            # Skip empty or non-XML chunks
            return []
        if not xml_chunk.strip().startswith("<soap:Envelope"):
            # Attempt to parse fragment directly if not a full envelope
            try:
                root = ET.fromstring(xml_chunk)
            except ET.ParseError:
                logging.warning(f"Skipping chunk due to parsing error (fragment attempt): {xml_chunk[:100]}...")
                return []
        else:
            root = ET.fromstring(xml_chunk)

        # Find all Data elements within the SOAP body
        body = root.find(".//soap:Body", NAMESPACES)
        if body is None:
            logging.warning(f"SOAP Body not found in chunk: {xml_chunk[:100]}...")
            return []

        # Look for the specific response structure containing Data elements
        response = body.find(".//eks:VetStat_CHRHentAntibiotikaForbrugResponse", NAMESPACES)
        if response is None:
            # Log the actual body content if response element is not found for debugging
            body_content_sample = ET.tostring(body, encoding="unicode", method="xml")[:200]
            logging.warning(
                f"VetStat_CHRHentAntibiotikaForbrugResponse not found in SOAP Body. Body starts with: {body_content_sample}..."
            )
            return []

        # Find the nested <eks:Response> element
        nested_response = response.find("eks:Response", NAMESPACES)
        if nested_response is None:
            # Log if the nested <eks:Response> is missing
            response_content_sample = ET.tostring(response, encoding="unicode", method="xml")[:200]
            logging.warning(
                f"Nested <eks:Response> element not found. Response content starts with: {response_content_sample}..."
            )
            return []

        # Find all Data elements within the nested response
        for data_elem in nested_response.findall(".//eks:Data", NAMESPACES):
            record = {}
            for child in data_elem:
                # Extract tag name without namespace and text content
                tag = child.tag.split("}", 1)[-1] if "}" in child.tag else child.tag
                record[tag] = child.text.strip() if child.text else None
            if record:
                data_list.append(record)

        if not data_list:
            # Use the extracted TrackID in the log message
            logging.warning("No Data elements found in the response.")  # DEBUG: Added TrackID

    except ET.ParseError as e:
        logging.error(f"XML parsing error: {e} in chunk starting with: {xml_chunk[:100]}...")
    except Exception as e:
        logging.error(f"Unexpected error processing chunk: {e}")

    return data_list


def parse_vetstat_xml(input_file: Path, output_file: Path):
    """Parse concatenated VetStat SOAP XML responses into JSON Lines format."""
    try:
        # Read the input file in text mode to handle XML chunks
        with open(input_file, "r", encoding="utf-8") as f:
            xml_content = f.read()

        # Split content into individual SOAP responses if multiple exist
        # Using a standard separator that should have been added during data collection
        xml_chunks = xml_content.split("\n<!-- RAW_RESPONSE_SEPARATOR -->\n")

        # Process each chunk and collect all data
        all_data = []
        for i, chunk in enumerate(xml_chunks, 1):
            if chunk.strip():  # Skip empty chunks
                chunk_data = extract_data_from_xml_chunk(chunk)
                all_data.extend(chunk_data)
                if chunk_data:
                    logging.info(f"Successfully parsed chunk {i} with {len(chunk_data)} records")
                # else: # DEBUG: Removed the generic 'No data extracted' log here, handled in extract_data_from_xml_chunk
                # logging.warning(f"No data extracted from chunk {i}") # DEBUG: Commented out

        # Write the collected data to JSON Lines format
        with open(output_file, "w", encoding="utf-8") as f:
            for record in all_data:
                json.dump(record, f, ensure_ascii=False)
                f.write("\n")

        logging.info(f"Successfully processed {len(xml_chunks)} XML chunks into {len(all_data)} records")
        return True

    except Exception as e:
        logging.error(f"Failed to process VetStat XML file: {e}", exc_info=True)
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse concatenated VetStat SOAP XML responses into JSON Lines.")
    parser.add_argument("input_file", type=Path, help="Path to the input vetstat_antibiotics.xml file.")
    parser.add_argument("output_file", type=Path, help="Path to the output JSON Lines file.")

    args = parser.parse_args()

    parse_vetstat_xml(args.input_file, args.output_file)
