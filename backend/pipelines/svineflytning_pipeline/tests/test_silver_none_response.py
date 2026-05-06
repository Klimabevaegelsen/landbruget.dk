"""Regression test: silver _transform_movements must tolerate None Response chunks.

Triggered by GitHub Actions run 25418075728 where the SOAP service returned a
chunk with `response["Response"] = None`, crashing the silver stage.
"""

from silver.transform import SvineflytningSilverProcessor


def test_transform_movements_skips_chunks_with_none_response(sample_bronze_chunk):
    none_response_chunk = {
        "timestamp": "2026-03-01T00:00:00",
        "start_date": "2026-03-01",
        "end_date": "2026-03-03",
        "response": {"GLRCHRWSInfoOutbound": {}, "Response": None},
    }
    raw_data = [none_response_chunk, sample_bronze_chunk]
    expected = len(
        sample_bronze_chunk["response"]["Response"]["SvineflytningListe"]["Svineflytning"]
    )

    processor = SvineflytningSilverProcessor(output_dir="/tmp/svineflytning_test")
    try:
        count = processor._transform_movements(raw_data, "20260301_000000")
    finally:
        if processor.conn is not None:
            processor.conn.close()

    assert count == expected
