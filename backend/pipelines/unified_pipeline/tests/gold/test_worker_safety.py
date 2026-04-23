from types import SimpleNamespace
from unittest.mock import Mock

from unified_pipeline.gold.worker_safety import WorkerSafetyGold


def test_get_latest_silver_path_prefers_underscore_dataset():
    worker_safety = WorkerSafetyGold.__new__(WorkerSafetyGold)
    worker_safety.config = SimpleNamespace(bucket="landbruget-data")
    worker_safety.log = Mock()
    worker_safety.storage = Mock()

    worker_safety.storage.list_files.side_effect = [
        ["landbruget-data/silver/worker_safety/20260419_181206/worker_safety_2020-2024_mv.parquet"]
    ]

    result = worker_safety._get_latest_silver_path("worker_safety")

    assert result == "landbruget-data/silver/worker_safety/20260419_181206"


def test_get_latest_silver_path_falls_back_to_legacy_space_dataset():
    worker_safety = WorkerSafetyGold.__new__(WorkerSafetyGold)
    worker_safety.config = SimpleNamespace(bucket="landbruget-data")
    worker_safety.log = Mock()
    worker_safety.storage = Mock()

    worker_safety.storage.list_files.side_effect = [
        [],
        [],
        ["landbruget-data/silver/worker safety/20260116_234019/worker_safety_2020-2024_mv.parquet"],
    ]

    result = worker_safety._get_latest_silver_path("worker_safety")

    assert result == "landbruget-data/silver/worker safety/20260116_234019"
