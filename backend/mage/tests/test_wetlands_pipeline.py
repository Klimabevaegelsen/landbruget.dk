import pytest
from mage_ai.data_preparation.repo_manager import get_repo_path
from mage_ai.orchestration.pipeline_scheduler import PipelineScheduler
from mage_ai.data_preparation.models.pipeline import Pipeline

def test_pipeline_loads():
    """Test that the wetlands pipeline can be loaded."""
    pipeline = Pipeline("wetlands_pipeline")
    assert pipeline is not None
    assert pipeline.uuid == "wetlands_pipeline"

def test_pipeline_blocks():
    """Test that all required blocks are present."""
    pipeline = Pipeline("wetlands_pipeline")
    block_uuids = [block.uuid for block in pipeline.blocks]
    
    required_blocks = [
        "wetlands_load_wfs",
        "wetlands_process_batch",
        "wetlands_combine_batches",
        "wetlands_store_raw",
        "wetlands_format_geojson",
        "wetlands_merge_grid"
    ]
    
    for block in required_blocks:
        assert block in block_uuids

def test_pipeline_configuration():
    """Test that pipeline configuration is valid."""
    pipeline = Pipeline("wetlands_pipeline")
    config = pipeline.metadata.get('configuration', {})
    
    assert 'data_source' in config
    assert 'batch_size' in config
    assert isinstance(config['batch_size'], int)

def test_pipeline_scheduling():
    """Test that pipeline can be scheduled."""
    scheduler = PipelineScheduler('wetlands_pipeline')
    assert scheduler.pipeline is not None 