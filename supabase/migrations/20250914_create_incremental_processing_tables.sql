-- CHR Incremental Processing Tables
-- Based on CHR_INCREMENTAL_PROCESSING_PLAN.md

-- 1. Pipeline Processing History Table
-- Tracks all pipeline runs and their processing status
CREATE TABLE pipeline_processing_history (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL, -- 'chr', 'svineflytning', 'international_transport'
    processing_mode VARCHAR(50) NOT NULL, -- 'full', 'incremental', 'backfill'
    bronze_timestamp VARCHAR(50) NOT NULL, -- GCS timestamp directory
    processing_start TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processing_end TIMESTAMPTZ,
    status VARCHAR(50) NOT NULL DEFAULT 'running', -- 'running', 'completed', 'failed', 'cancelled'

    -- Data coverage information
    data_period_start DATE, -- First date covered by this processing run
    data_period_end DATE,   -- Last date covered by this processing run
    months_processed INTEGER, -- Number of months processed in this run

    -- Processing details
    bronze_path TEXT NOT NULL, -- Full GCS path to bronze data
    silver_path TEXT, -- Full GCS path to silver output
    gold_path TEXT,   -- Full GCS path to gold output (if applicable)

    -- Performance metrics
    records_processed BIGINT DEFAULT 0,
    processing_duration_seconds INTEGER,
    memory_peak_mb INTEGER,

    -- Error handling
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,

    -- Metadata
    workflow_run_id VARCHAR(100), -- GitHub Actions workflow run ID
    triggered_by VARCHAR(100), -- 'schedule', 'manual', 'api', 'dependency'
    configuration JSONB, -- Processing configuration used

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for efficient querying
CREATE INDEX idx_pipeline_processing_history_pipeline_name ON pipeline_processing_history(pipeline_name);
CREATE INDEX idx_pipeline_processing_history_status ON pipeline_processing_history(status);
CREATE INDEX idx_pipeline_processing_history_bronze_timestamp ON pipeline_processing_history(bronze_timestamp);
CREATE INDEX idx_pipeline_processing_history_processing_start ON pipeline_processing_history(processing_start);
CREATE INDEX idx_pipeline_processing_history_data_period ON pipeline_processing_history(data_period_start, data_period_end);

-- 2. CHR Data Freshness Tracking Table
-- Tracks the freshness and completeness of CHR data
CREATE TABLE chr_data_freshness (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    bronze_timestamp VARCHAR(50) NOT NULL UNIQUE, -- GCS timestamp directory

    -- Data availability flags
    has_main_chr_data BOOLEAN DEFAULT FALSE,
    has_movement_data BOOLEAN DEFAULT FALSE,
    has_spf_su_data BOOLEAN DEFAULT FALSE,
    has_vetstat_xml BOOLEAN DEFAULT FALSE,
    has_vetstat_json BOOLEAN DEFAULT FALSE,
    has_vet_events BOOLEAN DEFAULT FALSE,

    -- Data coverage
    chr_records_count BIGINT DEFAULT 0,
    movement_records_count BIGINT DEFAULT 0,
    vetstat_records_count BIGINT DEFAULT 0,

    -- Date range covered by this bronze run
    data_coverage_start DATE,
    data_coverage_end DATE,
    months_covered INTEGER DEFAULT 0,

    -- Processing status
    bronze_processing_status VARCHAR(50) DEFAULT 'pending', -- 'pending', 'completed', 'failed'
    silver_processing_status VARCHAR(50) DEFAULT 'pending',
    gold_processing_status VARCHAR(50) DEFAULT 'pending',

    -- Data quality metrics
    data_quality_score DECIMAL(5,2), -- 0.00 to 100.00
    completeness_percentage DECIMAL(5,2), -- 0.00 to 100.00

    -- File sizes for monitoring
    bronze_total_size_mb DECIMAL(10,2),
    silver_total_size_mb DECIMAL(10,2),
    gold_total_size_mb DECIMAL(10,2),

    -- Timestamps
    discovered_at TIMESTAMPTZ DEFAULT NOW(),
    last_processed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for chr_data_freshness
CREATE INDEX idx_chr_data_freshness_bronze_timestamp ON chr_data_freshness(bronze_timestamp);
CREATE INDEX idx_chr_data_freshness_coverage ON chr_data_freshness(data_coverage_start, data_coverage_end);
CREATE INDEX idx_chr_data_freshness_status ON chr_data_freshness(bronze_processing_status, silver_processing_status);
CREATE INDEX idx_chr_data_freshness_discovered ON chr_data_freshness(discovered_at);

-- 3. Incremental Processing State Table
-- Tracks the current state for incremental processing decisions
CREATE TABLE incremental_processing_state (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL UNIQUE, -- 'chr', 'svineflytning', 'international_transport'

    -- Last successful processing information
    last_full_processing_timestamp VARCHAR(50), -- Last full/backfill processing bronze timestamp
    last_full_processing_date TIMESTAMPTZ,
    last_incremental_processing_timestamp VARCHAR(50), -- Last incremental processing bronze timestamp
    last_incremental_processing_date TIMESTAMPTZ,

    -- Data freshness tracking
    oldest_unprocessed_data_date DATE, -- Oldest date that hasn't been processed incrementally
    newest_available_data_date DATE,   -- Newest date available in bronze layer

    -- Processing configuration
    incremental_processing_enabled BOOLEAN DEFAULT TRUE,
    max_incremental_months INTEGER DEFAULT 3, -- Maximum months to process incrementally
    backfill_threshold_months INTEGER DEFAULT 6, -- Force backfill if gap exceeds this

    -- Performance tracking
    avg_incremental_processing_minutes DECIMAL(8,2),
    avg_full_processing_minutes DECIMAL(8,2),

    -- Next processing recommendation
    next_recommended_mode VARCHAR(50), -- 'incremental', 'backfill', 'full'
    next_recommended_timestamp VARCHAR(50), -- Recommended bronze timestamp to process
    recommendation_reason TEXT,

    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for incremental_processing_state
CREATE INDEX idx_incremental_processing_state_pipeline ON incremental_processing_state(pipeline_name);
CREATE INDEX idx_incremental_processing_state_enabled ON incremental_processing_state(incremental_processing_enabled);

-- 4. Data Dependencies Tracking Table
-- Tracks dependencies between different pipeline outputs
CREATE TABLE pipeline_data_dependencies (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    source_pipeline VARCHAR(100) NOT NULL, -- Pipeline that produces the data
    source_bronze_timestamp VARCHAR(50) NOT NULL,
    dependent_pipeline VARCHAR(100) NOT NULL, -- Pipeline that depends on the data
    dependency_type VARCHAR(50) NOT NULL, -- 'required', 'optional', 'enhancement'

    -- Processing coordination
    source_processing_completed BOOLEAN DEFAULT FALSE,
    dependent_processing_started BOOLEAN DEFAULT FALSE,
    dependent_processing_completed BOOLEAN DEFAULT FALSE,

    -- Timing
    source_completion_time TIMESTAMPTZ,
    dependent_start_time TIMESTAMPTZ,
    dependent_completion_time TIMESTAMPTZ,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for pipeline_data_dependencies
CREATE INDEX idx_pipeline_dependencies_source ON pipeline_data_dependencies(source_pipeline, source_bronze_timestamp);
CREATE INDEX idx_pipeline_dependencies_dependent ON pipeline_data_dependencies(dependent_pipeline);
CREATE INDEX idx_pipeline_dependencies_completion ON pipeline_data_dependencies(source_processing_completed, dependent_processing_started);

-- 5. Initialize default state for existing pipelines
INSERT INTO incremental_processing_state (pipeline_name, incremental_processing_enabled, max_incremental_months, backfill_threshold_months)
VALUES
    ('chr', TRUE, 3, 6),
    ('svineflytning', TRUE, 2, 4),
    ('international_transport', TRUE, 2, 4)
ON CONFLICT (pipeline_name) DO NOTHING;

-- 6. Create updated_at trigger function if it doesn't exist
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply updated_at triggers to all tables
CREATE TRIGGER update_pipeline_processing_history_updated_at BEFORE UPDATE ON pipeline_processing_history FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_chr_data_freshness_updated_at BEFORE UPDATE ON chr_data_freshness FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_incremental_processing_state_updated_at BEFORE UPDATE ON incremental_processing_state FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_pipeline_data_dependencies_updated_at BEFORE UPDATE ON pipeline_data_dependencies FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- 7. Add helpful views for monitoring and decision making

-- View: Current Pipeline Status
CREATE VIEW current_pipeline_status AS
SELECT
    ips.pipeline_name,
    ips.incremental_processing_enabled,
    ips.last_full_processing_date,
    ips.last_incremental_processing_date,
    ips.next_recommended_mode,
    ips.recommendation_reason,

    -- Latest processing run info
    pph.bronze_timestamp as latest_bronze_timestamp,
    pph.status as latest_processing_status,
    pph.processing_end as latest_processing_end,
    pph.records_processed as latest_records_processed,

    -- Data freshness (for CHR)
    cdf.months_covered as chr_months_covered,
    cdf.data_coverage_start as chr_coverage_start,
    cdf.data_coverage_end as chr_coverage_end,
    cdf.has_vetstat_json as chr_has_vetstat

FROM incremental_processing_state ips
LEFT JOIN pipeline_processing_history pph ON (
    pph.pipeline_name = ips.pipeline_name
    AND pph.id = (
        SELECT id FROM pipeline_processing_history
        WHERE pipeline_name = ips.pipeline_name
        ORDER BY processing_start DESC
        LIMIT 1
    )
)
LEFT JOIN chr_data_freshness cdf ON (
    cdf.bronze_timestamp = pph.bronze_timestamp
    AND ips.pipeline_name = 'chr'
);

-- View: Processing Performance Metrics
CREATE VIEW pipeline_performance_metrics AS
SELECT
    pipeline_name,
    processing_mode,
    COUNT(*) as total_runs,
    AVG(processing_duration_seconds) as avg_duration_seconds,
    AVG(records_processed) as avg_records_processed,
    AVG(memory_peak_mb) as avg_memory_mb,
    MAX(processing_end) as last_run_date,
    COUNT(CASE WHEN status = 'completed' THEN 1 END) as successful_runs,
    COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_runs
FROM pipeline_processing_history
WHERE processing_end IS NOT NULL
GROUP BY pipeline_name, processing_mode
ORDER BY pipeline_name, processing_mode;

-- Comments for documentation
COMMENT ON TABLE pipeline_processing_history IS 'Tracks all pipeline runs with detailed processing information and performance metrics';
COMMENT ON TABLE chr_data_freshness IS 'Tracks CHR data availability, coverage, and quality metrics for each bronze timestamp';
COMMENT ON TABLE incremental_processing_state IS 'Maintains current state and configuration for incremental processing decisions';
COMMENT ON TABLE pipeline_data_dependencies IS 'Tracks dependencies between pipeline outputs for coordination';

COMMENT ON VIEW current_pipeline_status IS 'Current status overview of all pipelines including latest runs and recommendations';
COMMENT ON VIEW pipeline_performance_metrics IS 'Performance metrics aggregated by pipeline and processing mode';
