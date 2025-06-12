BEGIN;

-- Add columns to track total successful runs and average run time
ALTER TABLE process
	ADD COLUMN IF NOT EXISTS process_total_success INT NULL,
	ADD COLUMN IF NOT EXISTS process_avg_run_time INTERVAL SECOND NOT NULL DEFAULT MAKE_INTERVAL(secs => 0);

COMMIT;