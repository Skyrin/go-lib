BEGIN;

-- Add columns to track when the process should run next
ALTER TABLE process
	ADD COLUMN IF NOT EXISTS process_last_run_time TIMESTAMP NULL,
	ADD COLUMN IF NOT EXISTS process_next_run_time TIMESTAMP NOT NULL DEFAULT NOW(),
	ADD COLUMN IF NOT EXISTS process_interval INTERVAL SECOND NOT NULL DEFAULT MAKE_INTERVAL(secs => 0);

-- Adding column to track time for each process run
ALTER TABLE process_run
	ADD COLUMN IF NOT EXISTS process_run_time INTERVAL SECOND NOT NULL DEFAULT MAKE_INTERVAL(secs => 0);

COMMIT;