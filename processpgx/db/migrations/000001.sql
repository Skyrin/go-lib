BEGIN;

DO $$ BEGIN
	CREATE TYPE process_status AS ENUM (
		'ready', 'running'
	);
	EXCEPTION
	WHEN duplicate_object THEN null;
END$$;

DO $$ BEGIN
	CREATE TYPE process_run_status AS ENUM (
		'running', 'completed', 'failed'
	);
	EXCEPTION
	WHEN duplicate_object THEN null;
END$$;

-- Used as a "lock" so only one process will run at a time
CREATE TABLE IF NOT EXISTS process (
	process_id BIGSERIAL PRIMARY KEY NOT NULL,
	process_code TEXT NOT NULL,
	process_name TEXT NOT NULL,
	process_status process_status NOT NULL,
	process_message TEXT NOT NULL,
	created_on TIMESTAMP NOT NULL,
	updated_on TIMESTAMP NOT NULL,
	CONSTRAINT process__ukey UNIQUE (process_code)
);

-- Used to track history of process runs
CREATE TABLE IF NOT EXISTS process_run (
	process_run_id BIGSERIAL PRIMARY KEY NOT NULL,
	process_id BIGINT NOT NULL REFERENCES process (process_id),
	process_run_status process_run_status NOT NULL,
	process_run_error TEXT NOT NULL,
	created_on TIMESTAMP NOT NULL,
	updated_on TIMESTAMP NOT NULL
);

COMMIT;