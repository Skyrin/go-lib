BEGIN;

-- Adding migration status 
DO $$ BEGIN
	CREATE TYPE skyrin_migration_status AS ENUM (
		'pending', 'failed', 'complete'
	);
	EXCEPTION
	WHEN duplicate_object THEN null;
END$$;

-- Defines the arc migrations table
CREATE TABLE IF NOT EXISTS skyrin_migration (
	skyrin_migration_id BIGSERIAL PRIMARY KEY NOT NULL,
	skyrin_migration_code TEXT NOT NULL, -- indicates which application this is for ('arc' is reserved for go-lib migrations)
	skyrin_migration_version BIGINT NOT NULL,
	skyrin_migration_status skyrin_migration_status NOT NULL,
	skyrin_migration_sql TEXT NOT NULL,
	skyrin_migration_err TEXT NOT NULL,
	created_on TIMESTAMP NOT NULL,
	updated_on TIMESTAMP NOT NULL,
	CONSTRAINT skyrin_migration_code_version__ukey UNIQUE (skyrin_migration_code, skyrin_migration_version)
);

COMMIT;