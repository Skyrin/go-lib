BEGIN;

-- Adding migration status 
DO $$ BEGIN
	CREATE TYPE arc_migration_status AS ENUM (
		'pending', 'failed', 'complete'
	);
	EXCEPTION
	WHEN duplicate_object THEN null;
END$$;

-- Defines the arc migrations table
CREATE TABLE IF NOT EXISTS arc_migration (
	arc_migration_id BIGSERIAL PRIMARY KEY NOT NULL,
	arc_migration_code TEXT NOT NULL, -- indicates which application this is for ('arc' is reserved for go-lib migrations)
	arc_migration_version BIGINT NOT NULL,
	arc_migration_status arc_migration_status NOT NULL,
	arc_migration_sql TEXT NOT NULL,
	arc_migration_err TEXT NOT NULL,
	created_on TIMESTAMP NOT NULL,
	updated_on TIMESTAMP NOT NULL,
	CONSTRAINT arc_migration_code_version__ukey UNIQUE (arc_migration_code, arc_migration_version)
);

COMMIT;