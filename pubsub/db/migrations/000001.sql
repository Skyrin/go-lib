BEGIN;

-- When data is published, all associated subscribers will be notified via this channel
CREATE OR REPLACE FUNCTION skyrin_dps_notify()
RETURNS trigger AS $$
DECLARE
BEGIN
	PERFORM pg_notify('skyrin_dps_notify', JSON_BUILD_OBJECT(
		'pubId',NEW.dps_pub_id,
		'dataType', NEW.dps_data_type,
		'dataId', NEW.dps_data_id,
		'deleted', NEW.dps_data_deleted,
		'version', NEW.dps_data_version,
		'json', NEW.dps_data_json
	)::TEXT);
	-- This is assumed to be an 'after' trigger, so the result is ignored
	RETURN NULL;
END;
$$ LANGUAGE plpgsql
STRICT IMMUTABLE;


-- Adding publisher status
DO $$ BEGIN
	CREATE TYPE t_skyrin_dps_pub_status AS ENUM (
		'active', 'inactive'
	);
	EXCEPTION
	WHEN duplicate_object THEN NULL;
END$$;

-- Adding subscriber status 
DO $$ BEGIN
	CREATE TYPE t_skyrin_dps_sub_status AS ENUM (
		'active', 'inactive'
	);
	EXCEPTION
	WHEN duplicate_object THEN NULL;
END$$;

-- Adding subscriber data status
DO $$ BEGIN
	CREATE TYPE t_skyrin_dps_sub_data_status AS ENUM (
		'pending', 'failed', 'completed'
	);
	EXCEPTION
	WHEN duplicate_object THEN NULL;
END$$;


-- The publisher table
CREATE TABLE IF NOT EXISTS skyrin_dps_pub (
	dps_pub_id SERIAL PRIMARY KEY NOT NULL,
	dps_pub_code TEXT NOT NULL,
	dps_pub_name TEXT NOT NULL,
	dps_pub_status t_skyrin_dps_pub_status NOT NULL DEFAULT 'active',
	created_on TIMESTAMP NOT NULL DEFAULT NOW(),
	updated_on TIMESTAMP NOT NULL DEFAULT NOW(),
	CONSTRAINT dps_pub__code__ukey 
		UNIQUE (dps_pub_code)
);


-- The subscriber table
CREATE TABLE IF NOT EXISTS skyrin_dps_sub (
	dps_sub_id SERIAL PRIMARY KEY NOT NULL,
	dps_sub_code TEXT NOT NULL,
	dps_sub_name TEXT NOT NULL,
	dps_sub_status t_skyrin_dps_sub_status NOT NULL DEFAULT 'active',
	dps_sub_retries INT NOT NULL DEFAULT 0, -- maximum number of retries before setting to failed
	created_on TIMESTAMP NOT NULL DEFAULT NOW(),
	updated_on TIMESTAMP NOT NULL DEFAULT NOW(),
	CONSTRAINT dps_sub__code__ukey 
		UNIQUE (dps_sub_code)
);


-- Table mapping subscribers to publishers
CREATE TABLE IF NOT EXISTS skyrin_dps_pub_sub_map (
	dps_pub_id INT NOT NULL,
	dps_sub_id INT NOT NULL,
	PRIMARY KEY (dps_pub_id, dps_sub_id),
	CONSTRAINT skyrin_dps_pub_sub_map__pub_id__fkey
		FOREIGN KEY (dps_pub_id)
		REFERENCES skyrin_dps_pub(dps_pub_id)
		ON DELETE CASCADE,
	CONSTRAINT skyrin_dps_pub_sub_map__sub_id__fkey
		FOREIGN KEY (dps_sub_id)
		REFERENCES skyrin_dps_sub(dps_sub_id)
		ON DELETE CASCADE
);


-- Defines publisher data table - storing the latest version and whether the record is marked as deleted
CREATE TABLE IF NOT EXISTS skyrin_dps_data (
	dps_pub_id INT NOT NULL,
	dps_data_type TEXT NOT NULL,
	dps_data_id TEXT NOT NULL,
	dps_data_deleted bool DEFAULT FALSE,
	dps_data_version INT NOT NULL,
	dps_data_json JSONB NULL, -- optional JSON representation of the published data
	created_on TIMESTAMP NOT NULL DEFAULT NOW(),
	updated_on TIMESTAMP NOT NULL DEFAULT NOW(),
	PRIMARY KEY (dps_pub_id, dps_data_type, dps_data_id),
	CONSTRAINT skyrin_dps_data__pub_id__fkey
		FOREIGN KEY (dps_pub_id)
		REFERENCES skyrin_dps_pub(dps_pub_id)
		ON DELETE CASCADE
);

-- Trigger skyrin_dps_data_notify after insert or update
DROP TRIGGER IF EXISTS skyrin_dps_data_notify ON skyrin_dps_data;
CREATE TRIGGER skyrin_dps_data_notify
	AFTER INSERT OR UPDATE ON skyrin_dps_data
	FOR EACH ROW
	EXECUTE PROCEDURE skyrin_dps_notify();

-- Define data pubsub item table. Tracks status of each item for a sub
CREATE TABLE IF NOT EXISTS skyrin_dps_sub_data (
	dps_sub_data_id BIGSERIAL NOT NULL,
	dps_sub_id INT NOT NULL,
	dps_pub_id INT NOT NULL,
	dps_data_type TEXT NOT NULL,
	dps_data_id TEXT NOT NULL,
	dps_sub_data_deleted BOOL DEFAULT FALSE, -- duplicated here to avoid locking data table when processing sub_data table
	dps_sub_data_version INT NOT NULL, -- stores the last version successfully pushed to this sub
	dps_sub_data_status t_skyrin_dps_sub_data_status NOT NULL DEFAULT 'pending',
	dps_sub_data_hash TEXT NOT NULL, -- hash of last item sent to this sub
	dps_sub_data_json JSONB NULL, -- optional JSON representation of the sent subscriber data
	dps_sub_data_retries INT NOT NULL DEFAULT 0,
	dps_sub_data_message TEXT NOT NULL DEFAULT '',
	created_on TIMESTAMP NOT NULL DEFAULT NOW(),
	updated_on TIMESTAMP NOT NULL DEFAULT NOW(),
	CONSTRAINT skyrin_dps_sub_data__sub_id__pub_id__type__id__ukey
		UNIQUE (dps_sub_id, dps_pub_id, dps_data_type, dps_data_id),
	CONSTRAINT skyrin_dps_sub_data__pub_id__type__id__fkey 
		FOREIGN KEY (dps_pub_id, dps_data_type, dps_data_id)
		REFERENCES skyrin_dps_data(dps_pub_id, dps_data_type, dps_data_id)
		ON DELETE CASCADE
);

-- Index for getting processable sub data records
CREATE INDEX IF NOT EXISTS skyrin_dps_sub_data__processable__key
	ON skyrin_dps_sub_data (dps_sub_id, dps_sub_data_status, dps_sub_data_id)
	WHERE dps_sub_data_status='pending';


COMMIT;
