BEGIN;

-- Adding algolia sync status 
DO $$ BEGIN
	CREATE TYPE algolia_sync_status AS ENUM (
		'pending', 'failed', 'complete'
	);
	EXCEPTION
	WHEN duplicate_object THEN null;
END$$;

-- Defines the algolia sync table
CREATE TABLE IF NOT EXISTS algolia_sync (
	algolia_sync_id BIGSERIAL PRIMARY KEY NOT NULL,
	algolia_sync_index TEXT NOT NULL,
	algolia_sync_object_id TEXT NOT NULL, -- This is the object id required by algolia
    algolia_sync_item_id BIGINT NOT NULL, -- The ID for the object being pushed to algolia
	algolia_sync_item TEXT NOT NULL, -- Object to push to algolia
	algolia_sync_item_hash TEXT NOT NULL,	
    algolia_sync_status algolia_sync_status NOT NULL DEFAULT 'pending',
	algolia_sync_item_delete bool DEFAULT FALSE,
	created_on TIMESTAMP NOT NULL,
	updated_on TIMESTAMP NOT NULL,
	CONSTRAINT algolia_sync_index__object_id__ukey UNIQUE (algolia_sync_index, algolia_sync_object_id)
);

COMMIT;