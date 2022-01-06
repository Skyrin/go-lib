BEGIN;

-- Adding sync status 
DO $$ BEGIN
	CREATE TYPE sync_status AS ENUM (
		'pending', 'failed', 'complete'
	);
	EXCEPTION
	WHEN duplicate_object THEN null;
END$$;

-- Defines the sync_queue table
CREATE TABLE IF NOT EXISTS sync_queue (
	sync_queue_id BIGSERIAL PRIMARY KEY NOT NULL,    	
    sync_queue_status sync_status NOT NULL DEFAULT 'pending',
	sync_queue_delete bool DEFAULT FALSE,
	sync_queue_item_id BIGINT NOT NULL,
	sync_queue_item jsonb,
	sync_queue_item_hash TEXT NOT NULL,	
    sync_queue_retries int NOT NULL DEFAULT 0,
    sync_queue_service TEXT NOT NULL,
	sync_queue_error TEXT NOT NULL DEFAULT '',
	sync_queue_item_type TEXT NOT NULL,
	created_on TIMESTAMP NOT NULL,
	updated_on TIMESTAMP NOT NULL,
	CONSTRAINT sync_queue_service__item_type__item_id__ukey 
		UNIQUE (sync_queue_service, sync_queue_item_type, sync_queue_item_id)
);

COMMIT;