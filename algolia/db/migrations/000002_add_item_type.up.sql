BEGIN;

ALTER TABLE algolia_sync
	ADD COLUMN algolia_sync_item_type TEXT DEFAULT '';

CREATE INDEX IF NOT EXISTS algolia_sync_item_type_idx 
	ON algolia_sync(algolia_sync_item_type);


COMMIT;