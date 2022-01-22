BEGIN;

-- TODO: Remove this and just use string?
DO $$ BEGIN
	CREATE TYPE arc_data_status AS ENUM (
		'pending', 'processing', 'processed'
	);
	EXCEPTION
	WHEN duplicate_object THEN null;
END$$;

DO $$ BEGIN
	CREATE TYPE arc_app_code AS ENUM (
		'arcimedes', 'cart', 'core'
	);
	EXCEPTION
	WHEN duplicate_object THEN null;
END$$;

-- TODO: Remove this and just use string?
DO $$ BEGIN
	CREATE TYPE arc_data_type AS ENUM (
		'category', 'customer', 'order', 'order-lease', 'product', 'purchase',
		'rental-asset', 'user'
	);
	EXCEPTION
	WHEN duplicate_object THEN null;
END$$;

-- This keeps track of data that originated from an arc deployment
CREATE TABLE IF NOT EXISTS arc_data (
	-- The associated app code: i.e. core, cart or arcimedes
	arc_app_code arc_app_code NOT NULL,
	-- The associated app core id: i.e. the cart store id or arcimedes table id
	-- (currently not used for core and would always be 0 for it)
	arc_app_core_id BIGINT NOT NULL,
	-- The data sync type: i.e. category, customer, product, rental asset, etc
	arc_data_type arc_data_type NOT NULL,
	-- The underlying arc object id: i.e. id of category/customer/product/etc
	arc_data_object_id BIGINT NOT NULL,
	-- The current status of this record
	arc_data_status arc_data_status NOT NULL,
	-- The JSON representation of the data record
	arc_data_object JSONB NOT NULL,
	-- The hash of the JSON - used to check if any changes from previous update
	arc_data_hash BYTEA NOT NULL,
	-- Indicates if this record is deleted (soft delete here as the app needs
	-- a reference to it in order to delete the app specific/related tables
	-- assocaited with it)
	arc_data_deleted BOOLEAN NOT NULL,
	created_on TIMESTAMP NOT NULL,
	updated_on TIMESTAMP NOT NULL,
	CONSTRAINT arc_data__pkey UNIQUE
		(arc_app_code,arc_app_core_id,arc_data_type,arc_data_object_id)
);

COMMIT;