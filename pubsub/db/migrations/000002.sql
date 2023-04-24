BEGIN;

-- When data is published, all associated subscribers will be notified via this channel. The JSON
-- string is not included in the event payload as it may be to large for the trigger. It must be
-- looked up as needed by each listening subscriber.
CREATE OR REPLACE FUNCTION skyrin_dps_notify()
RETURNS trigger AS $$
DECLARE
BEGIN
	PERFORM pg_notify('skyrin_dps_notify', JSON_BUILD_OBJECT(
		'pubId',NEW.dps_pub_id,
		'dataType', NEW.dps_data_type,
		'dataId', NEW.dps_data_id,
		'deleted', NEW.dps_data_deleted,
		'version', NEW.dps_data_version
	)::TEXT);
	-- This is assumed to be an 'after' trigger, so the result is ignored
	RETURN NULL;
END;
$$ LANGUAGE plpgsql
STRICT IMMUTABLE;


COMMIT;
