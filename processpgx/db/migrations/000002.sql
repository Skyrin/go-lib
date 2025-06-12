BEGIN;

DO $$ BEGIN
	ALTER TYPE process_status RENAME TO t_process_status;
		EXCEPTION
		WHEN undefined_object THEN null;
END$$;

DO $$ BEGIN
	UPDATE process
		SET process_status='ready'::t_process_status;
	EXCEPTION
	WHEN invalid_text_representation THEN null;
END$$;

DO $$ BEGIN
	ALTER TYPE t_process_status
		RENAME VALUE 'ready' TO 'active';
	EXCEPTION
	WHEN invalid_parameter_value THEN null;
END$$;

DO $$ BEGIN
	ALTER TYPE t_process_status
		RENAME VALUE 'running' TO 'inactive';
	EXCEPTION
	WHEN invalid_parameter_value THEN null;
END$$;

COMMIT;