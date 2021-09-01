
CREATE OR REPLACE FUNCTION arc_deployment_notify()
RETURNS trigger AS $$
DECLARE
BEGIN
	PERFORM pg_notify('arc_deployment_notify', NEW.arc_deployment_code);
	RETURN NULL;
END;
$$ LANGUAGE plpgsql
STRICT IMMUTABLE;

CREATE TRIGGER arc_deployment_notify
	AFTER INSERT OR UPDATE ON arc_deployment
	FOR EACH ROW
	EXECUTE PROCEDURE arc_deployment_notify();
