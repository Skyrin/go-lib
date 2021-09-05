BEGIN;

-- Notifies the specified channel that the specified deployment code, in the
-- arc_deployment table has been either inserted or updated. An associated
-- trigger will be created to call this.
CREATE OR REPLACE FUNCTION arc_deployment_notify()
RETURNS trigger AS $$
DECLARE
BEGIN
	PERFORM pg_notify('arc_deployment_notify', NEW.arc_deployment_code);
	-- This is assumed to be an 'after' trigger, so the result is ignored
	RETURN NULL;
END;
$$ LANGUAGE plpgsql
STRICT IMMUTABLE;


-- This defines configured arc deployments
CREATE TABLE IF NOT EXISTS arc_deployment (
	arc_deployment_id BIGSERIAL PRIMARY KEY NOT NULL,
	arc_deployment_code TEXT NOT NULL, -- The unique deployment code
	arc_deployment_manage_url TEXT NOT NULL,
	arc_deployment_api_url TEXT NOT NULL,
	arc_deployment_name TEXT NOT NULL,
	-- The client id to use for oauth2
	arc_deployment_oauth2_client_id TEXT NOT NULL,
	-- The client secret to use for oauth2
	arc_deployment_oauth2_client_secret TEXT NOT NULL,
	-- The token being used for oauth2 calls (retrieved via client credentials)
	-- This is automatically retrieved/refreshed by the code
	arc_deployment_token TEXT NOT NULL,
	-- The token expiry, used to tell the code when to refresh the token
	arc_deployment_token_expiry INT NOT NULL,
	-- The refresh token, used to refresh the access token
	arc_deployment_refresh_token TEXT NOT NULL,
	-- The refresh token expiry, if expired, will use the client credentials to
	-- refresh the token and/or refresh token. This will also be continuously
	-- refreshed if being used (so, technically should not need to use the
	-- client credentials again)
	arc_deployment_refresh_token_expiry INT NOT NULL,
	-- Used to log alerts, specifies the deployment's arcsignal event code and
	-- the publish key required to call it
	arc_deployment_log_event_code TEXT NOT NULL,
	arc_deployment_log_publish_key TEXT NOT NULL,
	updated_on TIMESTAMP NOT NULL,
	CONSTRAINT arc_deployment_code__ukey UNIQUE (arc_deployment_code)
);


-- Trigger an arc_deployment notify event after insert or update
DROP TRIGGER IF EXISTS arc_deployment_notify ON arc_deployment;
CREATE TRIGGER arc_deployment_notify
	AFTER INSERT OR UPDATE ON arc_deployment
	FOR EACH ROW
	EXECUTE PROCEDURE arc_deployment_notify();


-- Stores configured oauth2 arc credentials. Note, a deployments credentials
-- are stored in the arc_deployment table and solely used for API calls on
-- behalf of this app. These client credentials are used for user logins
-- (i.e. cart customer and arcimedes users, possibly core if there is a use
-- case). It is up to the application to configure how to use these credentials.
CREATE TABLE IF NOT EXISTS arc_credential (
	arc_credential_id BIGSERIAL PRIMARY KEY NOT NULL,
	arc_deployment_id BIGINT NOT NULL,
	arc_credential_name TEXT NOT NULL,
	arc_credential_client_id TEXT NOT NULL,
	arc_credential_client_secret TEXT NOT NULL,
	CONSTRAINT arc_deployment_id__fkey FOREIGN KEY (arc_deployment_id)
		REFERENCES arc_deployment(arc_deployment_id)
		ON DELETE CASCADE
);


-- This stores grants for users retrieved by this application to access
-- an arc deployment. When a user logs into this application, a grant will
-- be given to that user and stored into this table. It will also generate
-- a session (cookie) for the UI. The user will be able to refresh their
-- token as long as the session has not expired. The session will automatically
-- refresh as long as the user is interacting (making API calls to this
-- application).
CREATE TABLE IF NOT EXISTS arc_deployment_grant (
	arc_deployment_grant_id BIGSERIAL PRIMARY KEY NOT NULL,
	arc_deployment_id BIGINT NOT NULL,
	-- This represents the user id from the deployment that this grant was issued 
	arc_user_id BIGINT NOT NULL,
	-- Reference to client credentials used for the grant
	arc_credential_id BIGINT NOT NULL,
	arc_deployment_grant_token TEXT NOT NULL,
	arc_deployment_grant_token_expiry INT,
	arc_deployment_grant_token_hash TEXT NOT NULL,
	arc_deployment_grant_refresh_token TEXT,
	arc_deployment_grant_refresh_token_expiry INT,
	CONSTRAINT arc_deployment_grant_token_hash__ukey
		UNIQUE (arc_deployment_grant_token_hash),
	CONSTRAINT arc_deployment_id__fkey FOREIGN KEY (arc_deployment_id)
		REFERENCES arc_deployment(arc_deployment_id)
		ON DELETE CASCADE,
	CONSTRAINT arc_credential_id__fkey FOREIGN KEY (arc_credential_id)
		REFERENCES arc_credential(arc_credential_id)
		ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS arc_deployment_grant_refresh_token_expiry__key
	ON arc_deployment_grant(arc_deployment_grant_refresh_token_expiry);

COMMIT;