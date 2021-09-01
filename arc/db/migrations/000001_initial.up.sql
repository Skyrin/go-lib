BEGIN;

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
	-- This represents the application session, which the user/UI will have
	-- a cookie set whenever they login. Or use the token? and force the UI
	-- to refresh before it expires?
	arc_deployment_grant_session TEXT NULL,
	arc_deployment_grant_session_expiry INT NOT NULL,
	arc_deployment_grant_token TEXT NOT NULL,
	arc_deployment_grant_token_expiry INT,
	arc_deployment_grant_refresh_token TEXT,
	arc_deployment_grant_refresh_token_expiry INT,
	arc_deployment_grant_client_id TEXT NOT NULL,
	arc_deployment_grant_client_secret TEXT NOT NULL,
	CONSTRAINT arc_deployment_grant_session__ukey UNIQUE (arc_deployment_grant_session),
	CONSTRAINT arc_deployment_id__fkey FOREIGN KEY (arc_deployment_id)
		REFERENCES arc_deployment(arc_deployment_id)
		ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS arc_deployment_grant_session_expiry__key
	ON arc_deployment_grant(arc_deployment_grant_session_expiry);


-- This defines configured arc deployment store(s). It stores the oauth2
-- client id and secret (secret not currently used) so that this application
-- can login (get access tokens) on behalf of users
CREATE TABLE IF NOT EXISTS arc_deployment_store (
	arc_deployment_id BIGINT NOT NULL,
	-- The unique store code for this deployment
	arc_deployment_store_code TEXT NOT NULL,
	-- The client id for the store. This is required in order to
	-- use ouath2.Grant.login with a store customer.
	arc_deployment_store_client_id TEXT NOT NULL,
	-- The client secret for the store (not currently used)
	arc_deployment_store_client_secret TEXT NOT NULL,
	PRIMARY KEY (arc_deployment_id, arc_deployment_store_code),
	CONSTRAINT arc_deployment_id__fkey FOREIGN KEY (arc_deployment_id)
		REFERENCES arc_deployment(arc_deployment_id)
		ON DELETE CASCADE
);


COMMIT;