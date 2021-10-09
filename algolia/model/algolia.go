package model

// AlgoliaConfig contains the config information for the Algolia account to use
type AlgoliaConfig struct {
	App           string
	Key           string
	Index         string
	NumGoRoutines int
}

// AlgoliaObject an Algolia object
type AlgoliaObject struct {
	ObjectID string `json:"objectID"`
}
