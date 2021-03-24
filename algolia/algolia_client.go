// Package algolia provides the necessary calls to communicate with algolia (push, delete)

package algolia

import (
	"fmt"

	"github.com/Skyrin/go-lib/errors"
	"github.com/algolia/algoliasearch-client-go/v3/algolia/search"
)

// Algolia handler for pushing index updates to Algolia
type Algolia struct {
	Client *search.Client
	Index  *search.Index
}

// AlgoliaConfig contains the config information for the Algolia account to use
type AlgoliaConfig struct {
	App       string
	Key       string
	Index     string
}

// AlgoliaObject an Algolia object
type AlgoliaObject struct {
	ObjectID string `json:"objectID"`
}

// NewAlgolia initialize a new algolia client/index
func NewAlgolia(config *AlgoliaConfig) (alg *Algolia, err error) {
	// Validate all required environment configurations are specified
	if config.App == "" {
		return nil, fmt.Errorf("Algolia App not specified")
	}

	if config.Key == "" {
		return nil, fmt.Errorf("Algolia Key not specified")
	}

	if config.Index == "" {
		return nil, fmt.Errorf("Algolia Index not specified")
	}

	// Initialize the Algolia client
	alg = &Algolia{}
	alg.Client = search.NewClient(config.App, config.Key)
	alg.Index = alg.Client.InitIndex(config.Index)

	return alg, nil
}

// Push saves an item to the specified Algolia Index
func (alg *Algolia) Push(item interface{}) (err error) {
	_, err = alg.Index.SaveObject(item)
	if err != nil {
		return errors.Wrap(err, "Algolia.Push.1", "")
	}

	return nil
}

// Delete deletes an item from the specified Algolia Index
func (alg *Algolia) Delete(objectID string) (err error) {
	_, err = alg.Index.DeleteObject(objectID)
	if err != nil {
		return errors.Wrap(err, "Algolia.Delete.1", "")
	}

	return nil
}
