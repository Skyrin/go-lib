# go-lib/pubsub

This package allows the creation of publishers (pub) and subscribers (sub). Sub can be linked to multiple pub. When a pub receives new/updated data, it can update linked subs either immediately or in batches. It is up to the subs to define what to do with the published data. Published data must have a type and an id specified, which define that pub's unique record. Optionally, a JSON representation of the pub object can also be provided. Deleted pub data records are handled using soft deletes. An internal versioning system tracks the latest version. It is possible for a subscriber to send the same version more than once. So, the sub data handlers should handle this accordingly.


## Usage
Publish data
```
	version, err := pubsub.Publish(db, pubsub.PublishParam{
		PublishID: 1,
		Type:      "data-type",
		ID:        "data-id",
		Deleted:   false,
		JSON:      []byte(`{"type":"data-type","id":"data-id"}`),
	})
```

Mark as deleted
```
	version, err := pubsub.Publish(db, pubsub.PublishParam{
		PublishID: 1,
		Type:      "data-type",
		ID:        "data-id",
		Deleted:   true,
	})
```

Subscriber that both listens for updates and processes all new/updated records once
```
// Define the batch data handler that implements the pubsub.SubBatchHandler
type batchHandler struct {
	s    *pubsub.Subscriber
	time time.Time
}

// Push push the records
func (bh *batchHandler) Push(notifyList []*pubsub.Notify) (err error) {
	// Informational log - not required
	total, success, retry, fail := bh.s.GetStats()
	log.Info().Msgf("time: %v, %d total, %d success, %d retry, %d fail",
		time.Since(bh.time), total, success, retry, fail)

	// Lookup actual data and push..., return error if fails

	return nil
}

// Define listen handler
type listenHandler struct {
	time  time.Time
}

func (lh *listenHandler) Send(n *pubsub.Notify) (hash string, jsonBytes []byte, err error) {
	return "test." + n.ID + ".v" + strconv.Itoa(n.Version), nil, nil
}

// Initialize the subscriber
s, err := pubsub.NewSubscriber(*sql.Connection, "subscriber-example")
if err != nil {
	// handle err
}

// Initialize the listen handler
lh := &listenHandler{
	time: time.Now(),
}

// Listen for pub events
if err := s.Listen(*sql.ConnParam, lh); err != nil {
	// handle err
}

// Initialize the data handler
bh := &batchHandler{
	time: time.Now(),
	s:    s,
}

batchSize := 100 // The number of records to send to a subscriber at a time
batchLimit := 1000 // The total number of records to process in the batch

// Run the batch
if err := s.RunBatch(bh, batchSize, batchLimit); err != nil {
	// handle err
}

```



  * The sub data handler will receive a notification of new/updated pub data with the following: pub id, data type, data id, deleted field, JSON representation of pub data (if set), previous hash (if set) and the new pub data version (incremented every time a record is published).
  * The subscriber has a built in retry system. When a sub data handler returns an error, it will increment the retry count and try again the next time it is called. Once the retry count exceeds the subscriber's retry count, it will mark that record as failed and, if set, call the error handler. The subscriber can be configured to have 0 retries, in which case it will fail the first time.

 ### Missing features to be implemented:
  * API to create publishers, subscribers and pub/sub links. Currently, they must be created manualy in the database
  * Optionally utilize the process package to parallelize processing of sub data
  * Optionally require no duplicates be sent. This will slow down processing as it will have to ensure the record was processed before continuing
