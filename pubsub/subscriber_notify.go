package pubsub

import (
	"encoding/json"
	"time"

	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/pubsub/internal/sqlmodel"
	"github.com/Skyrin/go-lib/pubsub/model"
	"github.com/Skyrin/go-lib/sql"
	"github.com/lib/pq"
	"github.com/rs/zerolog/log"
)

const (
	channelName = "skyrin_dps_notify"

	// Error constants
	ECode070301 = e.Code0703 + "01"
	ECode070302 = e.Code0703 + "02"
	ECode070303 = e.Code0703 + "03"
	ECode070304 = e.Code0703 + "04"
	ECode070305 = e.Code0703 + "05"
	ECode070306 = e.Code0703 + "06"
	ECode070307 = e.Code0703 + "07"
	ECode070308 = e.Code0703 + "08"
	ECode070309 = e.Code0703 + "09"
	ECode07030A = e.Code0703 + "0A"
	ECode07030B = e.Code0703 + "0B"
	ECode07030C = e.Code0703 + "0C"
)

// Event the expected JSON from a skyrin_dps_notify call
type Event struct {
	PubID        int            `json:"pubId"`
	Type         string         `json:"dataType"`
	ID           string         `json:"dataId"`
	Deleted      bool           `json:"deleted"`
	Version      int            `json:"version"`
	PreviousHash string         `json:"-"`
	NewHash      string         `json:"-"`
	NewJSON      []byte         `json:"-"`
	sd           *model.SubData `json:"-"`
}

// SubDataListener defines the logic to send the publish event for a listening subscriber
type SubDataListener interface {
	// Send should send the publish event for the subscriber, returning the hash of the
	// object sent and optionally a JSON representation of the object. If it failed, it
	// should return an error.
	Send(ev *Event) (hash string, jsonBytes []byte, err error)
}

// Listen use to listen for change events to records in the skyrin_dps_data
// table. If an insert or update occurs, the event will be triggered with a JSON
// string. The subscriber will check if the pubId matches a linked publisher. If it
// does, it will proceed to process that record.
func (s *Subscriber) Listen(cp *sql.ConnParam, sdl SubDataListener) (err error) {
	s.h = sdl
	s.errCh = make(chan error, 2)
	s.doneCh = make(chan struct{})

	connStr := sql.GetConnectionStr(cp)

	s.listener = pq.NewListener(connStr, 10*time.Second, time.Minute, s.log)
	if err := s.listener.Listen(channelName); err != nil {
		s.listener.Close()
		return e.W(err, ECode070301)
	}

	go func() {
		// start listening
		if err := s.listen(); err != nil {
			log.Warn().Err(err).Msgf("%s%s", ECode070302)
		}
	}()

	return nil
}

// Close stops listening and cleans up
func (s *Subscriber) Close() (err error) {
	close(s.errCh)
	close(s.doneCh)
	if err := s.listener.Close(); err != nil {
		return e.W(err, ECode070303)
	}
	return nil
}

// log handles logging errors
func (s *Subscriber) log(ev pq.ListenerEventType, err error) {
	if err != nil {
		if s.errHandler != nil {
			// Call the defined error handler
			s.errHandler(err)
		} else {
			log.Warn().Err(err).Msgf("%s%s", ECode070304)
		}
	}

	if ev == pq.ListenerEventConnectionAttemptFailed {
		s.errCh <- err
	}
}

// listen listens until an error occurs or the listener is closed
func (s *Subscriber) listen() (err error) {
	for {
		select {
		case ev := <-s.listener.Notify:
			if ev == nil {
				continue
			}

			if err := s.notify(ev.Extra); err != nil {
				ne := e.W(err, ECode070305)

				if s.errHandler != nil {
					s.errHandler(ne)
				} else {
					log.Warn().Err(ne).Msg("notify failed")
				}
			}
		case <-s.doneCh:
			return nil
		case err := <-s.errCh:
			return e.W(err, ECode070306)
		case <-time.After(time.Minute):
			go s.listener.Ping()
		}
	}
}

// notify converts the jsonStr from the pg_notify call to a notify object, then locks the
// associated sub data record for update. It then calls processNotify, which will call the
// configured data handler and also handle error cases from that call. The sub data record
// will then be updated accordingly, i.e. marked as completed with the version updated, have
// the retry number increased, or mark it as failed with the error message.
func (s *Subscriber) notify(jsonStr string) (err error) {
	ev := &Event{}
	if err := json.Unmarshal([]byte(jsonStr), ev); err != nil {
		return e.W(err, ECode070307)
	}

	// Check if this subscriber is listening to this publisher
	for i := range s.pubList {
		if s.pubList[i].ID == ev.PubID {
			tx, err := s.db.BeginReturnDB()
			if err != nil {
				return e.W(err, ECode070308)
			}
			defer tx.RollbackIfInTxn()

			// Lock record for update
			sd, err := sqlmodel.SubDataGetBySubIDPubIDDataTypeAndDataIDForUpdate(tx,
				s.sub.ID, ev.PubID, ev.Type, ev.ID, ev.Version)
			if err != nil {
				// If the error is not the does not exist error, then return the error
				if !e.ContainsError(err, sqlmodel.ECode07090E) {
					return e.W(err, ECode070309)
				}

				// The record doesn't exist, it will try to be created in the upsert below, but need
				// to make the model object here
				sd = &model.SubData{
					SubID:   s.sub.ID,
					PubID:   ev.PubID,
					Type:    ev.Type,
					DataID:  ev.ID,
					Deleted: ev.Deleted,
					Version: ev.Version,
					Status:  model.SubDataStatusPending,
				}
			} else {
				if ev.Version <= sd.Version {
					// If the version in the notify isn't newer than the current sub data version, ignore it
					return nil
				}
			}

			ev.PreviousHash = sd.Hash

			// Send the event
			newHash, newJSON, err := s.h.Send(ev)
			sd.SetResponse(newHash, newJSON, err, ev.Version, ev.Deleted, s.sub)

			// Update the status of the sub data record
			if err := sqlmodel.SubDataUpsert(tx, sd); err != nil {
				return e.W(err, ECode07030B)
			}

			if err := tx.Commit(); err != nil {
				return e.W(err, ECode07030C)
			}

			break
		}
	}

	return nil
}
