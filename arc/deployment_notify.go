package arc

import (
	"time"

	"github.com/Skyrin/go-lib/errors"
	"github.com/Skyrin/go-lib/sql"
	"github.com/lib/pq"
	"github.com/rs/zerolog/log"
)

const CHANNEL_ARC_DEPLOYMENT_NOTIFY = "arc_deployment_notify"

// DeploymentNotify use to listen for change events to records in the arc_deployment
// table. If an insert or update occurs, the event will be triggered with the
// deployment code passed as the event data. It is the responsibility of the
// creater of this to do something with that deployment code (i.e. lookup the
// new data if it is using that code and update accordingly)
type DeploymentNotify struct {
	Listener *pq.Listener
	Failed   chan error
	Notify   func(deploymentCode string)
}

// NewDeploymentNotify create a new deployment notify instance
func NewDeploymentNotify() (dn *DeploymentNotify, err error) {
	dn = &DeploymentNotify{Failed: make(chan error, 2)}

	listener := pq.NewListener(sql.GetConnectionStr(nil), 10*time.Second, time.Minute, dn.Log)
	if err := listener.Listen(CHANNEL_ARC_DEPLOYMENT_NOTIFY); err != nil {
		listener.Close()
		return nil, errors.Wrap(err, "NewDeploymentNotify.2", "")
	}

	dn.Listener = listener

	go func() {
		if err := dn.Listen(); err != nil {
			log.Warn().Err(err).Msg("NewDeploymentNotify.3")
		}
	}()

	return dn, nil
}

// Log handles logging errors
func (dn *DeploymentNotify) Log(ev pq.ListenerEventType, err error) {
	if err != nil {
		log.Warn().Err(err).Msg("DeploymentNotify.1")
	}

	if ev == pq.ListenerEventConnectionAttemptFailed {
		dn.Failed <- err
	}
}

func (dn *DeploymentNotify) Listen() (err error) {
	for {
		select {
		case e := <-dn.Listener.Notify:
			if e == nil {
				continue
			}

			dn.Notify(e.Extra)
		case err := <-dn.Failed:
			return errors.Wrap(err, "DeploymentNotify.Listen.1", "")
		case <-time.After(time.Minute):
			go dn.Listener.Ping()
		}
	}
}

func (dn *DeploymentNotify) Close() (err error) {
	close(dn.Failed)
	return dn.Listener.Close()
}
