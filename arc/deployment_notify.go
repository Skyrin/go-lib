package arc

import (
	"time"

	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/sql"
	"github.com/lib/pq"
	"github.com/rs/zerolog/log"
)

const CHANNEL_ARC_DEPLOYMENT_NOTIFY = "arc_deployment_notify"

const (
	ECode040601 = e.Code0406 + "01"
	ECode040602 = e.Code0406 + "02"
	ECode040603 = e.Code0406 + "03"
	ECode040604 = e.Code0406 + "04"
)

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
func NewDeploymentNotify(cp *sql.ConnParam) (dn *DeploymentNotify, err error) {
	dn = &DeploymentNotify{Failed: make(chan error, 2)}

	connStr := sql.GetConnectionStr(cp)

	listener := pq.NewListener(connStr, 10*time.Second, time.Minute, dn.Log)
	if err := listener.Listen(CHANNEL_ARC_DEPLOYMENT_NOTIFY); err != nil {
		listener.Close()
		return nil, e.W(err, ECode040601)
	}

	dn.Listener = listener

	go func() {
		if err := dn.Listen(); err != nil {
			log.Warn().Err(err).Msgf("%s%s", ECode040602)
		}
	}()

	return dn, nil
}

// Log handles logging errors
func (dn *DeploymentNotify) Log(ev pq.ListenerEventType, err error) {
	if err != nil {
		log.Warn().Err(err).Msgf("%s%s", ECode040603)
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
			return e.W(err, ECode040604)
		case <-time.After(time.Minute):
			go dn.Listener.Ping()
		}
	}
}

func (dn *DeploymentNotify) Close() (err error) {
	close(dn.Failed)
	return dn.Listener.Close()
}
