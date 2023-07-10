package pubsub

import (
	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/pubsub/internal/sqlmodel"
	"github.com/Skyrin/go-lib/pubsub/model"
	"github.com/Skyrin/go-lib/sql"
)

const (
	// Error constants
	ECode070C01 = e.Code070C + "01"
)

// GetPublisher returns the pub record if it exists
func GetPublisher(db *sql.Connection, code string) (p *model.Pub, err error) {
	p, err = sqlmodel.PubGetByCode(db, code)
	if err != nil {
		return nil, e.W(err, ECode070C01)
	}

	return p, nil
}
