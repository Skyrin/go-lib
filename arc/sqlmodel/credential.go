package sqlmodel

import (
	"fmt"
	"strings"

	"github.com/Skyrin/go-lib/arc/model"
	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/sql"
)

const (
	CredentialTableName     = "arc_credential"
	CredentialDefaultSortBy = "arc_credential_id"
)

// UserGetParam model
type CredentialGetParam struct {
	Limit        uint64
	Offset       uint64
	ID           *int
	DeploymentID *int
	FlagCount    bool
	OrderByID    string
	OrderByCode  string
}

// CredentialGet performs select
func CredentialGet(db *sql.Connection,
	p *CredentialGetParam) (cList []*model.Credential, count int, err error) {
	if p.Limit == 0 {
		p.Limit = 1
	}

	fields := `arc_credential_id,arc_deployment_id,arc_credential_name,
	arc_credential_client_id,arc_credential_client_secret`

	sb := db.Select("{fields}").
		From(CredentialTableName).
		Limit(p.Limit)

	if p.ID != nil && *p.ID >= 0 {
		sb = sb.Where("arc_credential_id=?", *p.ID)
	}

	if p.DeploymentID != nil && *p.DeploymentID >= 0 {
		sb = sb.Where("arc_deployment_id=?", *p.DeploymentID)
	}

	stmt, bindList, err := sb.ToSql()
	if err != nil {
		return nil, 0, e.Wrap(err, e.Code040R, "01")
	}

	if p.FlagCount {
		row := db.QueryRow(strings.Replace(stmt, "{fields}", "count(*)", 1), bindList...)
		if err := row.Scan(&count); err != nil {
			return nil, 0, e.Wrap(err, e.Code040R, "02",
				fmt.Sprintf("CredentialGet.2 | stmt: %s, bindList: %+v",
					stmt, bindList))
		}
	}

	sb = sb.Offset(uint64(p.Offset))

	if p.OrderByID != "" {
		sb = sb.OrderBy(fmt.Sprintf("arc_credential_id %s", p.OrderByID))
	}

	if p.OrderByCode != "" {
		sb = sb.OrderBy(fmt.Sprintf("arc_credential_code %s", p.OrderByCode))
	}

	stmt, bindList, err = sb.ToSql()
	stmt = strings.Replace(stmt, "{fields}", fields, 1)

	rows, err := db.Query(stmt, bindList...)
	if err != nil {
		return nil, 0, e.Wrap(err, e.Code040R, "03")
	}
	defer rows.Close()

	for rows.Next() {
		c := &model.Credential{}
		if err := rows.Scan(&c.ID, &c.DeploymentID, &c.Name,
			&c.ClientID, &c.ClientSecret); err != nil {
			return nil, 0, e.Wrap(err, e.Code040R, "04")
		}

		cList = append(cList, c)
	}

	return cList, count, nil
}

// CredentialGetByID returns the deployment with the specified code
func CredentialGetByID(db *sql.Connection, id int) (c *model.Credential, err error) {
	cList, _, err := CredentialGet(db, &CredentialGetParam{
		Limit: 1,
		ID:    &id,
	})

	if err != nil {
		return nil, e.Wrap(err, e.Code040S, "01")
	}

	if len(cList) != 1 {
		return nil, e.New(e.Code040S, "01", e.MsgCredentialDoesNotExist)
	}

	return cList[0], nil
}
