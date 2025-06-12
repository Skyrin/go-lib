package sqlmodel

import (
	"context"
	"fmt"
	"strings"

	"github.com/Skyrin/go-lib/arcpgx/model"
	"github.com/Skyrin/go-lib/e"
	sql "github.com/Skyrin/go-lib/sqlpgx"
)

const (
	CredentialTableName     = "arc_credential"
	CredentialDefaultSortBy = "arc_credential_id"

	ECode040E01 = e.Code040E + "01"
	ECode040E02 = e.Code040E + "02"
	ECode040E03 = e.Code040E + "03"
	ECode040E04 = e.Code040E + "04"
	ECode040E05 = e.Code040E + "05"
	ECode040E06 = e.Code040E + "06"
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
func CredentialGet(ctx context.Context, db *sql.Connection,
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
		return nil, 0, e.W(err, ECode040E01)
	}

	if p.FlagCount {
		row := db.QueryRow(ctx, strings.Replace(stmt, "{fields}", "count(*)", 1), bindList...)
		if err := row.Scan(&count); err != nil {
			return nil, 0, e.W(err, ECode040E02,
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

	rows, err := db.Query(ctx, stmt, bindList...)
	if err != nil {
		return nil, 0, e.W(err, ECode040E03)
	}
	defer rows.Close()

	for rows.Next() {
		c := &model.Credential{}
		if err := rows.Scan(&c.ID, &c.DeploymentID, &c.Name,
			&c.ClientID, &c.ClientSecret); err != nil {
			return nil, 0, e.W(err, ECode040E04)
		}

		cList = append(cList, c)
	}

	return cList, count, nil
}

// CredentialGetByID returns the deployment with the specified code
func CredentialGetByID(ctx context.Context, db *sql.Connection, id int) (c *model.Credential, err error) {
	cList, _, err := CredentialGet(ctx, db, &CredentialGetParam{
		Limit: 1,
		ID:    &id,
	})

	if err != nil {
		return nil, e.W(err, ECode040E05)
	}

	if len(cList) != 1 {
		return nil, e.N(ECode040E06, e.MsgCredentialDoesNotExist)
	}

	return cList[0], nil
}
