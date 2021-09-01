package sqlmodel

import (
	"fmt"
	"strings"

	"github.com/Skyrin/go-lib/arc/model"
	gle "github.com/Skyrin/go-lib/errors"
	"github.com/Skyrin/go-lib/sql"
)

const (
	DeploymentStoreTableName     = "arc_deployment_store"
	DeploymentStoreDefaultSortBy = "arc_deployment_id"
)

// DeploymentStoreGetParam get params
type DeploymentStoreGetParam struct {
	Limit        uint64
	Offset       uint64
	DeploymentID *int
	Code         *string
	FlagCount    bool
	OrderByID    string
	OrderByCode  string
}

// DeploymentStoreGet performs select
func DeploymentStoreGet(db *sql.Connection,
	p *DeploymentStoreGetParam) (dsList []*model.DeploymentStore, count int, err error) {
	if p.Limit == 0 {
		p.Limit = 1
	}

	fields := `arc_deployment_id,arc_deployment_store_code,
	arc_deployment_store_client_id,arc_deployment_store_client_secret`

	sb := db.Select("{fields}").
		From(DeploymentStoreTableName).
		Limit(p.Limit)

	if p.DeploymentID != nil && *p.DeploymentID > 0 {
		sb = sb.Where("arc_deployment_id=?", *p.DeploymentID)
	}

	if p.Code != nil && len(*p.Code) > 0 {
		sb = sb.Where("arc_deployment_store_code=?", *p.Code)
	}

	stmt, bindList, err := sb.ToSql()
	if err != nil {
		return nil, 0, gle.Wrap(err, "DeploymentStoreGet.1", "")
	}

	if p.FlagCount {
		row := db.QueryRow(strings.Replace(stmt, "{fields}", "count(*)", 1), bindList...)
		if err := row.Scan(&count); err != nil {
			return nil, 0, gle.Wrap(err, fmt.Sprintf("DeploymentStoreGet.2 | stmt: %s, bindList: %+v",
				stmt, bindList), "")
		}
	}

	sb = sb.Offset(uint64(p.Offset))

	if p.OrderByID != "" {
		sb = sb.OrderBy(fmt.Sprintf("arc_deployment_id %s", p.OrderByID))
	}

	if p.OrderByCode != "" {
		sb = sb.OrderBy(fmt.Sprintf("arc_deployment_store_code %s", p.OrderByCode))
	}

	stmt, bindList, err = sb.ToSql()
	stmt = strings.Replace(stmt, "{fields}", fields, 1)

	rows, err := db.Query(stmt, bindList...)
	if err != nil {
		return nil, 0, gle.Wrap(err, "DeploymentStoreGet.3", "")
	}
	defer rows.Close()

	for rows.Next() {
		ds := &model.DeploymentStore{}
		if err := rows.Scan(&ds.DeploymentID, &ds.Code,
			&ds.ClientID, &ds.ClientSecret); err != nil {
			return nil, 0, gle.Wrap(err, "DeploymentStoreGet.4", "")
		}

		dsList = append(dsList, ds)
	}

	return dsList, count, nil
}

// DeploymentStoreGetByCodeAndDeploymentID returns the deployment store with the
// deployment id and deployment store code
func DeploymentStoreGetByCodeAndDeploymentID(db *sql.Connection,
	deploymentId int, code string) (d *model.DeploymentStore, err error) {

	dsList, _, err := DeploymentStoreGet(db, &DeploymentStoreGetParam{
		Limit:        1,
		Code:         &code,
		DeploymentID: &deploymentId,
	})

	if err != nil {
		return nil, gle.Wrap(err, "DeploymentStoreGetByCode.1", "")
	}

	if len(dsList) != 1 {
		return nil, gle.Wrap(err, "DeploymentStoreGetByCode.2", "")
	}

	return dsList[0], nil
}
