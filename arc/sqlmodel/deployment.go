package sqlmodel

import (
	"fmt"
	"strings"

	"github.com/Skyrin/go-lib/arc/model"
	gle "github.com/Skyrin/go-lib/errors"
	"github.com/Skyrin/go-lib/sql"
)

const (
	DeploymentTableName     = "arc_deployment"
	DeploymentDefaultSortBy = "arc_deployment_id"
)

// UserGetParam model
type DeploymentGetParam struct {
	Limit       uint64
	Offset      uint64
	ID          *int
	Code        *string
	FlagCount   bool
	OrderByID   string
	OrderByCode string
}

// UserUpdateParam model
type DeploymentUpdateParam struct {
	Token              *string
	TokenExpiry        *int
	RefreshToken       *string
	RefreshTokenExpiry *int
}

// DeploymentUpdate performs update, setting token/refresh token (and expiries)
func DeploymentUpdate(db *sql.Connection, id int, dup *DeploymentUpdateParam) (err error) {
	ub := db.Update(DeploymentTableName).
		Set("updated_on", "now()").
		Where("arc_deployment_id=?", id)

	if dup == nil {
		return nil // Nothing to update
	}

	if dup.Token != nil {
		ub = ub.Set("arc_deployment_token", *dup.Token)
	}

	if dup.TokenExpiry != nil {
		ub = ub.Set("arc_deployment_token_expiry", *dup.TokenExpiry)
	}

	if dup.RefreshToken != nil {
		ub = ub.Set("arc_deployment_refresh_token", *dup.RefreshToken)
	}

	if dup.RefreshTokenExpiry != nil {
		ub = ub.Set("arc_deployment_refresh_token_expiry", *dup.RefreshTokenExpiry)
	}

	err = db.ExecUpdate(ub)
	if err != nil {
		return gle.Wrap(err, "DeploymentUpdate.1", "")
	}

	return nil
}

// DeploymentGet performs select
func DeploymentGet(db *sql.Connection,
	p *DeploymentGetParam) (dList []*model.Deployment, count int, err error) {
	if p.Limit == 0 {
		p.Limit = 1
	}

	fields := `arc_deployment_id,arc_deployment_code,arc_deployment_name,
	arc_deployment_manage_url,arc_deployment_api_url,
	arc_deployment_oauth2_client_id,arc_deployment_oauth2_client_secret,
	arc_deployment_token,arc_deployment_token_expiry,
	arc_deployment_refresh_token,arc_deployment_refresh_token_expiry,
	arc_deployment_log_event_code,arc_deployment_log_publish_key`

	sb := db.Select("{fields}").
		From(DeploymentTableName).
		Limit(p.Limit)

	if p.ID != nil && *p.ID > 0 {
		sb = sb.Where("arc_deployment_id=?", *p.ID)
	}

	if p.Code != nil && len(*p.Code) > 0 {
		sb = sb.Where("arc_deployment_code=?", *p.Code)
	}

	stmt, bindList, err := sb.ToSql()
	if err != nil {
		return nil, 0, gle.Wrap(err, "DeploymentGet.1", "")
	}

	if p.FlagCount {
		row := db.QueryRow(strings.Replace(stmt, "{fields}", "count(*)", 1), bindList...)
		if err := row.Scan(&count); err != nil {
			return nil, 0, gle.Wrap(err, fmt.Sprintf("DeploymentGet.2 | stmt: %s, bindList: %+v",
				stmt, bindList), "")
		}
	}

	sb = sb.Offset(uint64(p.Offset))

	if p.OrderByID != "" {
		sb = sb.OrderBy(fmt.Sprintf("arc_deployment_id %s", p.OrderByID))
	}

	if p.OrderByCode != "" {
		sb = sb.OrderBy(fmt.Sprintf("arc_deployment_code %s", p.OrderByCode))
	}

	stmt, bindList, err = sb.ToSql()
	stmt = strings.Replace(stmt, "{fields}", fields, 1)

	rows, err := db.Query(stmt, bindList...)
	if err != nil {
		return nil, 0, gle.Wrap(err, "DeploymentGet.3", "")
	}
	defer rows.Close()

	for rows.Next() {
		d := &model.Deployment{}
		if err := rows.Scan(&d.ID, &d.Code, &d.Name,
			&d.ManageURL, &d.APIURL,
			&d.ClientID, &d.ClientSecret,
			&d.Token, &d.TokenExpiry,
			&d.RefreshToken, &d.RefreshTokenExpiry,
			&d.LogEventCode, &d.LogPublishKey); err != nil {
			return nil, 0, gle.Wrap(err, "DeploymentGet.4", "")
		}

		dList = append(dList, d)
	}

	return dList, count, nil
}

// DeploymentGetByCode returns the deployment with the specified code
func DeploymentGetByCode(db *sql.Connection, code string) (d *model.Deployment, err error) {
	dList, _, err := DeploymentGet(db, &DeploymentGetParam{
		Limit: 1,
		Code:  &code,
	})

	if err != nil {
		return nil, gle.Wrap(err, "DeploymentGetByCode.1", "")
	}

	if len(dList) != 1 {
		return nil, gle.Wrap(err, "DeploymentGetByCode.2", "")
	}

	return dList[0], nil
}
