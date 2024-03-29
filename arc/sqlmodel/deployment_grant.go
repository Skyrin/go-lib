package sqlmodel

import (
	"crypto"
	"fmt"
	"strings"

	"github.com/Skyrin/go-lib/arc/model"
	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/sql"
)

const (
	DeploymentGrantTableName     = "arc_deployment_grant"
	DeploymentGrantDefaultSortBy = "arc_deployment_grant_id"

	ECode040Z01 = e.Code040Z + "01"
	ECode040Z02 = e.Code040Z + "02"
	ECode040Z03 = e.Code040Z + "03"
	ECode040Z04 = e.Code040Z + "04"
	ECode040Z05 = e.Code040Z + "05"
	ECode040Z06 = e.Code040Z + "06"
	ECode040Z07 = e.Code040Z + "07"
	ECode040Z08 = e.Code040Z + "08"
	ECode040Z09 = e.Code040Z + "09"
	ECode040Z0A = e.Code040Z + "0A"
)

// DeploymentGrantGetParam get params
type DeploymentGrantGetParam struct {
	Limit                     uint64
	Offset                    uint64
	ID                        *int
	DeploymentID              *int
	Token                     *string
	FlagCount                 bool
	OrderByID                 string
	OrderByRefreshTokenExpiry string
}

// DeploymentGrantUpdateParam update params
type DeploymentGrantUpdateParam struct {
	Token              *string
	TokenExpiry        *int
	RefreshToken       *string
	RefreshTokenExpiry *int
}

// DeploymentGrantInsertParam insert params
type DeploymentGrantInsertParam struct {
	DeploymentID       int
	ArcUserID          int
	CredentialID       int
	Token              string
	TokenExpiry        int
	RefreshToken       string
	RefreshTokenExpiry int
}

func deploymentGrantHashToken(token string) string {
	h := crypto.SHA512.New()
	defer h.Reset()
	_, _ = h.Write([]byte(token))
	return fmt.Sprintf("%x", h.Sum(nil))
}

// DeploymentGrantInsert performs insert
func DeploymentGrantInsert(db *sql.Connection, ip *DeploymentGrantInsertParam) (id int, err error) {
	ib := db.Insert(DeploymentGrantTableName).
		Columns(`arc_deployment_id,arc_user_id,
		arc_credential_id,
		arc_deployment_grant_token,arc_deployment_grant_token_expiry,
		arc_deployment_grant_token_hash,
		arc_deployment_grant_refresh_token,arc_deployment_grant_refresh_token_expiry`).
		Values(ip.DeploymentID, ip.ArcUserID,
			ip.CredentialID,
			ip.Token, ip.TokenExpiry,
			deploymentGrantHashToken(ip.Token),
			ip.RefreshToken, ip.RefreshTokenExpiry,
		).Suffix("RETURNING arc_deployment_grant_id")

	id, err = db.ExecInsertReturningID(ib)
	if err != nil {
		return 0, e.W(err, ECode040Z02)
	}

	return id, nil
}

// DeploymentGrantUpdate performs update, setting token/refresh token (and expiries)
func DeploymentGrantUpdate(db *sql.Connection, id int, up *DeploymentGrantUpdateParam) (err error) {
	ub := db.Update(DeploymentGrantTableName).
		Where("arc_deployment_grant_id=?", id)

	if up == nil {
		return nil // Nothing to update
	}

	if up.Token != nil {
		ub = ub.Set("arc_deployment_grant_token", *up.Token)
		ub = ub.Set("arc_deployment_grant_token_hash",
			deploymentGrantHashToken(*up.Token))
	}

	if up.TokenExpiry != nil {
		ub = ub.Set("arc_deployment_grant_token_expiry", *up.TokenExpiry)
	}

	if up.RefreshToken != nil {
		ub = ub.Set("arc_deployment_grant_refresh_token", *up.RefreshToken)
	}

	if up.RefreshTokenExpiry != nil {
		ub = ub.Set("arc_deployment_grant_refresh_token_expiry", *up.RefreshTokenExpiry)
	}

	err = db.ExecUpdate(ub)
	if err != nil {
		return e.W(err, ECode040Z03)
	}

	return nil
}

// DeploymentGrantGet performs select
func DeploymentGrantGet(db *sql.Connection,
	p *DeploymentGrantGetParam) (dgList []*model.DeploymentGrant, count int, err error) {
	if p.Limit == 0 {
		p.Limit = 1
	}

	fields := `arc_deployment_grant_id,arc_deployment_id,arc_user_id,
	arc_credential_id,
	arc_deployment_grant_token,arc_deployment_grant_token_expiry,
	arc_deployment_grant_refresh_token,arc_deployment_grant_refresh_token_expiry`

	sb := db.Select("{fields}").
		From(DeploymentGrantTableName).
		Limit(p.Limit)

	if p.ID != nil && *p.ID > 0 {
		sb = sb.Where("arc_deployment_grant_id=?", *p.ID)
	}

	if p.DeploymentID != nil && *p.DeploymentID > 0 {
		sb = sb.Where("arc_deployment_id=?", *p.DeploymentID)
	}

	if p.Token != nil && len(*p.Token) > 0 {
		sb = sb.Where("arc_deployment_grant_token=?", *p.Token)
	}

	stmt, bindList, err := sb.ToSql()
	if err != nil {
		return nil, 0, e.W(err, ECode040Z04)
	}

	if p.FlagCount {
		row := db.QueryRow(strings.Replace(stmt, "{fields}", "count(*)", 1), bindList...)
		if err := row.Scan(&count); err != nil {
			return nil, 0, e.W(err, ECode040Z05,
				fmt.Sprintf("stmt: %s", stmt))
		}
	}

	sb = sb.Offset(uint64(p.Offset))

	if p.OrderByID != "" {
		sb = sb.OrderBy(fmt.Sprintf("arc_deployment_grant_id %s", p.OrderByID))
	}

	if p.OrderByRefreshTokenExpiry != "" {
		sb = sb.OrderBy(fmt.Sprintf("arc_deployment_grant_refresh_token_expiry %s", p.OrderByRefreshTokenExpiry))
	}

	stmt, bindList, err = sb.ToSql()
	stmt = strings.Replace(stmt, "{fields}", fields, 1)

	rows, err := db.Query(stmt, bindList...)
	if err != nil {
		return nil, 0, e.W(err, ECode040Z06)
	}
	defer rows.Close()

	for rows.Next() {
		dg := &model.DeploymentGrant{}
		if err := rows.Scan(&dg.ID, &dg.DeploymentID, &dg.ArcUserID,
			&dg.CredentialID,
			&dg.Token, &dg.TokenExpiry,
			&dg.RefreshToken, &dg.RefreshTokenExpiry); err != nil {
			return nil, 0, e.W(err, ECode040Z07)
		}

		dgList = append(dgList, dg)
	}

	return dgList, count, nil
}

// DeploymentGrantGetByToken returns the deployment with the specified token
func DeploymentGrantGetByToken(db *sql.Connection, token string) (dg *model.DeploymentGrant, err error) {
	dgList, _, err := DeploymentGrantGet(db, &DeploymentGrantGetParam{
		Limit: 1,
		Token: &token,
	})

	if err != nil {
		return nil, e.W(err, ECode040Z08)
	}

	if len(dgList) != 1 {
		return nil, e.N(ECode040Z01, e.MsgGrantDoesNotExist)
	}

	return dgList[0], nil
}

// DeploymentGrantPurgeByToken purges a record by the token
func DeploymentGrantPurgeByToken(db *sql.Connection, token string) (err error) {
	delB := db.Delete(DeploymentGrantTableName).
		Where("arc_deployment_grant_token=?", token)

	if err := db.ExecDelete(delB); err != nil {
		return e.W(err, ECode040Z09)
	}

	return nil
}

// DeploymentGrantPurgeByExpiredRefreshToken purges all grants where the refresh token has expired and
// is no longer useable (the token should have expired as well at this point because refresh tokens
// are periodically updated when fetching new tokens)
func DeploymentGrantPurgeByExpiredRefreshToken(db *sql.Connection, refreshTokenExpiry int) (err error) {
	delB := db.Delete(DeploymentGrantTableName).
		Where("arc_deployment_grant_refresh_token_expiry < ?", refreshTokenExpiry)

	if err := db.ExecDelete(delB); err != nil {
		return e.W(err, ECode040Z0A)
	}

	return nil
}

