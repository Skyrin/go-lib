package sqlmodel

import (
	"context"
	"crypto"
	"fmt"
	"strings"

	"github.com/Skyrin/go-lib/arcpgx/model"
	"github.com/Skyrin/go-lib/e"
	sql "github.com/Skyrin/go-lib/sqlpgx"
)

const (
	DeploymentGrantTableName     = "arc_deployment_grant"
	DeploymentGrantDefaultSortBy = "arc_deployment_grant_id"

	ECode040G01 = e.Code040G + "01"
	ECode040G02 = e.Code040G + "02"
	ECode040G03 = e.Code040G + "03"
	ECode040G04 = e.Code040G + "04"
	ECode040G05 = e.Code040G + "05"
	ECode040G06 = e.Code040G + "06"
	ECode040G07 = e.Code040G + "07"
	ECode040G08 = e.Code040G + "08"
	ECode040G09 = e.Code040G + "09"
	ECode040G0A = e.Code040G + "0A"
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
func DeploymentGrantInsert(ctx context.Context, db *sql.Connection, ip *DeploymentGrantInsertParam) (id int, err error) {
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

	id, err = db.ExecInsertReturningID(ctx, ib)
	if err != nil {
		return 0, e.W(err, ECode040G02)
	}

	return id, nil
}

// DeploymentGrantUpdate performs update, setting token/refresh token (and expiries)
func DeploymentGrantUpdate(ctx context.Context, db *sql.Connection, id int, up *DeploymentGrantUpdateParam) (err error) {
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

	err = db.ExecUpdate(ctx, ub)
	if err != nil {
		return e.W(err, ECode040G03)
	}

	return nil
}

// DeploymentGrantGet performs select
func DeploymentGrantGet(ctx context.Context, db *sql.Connection,
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
		return nil, 0, e.W(err, ECode040G04)
	}

	if p.FlagCount {
		row := db.QueryRow(ctx, strings.Replace(stmt, "{fields}", "count(*)", 1), bindList...)
		if err := row.Scan(&count); err != nil {
			return nil, 0, e.W(err, ECode040G05,
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

	rows, err := db.Query(ctx, stmt, bindList...)
	if err != nil {
		return nil, 0, e.W(err, ECode040G06)
	}
	defer rows.Close()

	for rows.Next() {
		dg := &model.DeploymentGrant{}
		if err := rows.Scan(&dg.ID, &dg.DeploymentID, &dg.ArcUserID,
			&dg.CredentialID,
			&dg.Token, &dg.TokenExpiry,
			&dg.RefreshToken, &dg.RefreshTokenExpiry); err != nil {
			return nil, 0, e.W(err, ECode040G07)
		}

		dgList = append(dgList, dg)
	}

	return dgList, count, nil
}

// DeploymentGrantGetByToken returns the deployment with the specified token
func DeploymentGrantGetByToken(ctx context.Context, db *sql.Connection, token string) (dg *model.DeploymentGrant, err error) {
	dgList, _, err := DeploymentGrantGet(ctx, db, &DeploymentGrantGetParam{
		Limit: 1,
		Token: &token,
	})

	if err != nil {
		return nil, e.W(err, ECode040G08)
	}

	if len(dgList) != 1 {
		return nil, e.N(ECode040G01, e.MsgGrantDoesNotExist)
	}

	return dgList[0], nil
}

// DeploymentGrantPurgeByToken purges a record by the token
func DeploymentGrantPurgeByToken(ctx context.Context, db *sql.Connection, token string) (err error) {
	delB := db.Delete(DeploymentGrantTableName).
		Where("arc_deployment_grant_token=?", token)

	if err := db.ExecDelete(ctx, delB); err != nil {
		return e.W(err, ECode040G09)
	}

	return nil
}

// DeploymentGrantPurgeByExpiredRefreshToken purges all grants where the refresh token has expired and
// is no longer useable (the token should have expired as well at this point because refresh tokens
// are periodically updated when fetching new tokens)
func DeploymentGrantPurgeByExpiredRefreshToken(ctx context.Context, db *sql.Connection, refreshTokenExpiry int) (err error) {
	delB := db.Delete(DeploymentGrantTableName).
		Where("arc_deployment_grant_refresh_token_expiry < ?", refreshTokenExpiry)

	if err := db.ExecDelete(ctx, delB); err != nil {
		return e.W(err, ECode040G0A)
	}

	return nil
}
