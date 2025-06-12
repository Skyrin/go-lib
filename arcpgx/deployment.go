package arcpgx

import (
	"context"
	"fmt"
	"strings"

	"github.com/Skyrin/go-lib/arcpgx/model"
	"github.com/Skyrin/go-lib/arcpgx/sqlmodel"
	"github.com/Skyrin/go-lib/e"
	sql "github.com/Skyrin/go-lib/sqlpgx"
	"github.com/rs/zerolog/log"
)

const (
	ECode040O01 = e.Code040O + "01"
	ECode040O02 = e.Code040O + "02"
	ECode040O03 = e.Code040O + "03"
)

type Deployment struct {
	DB        *sql.Connection
	Model     *model.Deployment
	Listener  *DeploymentNotify
	StoreCode string
}

// Refresh updates this objects properties from the corresponding record in
// the database
func (d *Deployment) Refresh(ctx context.Context) {
	newDep, err := sqlmodel.DeploymentGetByCode(ctx, d.DB, d.Model.Code)
	if err != nil {
		log.Error().Err(err).Msg("Deployment.Refresh.1")
	}

	d.Model = newDep
}

// UpdateGrant updates this deployment's database record with the new
// grant info (token/expiry and refresh token/expiry)
func (d *Deployment) UpdateGrant(ctx context.Context, g *Grant) (err error) {
	if err := sqlmodel.DeploymentUpdate(ctx, d.DB, d.Model.ID, &sqlmodel.DeploymentUpdateParam{
		Token:              &g.Token,
		TokenExpiry:        &g.TokenExpiry,
		RefreshToken:       &g.RefreshToken,
		RefreshTokenExpiry: &g.RefreshTokenExpiry,
	}); err != nil {
		return e.W(err, ECode040O01)
	}

	return nil
}

// NewDeployment initializes a new deployment and returns it
func NewDeployment(ctx context.Context, db *sql.Connection, cp *sql.ConnParam, deploymentCode string) (d *Deployment, err error) {
	md, err := sqlmodel.DeploymentGetByCode(ctx, db, deploymentCode)
	if err != nil {
		return nil, e.W(err, ECode040O02)
	}

	dn, err := NewDeploymentNotify(cp)
	if err != nil {
		return nil, e.W(err, ECode040O03)
	}

	dn.Notify = func(deploymentCode string) {
		if d.Model.Code == deploymentCode {
			d.Refresh(ctx)
		}
	}

	d = &Deployment{
		DB:       db,
		Model:    md,
		Listener: dn,
	}

	return d, nil
}

// getAPICoreServiceURL returns core service URL for arc API domain
func (d *Deployment) getAPICoreServiceURL() string {
	var sb strings.Builder
	_, _ = sb.WriteString(d.Model.APIURL)
	_, _ = sb.WriteString(corePath)
	return sb.String()
}

// getAPICoreServiceURL returns core service URL for arc manager domain
func (d *Deployment) getManageCoreServiceURL() string {
	var sb strings.Builder
	_, _ = sb.WriteString(d.Model.ManageURL)
	_, _ = sb.WriteString(corePath)
	return sb.String()
}

// getAPIArcimedesServiceURL returns arcimedes service URL for arc API domain
func (d *Deployment) getAPIArcimedesServiceURL() string {
	var sb strings.Builder
	_, _ = sb.WriteString(d.Model.APIURL)
	_, _ = sb.WriteString(arcimedesPath)
	return sb.String()
}

// getManageArcimedesServiceURL returns arcimedes service URL for arc manager domain
func (d *Deployment) getManageArcimedesServiceURL() string {
	var sb strings.Builder
	_, _ = sb.WriteString(d.Model.ManageURL)
	_, _ = sb.WriteString(arcimedesPath)
	return sb.String()
}

// getAPICartServiceURL returns cart service URL for the specified store code for arc API domain
func (d *Deployment) getAPICartServiceURL(storeCode string) string {
	var sb strings.Builder
	_, _ = sb.WriteString(d.Model.APIURL)
	_, _ = sb.WriteString(fmt.Sprintf(cartPath, storeCode))
	return sb.String()
}

// getManageCartServiceURL returns cart service URL for the specified store code for arc manager domain
func (d *Deployment) getManageCartServiceURL(storeCode string) string {
	var sb strings.Builder
	_, _ = sb.WriteString(d.Model.ManageURL)
	_, _ = sb.WriteString(fmt.Sprintf(cartPath, storeCode))
	return sb.String()
}
