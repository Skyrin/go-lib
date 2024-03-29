package arc

import (
	"fmt"
	"strings"

	"github.com/Skyrin/go-lib/arc/model"
	"github.com/Skyrin/go-lib/arc/sqlmodel"
	"github.com/Skyrin/go-lib/e"
	"github.com/Skyrin/go-lib/sql"
	"github.com/rs/zerolog/log"
)

const (
	ECode040701 = e.Code0407 + "01"
	ECode040702 = e.Code0407 + "02"
	ECode040703 = e.Code0407 + "03"
)

type Deployment struct {
	DB        *sql.Connection
	Model     *model.Deployment
	Listener  *DeploymentNotify
	StoreCode string
}

// Refresh updates this objects properties from the corresponding record in
// the database
func (d *Deployment) Refresh() {
	newDep, err := sqlmodel.DeploymentGetByCode(d.DB, d.Model.Code)
	if err != nil {
		log.Error().Err(err).Msg("Deployment.Refresh.1")
	}

	d.Model = newDep
}

// UpdateGrant updates this deployment's database record with the new
// grant info (token/expiry and refresh token/expiry)
func (d *Deployment) UpdateGrant(g *Grant) (err error) {
	if err := sqlmodel.DeploymentUpdate(d.DB, d.Model.ID, &sqlmodel.DeploymentUpdateParam{
		Token:              &g.Token,
		TokenExpiry:        &g.TokenExpiry,
		RefreshToken:       &g.RefreshToken,
		RefreshTokenExpiry: &g.RefreshTokenExpiry,
	}); err != nil {
		return e.W(err, ECode040701)
	}

	return nil
}

// NewDeployment initializes a new deployment and returns it
func NewDeployment(db *sql.Connection, cp *sql.ConnParam, deploymentCode string) (d *Deployment, err error) {
	md, err := sqlmodel.DeploymentGetByCode(db, deploymentCode)
	if err != nil {
		return nil, e.W(err, ECode040702)
	}

	dn, err := NewDeploymentNotify(cp)
	if err != nil {
		return nil, e.W(err, ECode040703)
	}

	dn.Notify = func(deploymentCode string) {
		if d.Model.Code == deploymentCode {
			d.Refresh()
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
