package arc

import (
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
	return d.getCoreServiceURL(HostAPI)
}

// getAPICoreServiceURL returns core service URL for arc manager domain
func (d *Deployment) getManageCoreServiceURL() string {
	return d.getCoreServiceURL(HostManager)
}

// getAPIArcimedesServiceURL returns arcimedes service URL for arc API domain
func (d *Deployment) getAPIArcimedesServiceURL() string {
	return d.getArcimedesServiceURL(HostAPI)
}

// getManageArcimedesServiceURL returns arcimedes service URL for arc manager domain
func (d *Deployment) getManageArcimedesServiceURL() string {
	return d.getArcimedesServiceURL(HostManager)
}

// getAPICartServiceURL returns cart service URL for the specified store code for arc API domain
func (d *Deployment) getAPICartServiceURL(storeCode string) string {
	return d.getCartServiceURL(HostAPI, storeCode)
}

// getManageCartServiceURL returns cart service URL for the specified store code for arc manager domain
func (d *Deployment) getManageCartServiceURL(storeCode string) string {
	return d.getCartServiceURL(HostManager, storeCode)
}

// getCoreServiceURL return core service url depending on host type
func (d *Deployment) getCoreServiceURL(host Host) string {
	var sb strings.Builder
	if host == HostManager {
		_, _ = sb.WriteString(d.Model.ManageURL)
	} else {
		_, _ = sb.WriteString(d.Model.APIURL)
	}

	_, _ = sb.WriteString(corePath)
	_, _ = sb.WriteString(servicesPath)
	return sb.String()
}

// getArcimedesServiceURL return arcimedes service url depending on host type
func (d *Deployment) getArcimedesServiceURL(host Host) string {
	var sb strings.Builder
	if host == HostManager {
		_, _ = sb.WriteString(d.Model.ManageURL)
	} else {
		_, _ = sb.WriteString(d.Model.APIURL)
	}

	_, _ = sb.WriteString(arcimedesPath)
	_, _ = sb.WriteString(servicesPath)
	return sb.String()
}

// getCartServiceURL return cart service url depending on host type
func (d *Deployment) getCartServiceURL(host Host, storeCode string) string {
	var sb strings.Builder
	if host == HostManager {
		_, _ = sb.WriteString(d.Model.ManageURL)
	} else {
		_, _ = sb.WriteString(d.Model.APIURL)
	}

	_, _ = sb.WriteString(cartPath)
	_, _ = sb.WriteString(storeCode)
	_ = sb.WriteByte('/')
	_, _ = sb.WriteString(servicesPath)
	return sb.String()
}
