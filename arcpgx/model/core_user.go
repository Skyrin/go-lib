package model

const (
	CoreUserTypeApi         = "api"
	CoreUserTypeApp         = "app"
	CoreUserTypeApplication = "application"
	CoreUserTypeGuest       = "guest"
	CoreUserTypeSuper       = "super"
)

// CoreUser model
type CoreUser struct {
	ID        int        `json:"id"`
	AppCode   string     `json:"__appCd"`
	AppCoreID int        `json:"__appCoreId"`
	Username  string     `json:"username"`
	Email     string     `json:"email"`
	Type      string     `json:"typeCode"`
	Person    CorePerson `json:"person"`
}
