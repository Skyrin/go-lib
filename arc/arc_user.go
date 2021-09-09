package arc

// ArcUser model
type ArcUser struct {
	ID         int        `json:"id"`
	ArcUserID  int        `json:"-"`
	Username   string     `json:"username"`
	Email      string     `json:"email"`
	Password   string     `json:"-"`
	FirstName  string     `json:"-"`
	MiddleName string     `json:"-"`
	LastName   string     `json:"-"`
	Type       string     `json:"typeCode"`
	Person     CorePerson `json:"person"`
}

type CorePerson struct {
	FirstName  string `json:"firstName"`
	MiddleName string `json:"middleName"`
	LastName   string `json:"lastName"`
}
