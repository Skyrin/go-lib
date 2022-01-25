package e

// Constants in here define error codes that are unique to a package/function.
// The first two characters define the package, within this repo, and the
// second two characters define the function within that package. Furthermore,
// when creating an error, the e.New func should be called, which will also
// take a two character unique id within the function.
//
// Valid values for the characters are: 0-9 and A-Z. Packages starting with a
// number should be reserved for packages within the go-lib repository. Other
// repository packages may use any code starting with a letter. Note, this does
// not guarantee uniqueness across all repos, but it is assumed that other
// repos will not include eachother. If they do, some extra checks should be
// taken to ensure unique error codes.

const (
	// package: migration
	Code0001 = "0001" // package:migration | migration/migration.go
	Code0002 = "0002" // package:migration | migration/migration_list.go
	Code0003 = "0003" // package:migration/sqlmodel | migration/sqlmodel/migration.go

	// package: sql
	Code0201 = "0201" // package:sql | sql/count.go
	Code0202 = "0202" // package:sql | sql/row.go
	Code0203 = "0203" // package:sql | sql/sql.go
	Code0204 = "0204" // package:sql | sql/status.go
	Code0205 = "0205" // package:sql | sql/txn.go
	Code0206 = "0206" // package:sql | sql/rows.go

	// package: process
	Code0301 = "0301" // package:process | process/process.go
	Code0302 = "0302" // package:sqlmodel | process/internal/sqlmodel/process.go
	Code0303 = "0303" // package:sqlmodel | process/internal/sqlmodel/process_run.go

	//package: arc
	Code0401 = "0401" // package:arc | arc/arc_client.go
	Code0402 = "0402" // package:arc | arc/arcimedes_user.go
	Code0403 = "0403" // package:arc | arc/cart_customer.go
	Code0404 = "0404" // package:arc | arc/core_user.go
	Code0405 = "0405" // package:arc | arc/deployment_data.go
	Code0406 = "0406" // package:arc | arc/deployment_notify.go
	Code0407 = "0407" // package:arc | arc/deployment.go
	Code0408 = "0408" // package:arc | arc/grant_login.go
	Code0409 = "0409" // package:arc | arc/grant.go
	Code040A = "040A" // package:arc | arc/response.go
	Code040B = "040B" // package:arc/sqlmodel | arc/sqlmodel/credential.go
	Code040C = "040C" // package:arc/sqlmodel | arc/sqlmodel/data.go
	Code040D = "040D" // package:arc/sqlmodel | arc/sqlmodel/deployment.go
	// Code040E = "040E"
	// Code040F = "040F"
	// Code040G = "040G"
	// Code040H = "040H"
	// Code040I = "040I"
	// Code040J = "040J"
	// Code040K = "040K"
	// Code040L = "040L"
	// Code040M = "040M"
	// Code040N = "040N"
	// Code040O = "040O"
	// Code040P = "040P"
	// Code040Q = "040Q"
	// Code040R = "040R"
	// Code040S = "040S"
	// Code040T = "040T"
	// Code040U = "040U"
	// Code040V = "040V"
	// Code040W = "040W"
	// Code040X = "040X"
	// Code040Y = "040Y"
	Code040Z = "040Z" // package:arc/sqlmodel | arc/sqlmodel/deployment_grant.go

	//package: algolia
	Code0501 = "0501"
	Code0502 = "0502"
	Code0503 = "0503"

	//package: sync
	Code0601 = "0601"
	Code0602 = "0602"
	Code0603 = "0603"
)
