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

	// package: migrationpgx
	Code0101 = "0101" // package:migrationpgx | migration/migration.go
	Code0102 = "0102" // package:migrationpgx | migration/migration_list.go
	Code0103 = "0103" // package:migrationpgx/sqlmodel | migration/sqlmodel/migration.go

	// package: sql
	Code0201 = "0201" // package:sql | sql/count.go
	Code0202 = "0202" // package:sql | sql/row.go
	Code0203 = "0203" // package:sql | sql/sql.go
	Code0204 = "0204" // package:sql | sql/status.go
	Code0205 = "0205" // package:sql | sql/txn.go
	Code0206 = "0206" // package:sql | sql/rows.go
	Code0207 = "0207" // package:sql | sql/bulk.go
	Code0208 = "0208" // package:sql | sql/statement.go
	Code0209 = "0209" // package:sql | sql/bulk_update.go

	// package: process
	Code0301 = "0301" // package:process | process/process.go
	Code0302 = "0302" // package:sqlmodel | process/internal/sqlmodel/process.go
	Code0303 = "0303" // package:sqlmodel | process/internal/sqlmodel/process_run.go
	Code0304 = "0304" // package:process | process/queue.go

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
	Code040E = "040E"
	Code040F = "040F"
	Code040G = "040G"
	Code040H = "040H"
	Code040I = "040I"
	Code040J = "040J"
	Code040K = "040K"
	Code040L = "040L"
	Code040M = "040M"
	Code040N = "040N"
	Code040O = "040O"
	Code040P = "040P"
	Code040Q = "040Q"
	Code040R = "040R"
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

	//package: pubsub
	Code0701 = "0701" // package pubsub | pubsub/publish.go
	Code0702 = "0702" // package pubsub | pubsub/subscriber.go
	Code0703 = "0703" // package pubsub | pubsub/subscriber_notify.go
	Code0704 = "0704" // package pubsub | pubsub/subscriber_process.go
	Code0705 = "0705" // package pubsub/sqlmodel | pubsub/internal/sub_data_bulk.go
	Code0706 = "0706" // package pubsub/sqlmodel | pubsub/internal/sqlmodel/pub.go
	Code0707 = "0707" // package pubsub/sqlmodel | pubsub/internal/sqlmodel/sub.go
	Code0708 = "0708" // package pubsub/sqlmodel | pubsub/internal/sqlmodel/data.go
	Code0709 = "0709" // package pubsub/sqlmodel | pubsub/internal/sqlmodel/sub_data.go
	Code070A = "070A" // package pubsub | pubsub/subscriber_batch.go
	Code070B = "070B" // package pubsub | pubsub/subscriber_batch_queue.go
	Code070C = "070C" // package pubsub | pubsub/publisher.go
	Code070D = "070D" // package pubsub | pubsub/publish_batch.go

	//package: kafka_aws_ec2
	Code0800 = "0800" // package kafka | kafka/connection.go
	Code0801 = "0801" // package kafka_aws_ec2 | kafka/aws/ec2/sasl.go

	// package: sqlpgx
	Code0901 = "0901" // package:sqlpgx | sqlpgx/count.go
	Code0902 = "0902" // package:sqlpgx | sqlpgx/row.go
	Code0903 = "0903" // package:sqlpgx | sqlpgx/sql.go
	Code0904 = "0904" // package:sqlpgx | sqlpgx/status.go
	Code0905 = "0905" // package:sqlpgx | sqlpgx/txn.go
	Code0906 = "0906" // package:sqlpgx | sqlpgx/rows.go
	Code0907 = "0907" // package:sqlpgx | sqlpgx/bulk.go
	Code0908 = "0908" // package:sqlpgx | sqlpgx/statement.go
	Code0909 = "0909" // package:sqlpgx | sqlpgx/bulk_update.go

	// package: processpgx
	Code0A01 = "0A01" // package:processpgx | processpgx/process.go
	Code0A02 = "0A02" // package:sqlmodel | processpgx/internal/sqlmodel/process.go
	Code0A03 = "0A03" // package:sqlmodel | processpgx/internal/sqlmodel/process_run.go
	Code0A04 = "0A04" // package:processpgx | processpgx/queue.go
)
