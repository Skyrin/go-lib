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
	Code0001 = "0001"
	Code0002 = "0002"
	Code0003 = "0003"
	Code0004 = "0004"
	Code0005 = "0005"
	Code0006 = "0006"
	Code0007 = "0007"
	Code0008 = "0008"

	// package: migration/sqlmodel
	Code0101 = "0101"
	Code0102 = "0102"
	Code0103 = "0103"
	Code0104 = "0104"
	Code0105 = "0105"

	// package: sql
	Code0201 = "0201"
	Code0202 = "0202"
	Code0203 = "0203"
	Code0204 = "0204"
	Code0205 = "0205"
	Code0206 = "0206"
	Code0207 = "0207"
	Code0208 = "0208"
	Code0209 = "0209"
	Code020A = "020A"
	Code020B = "020B"
	Code020C = "020C"
	Code020D = "020D"
	Code020E = "020E"
	Code020F = "020F"
	Code020G = "020G"
	Code020H = "020H"
	Code020I = "020I"
	Code020J = "020J"
	Code020K = "020K"
	Code020L = "020L"
	Code020M = "020M"
	Code020N = "020N"
	Code020O = "020O"

	//package: http
	// Code0301 = "0301"

	//package: arc
	Code0401 = "0401"
	Code0402 = "0402"
	Code0403 = "0403"
	Code0404 = "0404"
	Code0405 = "0405"
	Code0406 = "0406"
	Code0407 = "0407"
	Code0408 = "0408"
	Code0409 = "0409"
	Code040A = "040A"
	Code040B = "040B"
	Code040C = "040C"
	Code040D = "040D"
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
	Code040S = "040S"
	Code040T = "040T"
	Code040U = "040U"
	Code040V = "040V"
	Code040W = "040W"
	Code040X = "040X"
	Code040Y = "040Y"
	Code040Z = "040Z"
	Code0410 = "0410"

	//package: algolia
	Code0501 = "0501"
	Code0502 = "0502"
	Code0503 = "0503"
	Code0504 = "0504"
	Code0505 = "0505"
	Code0506 = "0506"
	Code0507 = "0507"
	Code0508 = "0508"
	Code0509 = "0509"
	Code050A = "050A"
	Code050B = "050B"
	Code050C = "050C"
	Code050D = "050D"
	Code050E = "050E"
	Code050F = "050F"
)
