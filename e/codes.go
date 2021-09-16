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
	// Code0100 = "0100"

	// package: sql
	// Code0200 = "0200"

	//package: http
	// Code0300 = "0300"

	//package: arc
	Code0400 = "0400"
)
