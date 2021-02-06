package lib

var (
	// Used for compile time versioning - to set properly, ensure to run
	// the go install/build command like the following:
	// go install -ldflags "-X github.com/Skyrin/go-lib/lib.version=local -X github.com/Skyrin/go-lib/lib.build=infinite"

	// Sha the commit sha
	Sha string
	// Build the build number
	Build string
)

// Version returns the version/build
// path to this in order for it to be usable.
func Version() (string, string) {
	return Sha, Build
}
