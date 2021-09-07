package migration

import (
	"embed"
	"fmt"
	"strconv"
	"strings"

	"github.com/Skyrin/go-lib/errors"
	"github.com/Skyrin/go-lib/migration/model"
)

type File struct {
	Name    string
	Version int
	SQL     []byte
}

type List struct {
	code       string
	path       string
	migrations embed.FS
	files      []*File
	new        bool
	// GetMigrations() (string, embed.FS)
}

// NewList initialize a new list
func NewList(code, path string, migrations embed.FS) (l *List) {
	return &List{
		code:       code,
		path:       path,
		migrations: migrations,
	}
}

// GetVersionFromName parse the name for the version. The name is expected to have
// the version first as a 0 padded number and then an underscore. The rest of the
// name can be anything.
func (f *File) GetVersionFromName() (v int, err error) {
	sList := strings.Split(f.Name, "_")
	if len(sList) == 0 {
		return 0, fmt.Errorf(model.ErrMigrationFileNameInvalid)
	}

	v, err = strconv.Atoi(sList[0])
	if err != nil {
		return 0, errors.Wrap(err, "GetVersionFromName.1", model.ErrMigrationFileNameInvalid)
	}

	if v <= 0 {
		return 0, errors.Wrap(err, "GetVersionFromName.2", model.ErrMigrationFileNameInvalid)
	}

	return v, nil
}

// GetLatestMigrationFiles gets all migration files after the specified version from
// the migration list's embeded file system
func (l List) GetLatestMigrationFiles(v int) (fList []*File, err error) {

	dirList, err := l.migrations.ReadDir(l.path)
	if err != nil {
		return nil, errors.Wrap(err, "GetLatestMigrationFiles.1", "")
	}
	fList = make([]*File, 0, len(dirList))

	for _, file := range dirList {
		if file.IsDir() {
			continue
		}
		// Get version
		f := &File{
			Name: file.Name(),
		}

		embededFilePath := strings.Join([]string{
			l.path,
			file.Name(),
		}, "/")

		f.Version, err = f.GetVersionFromName()
		if err != nil {
			return nil, errors.Wrap(err, "GetLatestMigrationFiles.2", "")
		}

		// TODO: ensure incremental versions?
		// If the file version is less than the get from version, then move to the next one
		if f.Version < v {
			continue
		}

		// Should be a file we are looking for
		f.SQL, err = l.migrations.ReadFile(embededFilePath)
		if err != nil {
			return nil, errors.Wrap(err, "GetLatestMigrationFiles.3", "")
		}

		fList = append(fList, f)
	}

	return fList, nil
}
