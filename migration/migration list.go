package migration

import (
	"embed"
	"sort"
	"strconv"
	"strings"

	"github.com/Skyrin/go-lib/e"
)

const (
	ECode000201 = e.Code0002 + "01"
	ECode000202 = e.Code0002 + "02"
	ECode000203 = e.Code0002 + "03"
	ECode000204 = e.Code0002 + "04"
	ECode000205 = e.Code0002 + "05"
	ECode000206 = e.Code0002 + "06"
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
		return 0, e.WWM(nil, ECode000201, e.MsgMigrationFileNameInvalid)
	}

	if len(sList) == 1 {
		sList = strings.Split(f.Name, ".")
	}

	v, err = strconv.Atoi(sList[0])
	if err != nil {
		return 0, e.WWM(err, ECode000202, e.MsgMigrationFileNameInvalid)
	}

	if v <= 0 {
		return 0, e.WWM(err, ECode000203, e.MsgMigrationFileNameInvalid)
	}

	return v, nil
}

// GetLatestMigrationFiles gets all migration files after the specified version from
// the migration list's embeded file system
func (l List) GetLatestMigrationFiles(v int) (fList []*File, err error) {

	dirList, err := l.migrations.ReadDir(l.path)
	if err != nil {
		return nil, e.W(err, ECode000204)
	}
	fList = make([]*File, 0, len(dirList))

	// Load files first, then sort according to version
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
			return nil, e.W(err, ECode000205)
		}

		// TODO: ensure incremental versions?
		// If the file version is less than the get from version, then move to the next one
		if f.Version < v {
			continue
		}

		// Should be a file we are looking for
		f.SQL, err = l.migrations.ReadFile(embededFilePath)
		if err != nil {
			return nil, e.W(err, ECode000206)
		}

		fList = append(fList, f)
	}

	// Sort files by version ascending
	sort.Slice(fList, func(i, j int) bool {
		return fList[i].Version < fList[j].Version
	})

	return fList, nil
}
