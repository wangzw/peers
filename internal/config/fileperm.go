package config

import "io/fs"

func rootOnlyWritableFor(uid uint32, mode fs.FileMode) bool {
	return uid == 0 && mode.Perm()&0o022 == 0
}
