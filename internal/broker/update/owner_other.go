//go:build !unix

package update

import "os"

func chownFile(*os.File, os.FileInfo) error {
	return nil
}
