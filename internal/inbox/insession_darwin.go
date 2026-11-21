//go:build darwin

package inbox

import "golang.org/x/sys/unix"

// platformParentPID returns the parent process id of pid, read from the kernel
// process table.
func platformParentPID(pid int) (int, error) {
	info, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return 0, err
	}
	return int(info.Eproc.Ppid), nil
}

// platformEnviron returns the environment of pid, read from kern.procargs2.
func platformEnviron(pid int) ([]string, error) {
	raw, err := unix.SysctlRaw("kern.procargs2", pid)
	if err != nil {
		return nil, err
	}
	return parseProcargs2(raw), nil
}
