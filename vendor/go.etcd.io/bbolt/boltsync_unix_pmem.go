// +build !windows,!plan9,!linux,!openbsd,pmem

package bbolt

// fdatasync flushes written data to a file descriptor.
func fdatasync(db *DB) error {
	return db.pw.Sync()
}
