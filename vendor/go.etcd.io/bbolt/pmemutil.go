// +build pmem

package bbolt

/*
#cgo CFLAGS: -g -Wall
#cgo LDFLAGS: -lpmemlog -lpmem
#include <sys/stat.h>
#include <endian.h>
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "libpmem.h"

typedef struct {
	unsigned char *pmemaddr;
	size_t mapped_len;
} mem_layout;

mem_layout *mmap_pmem(const char *path, size_t pmem_len) {
	unsigned char *pmemaddr;
	size_t mapped_len;
	int is_pmem;

	if ((pmemaddr = pmem_map_file(path, pmem_len, PMEM_FILE_CREATE,
			0666, &mapped_len, &is_pmem)) == NULL) {
		perror("pmem_map_file");
		exit(1);
	}
	mem_layout *ml = malloc(sizeof(mem_layout));
	ml->pmemaddr   = pmemaddr;
	ml->mapped_len = mapped_len;

	return ml;
}

unsigned char *get_pmemaddr(const mem_layout *ml) {
	return ml->pmemaddr;
}

size_t get_mapped_len(const mem_layout *ml) {
	return ml->mapped_len;
}

void write_at(const mem_layout *ml, const unsigned char *buf, size_t len, unsigned int offset) {
	pmem_memmove_nodrain(ml->pmemaddr + (size_t) offset, buf, len);
}

void munmap_pmem(const mem_layout *ml) {
	pmem_unmap(ml->pmemaddr, ml->mapped_len);
}
*/
import "C"
import (
	"unsafe"
)

type pmem_layout *C.mem_layout

func mmap_pmem(path string, sz int) (ml pmem_layout, data []byte, err error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	ml = C.mmap_pmem(cpath, C.size_t(sz))
	pmem_addr := C.get_pmemaddr(ml)

	// Slice memory layout
	var sl = struct {
		addr uintptr
		len  int
		cap  int
	}{uintptr(unsafe.Pointer(pmem_addr)), sz, sz}

	// Use unsafe to turn sl into a []byte.
	data = *(*[]byte)(unsafe.Pointer(&sl))

	return ml, data, nil
}

func munmap_pmem(ml pmem_layout) {
	C.munmap_pmem(ml)
}

type PmemWriter struct {
	ml pmem_layout
}

func setPmemWriter(db *DB) {
	pw := &PmemWriter{db.ml}
	db.pw = pw
}

func (pw *PmemWriter) WriteAt(b []byte, off int64) (n int, err error) {
	cdata := C.CBytes(b)
	defer C.free(unsafe.Pointer(cdata))

	C.write_at(pw.ml, (*C.uchar)(cdata), C.size_t(len(b)), C.uint(off))

	return len(b), nil
}

func (pw *PmemWriter) sync() (err error) {
	C.pmem_drain()
	return nil
}

