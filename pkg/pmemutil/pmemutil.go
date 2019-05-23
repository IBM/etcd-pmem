// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package pmemutil

/*
#cgo CFLAGS: -g -Wall
#cgo LDFLAGS: -lpmemlog -lpmem
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "libpmem.h"
#include <libpmemlog.h>

// size of the pmemlog pool -- 64MB
#define	PMEM_LEN 4096

int byteToString(PMEMlogpool *plp, const unsigned char *buf, size_t len) {
	return pmemlog_append(plp, buf, len);
}

static int
printit(const void *buf, size_t len, void *arg)
{
	memcpy(arg, buf, len);
	return 0;
}

void logprint(PMEMlogpool *plp, unsigned char *out) {
	pmemlog_walk(plp, 0, printit, out);
}

int IsPmemTrue(char *path) {
	char *pmemaddr;
	size_t mapped_len;
	int is_pmem;

	if ((pmemaddr = pmem_map_file(path, PMEM_LEN,
				PMEM_FILE_CREATE|PMEM_FILE_EXCL,
				0666, &mapped_len, &is_pmem)) == NULL) {
		perror("Error - pmem_map_file");
		exit(1);
	}

	pmem_unmap(pmemaddr, mapped_len);
	return is_pmem;
}

PMEMlogpool *pmemlogOpen(char *path) {
	PMEMlogpool *plp;
	plp = pmemlog_open(path);
	if (plp == NULL) {
		perror(path);
		exit(1);
	}
	return plp;
}

PMEMlogpool *pmemlogCreateOrOpen(char *path, size_t poolSize, unsigned int mode) {
	PMEMlogpool *plp;
	plp= pmemlog_create(path, poolSize, mode);
	if (plp == NULL) {
		perror(path);
		plp = pmemlog_open(path);
	}
	if (plp == NULL) {
		perror(path);
	}
	return plp;
}
*/
import "C"

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"unsafe"

	"go.etcd.io/etcd/pkg/fileutil"
)

type Pmemlogpool *C.PMEMlogpool

const letterBytes = "abcdefghijklmnopqrstuvwxyz"

// RandStringBytesRmndr generates random string that is required for random filename
func RandStringBytesRmndr(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

// IsPmemTrue checks if a particular directory path is in pmem or not
func IsPmemTrue(dirpath string) (bool, error) {
	path := filepath.Join(filepath.Clean(dirpath), RandStringBytesRmndr(5))

	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	isPmem := int(C.IsPmemTrue(cpath))
	err := os.Remove(path)
	if isPmem == 0 {
		return false, err
	}
	return true, err
}

// InitiatePmemLogPool initiates a log pool
func InitiatePmemLogPool(path string, poolSize int64) (err error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	plp := C.pmemlogCreateOrOpen(cpath, C.size_t(poolSize), C.uint(fileutil.PrivateFileMode))
	if plp == nil {
		err = errors.New("Failed to open pmem file")
	}
	defer func() {
		if plp != nil {
			Close(plp)
		}
	}()

	return err
}

// Seek gives the total bytes written in a particular file
func Seek(plp Pmemlogpool) int64 {
	defer Close(plp)
	return int64(C.pmemlog_tell(plp))
}

// Close closes the logpool
func Close(plp Pmemlogpool) {
	C.pmemlog_close(plp)
}

// Pmemwriter structure just stores the buffer that would be written to pmem
type Pmemwriter struct {
	path string
}

// OpenForWrite returns the Pmem writer
func OpenForWrite(path string) (pw *Pmemwriter) {
	pw = &Pmemwriter{
		path: path,
	}
	return pw
}

// GetLogPool fetches the the log pool. Make sure you close this just after using the log pool.
func (pw *Pmemwriter) GetLogPool() (plp Pmemlogpool, err error) {
	cpath := C.CString(pw.path)
	defer C.free(unsafe.Pointer(cpath))

	plp = C.pmemlogOpen(cpath)
	if plp == nil {
		err = errors.New("Failed to open pmem file")
	}
	return plp, err
}

// Write writes len(b) bytes to the pmem buffer
func (pw *Pmemwriter) Write(b []byte) (n int, err error) {
	cpath := C.CString(pw.path)
	defer C.free(unsafe.Pointer(cpath))

	plp := C.pmemlogOpen(cpath)
	if plp == nil {
		err = errors.New("Failed to open pmem file for write")
	}
	defer Close(plp)

	ptr := C.malloc(C.size_t(len(b)))
	defer C.free(unsafe.Pointer(ptr))

	copy((*[1 << 24]byte)(ptr)[0:len(b)], b)
	cdata := C.CBytes(b)
	defer C.free(unsafe.Pointer(cdata))

	if plp != nil {
		if int(C.byteToString(plp, (*C.uchar)(cdata), C.size_t(len(string(b))))) < 0 {
			err = errors.New("Log could not be appended in pmem")
		}
	}
	return len(b), err
}

// Pmemreader implements buffering for io.Reader object
type Pmemreader struct {
	path string
}

// OpenForRead opens a pmemlog from a path and returns the Pmem reader
func OpenForRead(path string) (pr *Pmemreader) {
	pr = &Pmemreader{
		path: path,
	}
	return pr
}

// Print prints the log
/* func Print(plp Pmemlogpool) (b []byte) {
	len := C.pmemlog_tell(plp)
	ptr := C.malloc(C.size_t(len))
	defer C.free(unsafe.Pointer(ptr))
	C.logprint(plp, (*C.uchar)(ptr))
	return C.GoBytes(ptr, C.int(len))
} */

// GetLogPool fetches the the log pool. Make sure you close this just after using the log pool.
func (pr *Pmemreader) GetLogPool() (plp Pmemlogpool, err error) {
	cpath := C.CString(pr.path)
	defer C.free(unsafe.Pointer(cpath))

	plp = C.pmemlogOpen(cpath)
	if plp == nil {
		err = errors.New("Failed to open pmem file")
	}
	return plp, err
}

// Reader reads data into b
func (pr *Pmemreader) Read(b []byte) (n int, err error) {
	cpath := C.CString(pr.path)
	defer C.free(unsafe.Pointer(cpath))

	plp := C.pmemlogOpen(cpath)
	if plp == nil {
		err = errors.New("Failed to open pmem file for read")
	}
	defer Close(plp)

	length := C.pmemlog_tell(plp)

	ptr := C.malloc(C.size_t(length))
	defer C.free(unsafe.Pointer(ptr))

	C.logprint(plp, (*C.uchar)(ptr))
	fmt.Println("The length in pmemutil read is:", length)
	return copy(b, C.GoBytes(ptr, C.int(length))), nil
}

// Close is a placeholder
func (pr *Pmemreader) Close() (err error) {
	return nil
}
