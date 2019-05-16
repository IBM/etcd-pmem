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
*/
import "C"

import (
	"errors"
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
        b[i] = letterBytes[rand.Int63() % int64(len(letterBytes))]
    }
    return string(b)
}

// Pmemwriter structure just stores the buffer that would be written to pmem
type Pmemwriter struct {
	plp Pmemlogpool
}

// Newpmemwriter returns a new pmem writer
func Newpmemwriter() *Pmemwriter {
	return &Pmemwriter{
		plp: nil,
	}
}

// Print prints the log
func Print(plp Pmemlogpool) (b []byte) {
	len := C.pmemlog_tell(plp)
	ptr := C.malloc(C.size_t(len))
        defer C.free(unsafe.Pointer(ptr))
	C.logprint(plp, (*C.uchar)(ptr))
	return C.GoBytes(ptr, C.int(len))
}

// Close closes the logpool
func Close(plp Pmemlogpool) {
	C.pmemlog_close(plp)
}

// IsPmemTrue checks if a particular directory path is in pmem or not
func IsPmemTrue(dirpath string) (bool, error) {
	path := filepath.Join(filepath.Clean(dirpath), RandStringBytesRmndr(5))

	is_pmem := int(C.IsPmemTrue(C.CString(path)))
	err := os.Remove(path)
	if is_pmem == 0 {
		return false, err
	} 
	return true, err
}

// Seek gives the total bytes written in a particular file
func Seek(plp Pmemlogpool) int64 {
	return int64(C.pmemlog_tell(plp))
}

// Write writes len(b) bytes to the pmem buffer
func (p *Pmemwriter) Write(b []byte) (n int, err error) {
	ptr := C.malloc(C.size_t(len(b)))
	defer C.free(unsafe.Pointer(ptr))

	copy((*[1 << 24] byte)(ptr)[0:len(b)], b)
	cdata := C.CBytes(b)
	defer C.free(unsafe.Pointer(cdata))

	if p.plp != nil {
		if int(C.byteToString(p.plp, (*C.uchar)(cdata), C.size_t(len(string(b))))) < 0 {
			err = errors.New("Log could not be appended in pmem")
		}
	}
	return len(b), err
}

// InitiatePmemLogPool initiates a log pool
func (p *Pmemwriter) InitiatePmemLogPool(path string, poolSize int64) (err error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	plp := C.pmemlog_create(cpath, C.size_t(poolSize), C.uint(fileutil.PrivateFileMode))
	if plp == nil {
		plp = C.pmemlog_open(cpath)
	}
	if plp == nil {
		err = errors.New("Failed to open pmem file")
	}
	p.plp = plp
	return err
}

// GetLogPool fetches the the log pool
func (p *Pmemwriter) GetLogPool() (plp Pmemlogpool) {
	plp = p.plp
	return plp
}
