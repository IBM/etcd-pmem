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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "libpmem.h"
#include <libpmemlog.h>

// size of the pmemlog pool -- 1 GB
#define POOL_SIZE ((size_t)(1 << 30))

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
*/
import "C"

import (
	"errors"
	"unsafe"

	"go.etcd.io/etcd/pkg/fileutil"
)

type Pmemlogpool *C.PMEMlogpool

type Pmem struct {
	Plp Pmemlogpool
	Size int
}

// Pmemwriter structure just stores the buffer that would be written to pmem
type Pmemwriter struct {
	pmem Pmem
}

// Newpmemwriter returns a new pmem writer
func Newpmemwriter() *Pmemwriter {
	return &Pmemwriter{
		pmem: Pmem{
			Plp: nil,
			Size: 0,
		},
	}
}

// Print prints the log
func Print(pmem *Pmem) (b []byte) {
	len := C.int(pmem.Size)
	ptr := C.malloc(C.size_t(len))
        defer C.free(unsafe.Pointer(ptr))
	C.logprint(pmem.Plp, (*C.uchar)(ptr))
	return C.GoBytes(ptr, len)
}

// Close closes the logpool
func Close(plp Pmemlogpool) {
	C.pmemlog_close(plp)
}

// Write writes len(b) bytes to the pmem buffer
func (p *Pmemwriter) Write(b []byte) (n int, err error) {
	ptr := C.malloc(C.size_t(len(b)))
	defer C.free(unsafe.Pointer(ptr))

	copy((*[1 << 24] byte)(ptr)[0:len(b)], b)
	cdata := C.CBytes(b)
	defer C.free(unsafe.Pointer(cdata))

	if p.pmem.Plp != nil {
		if int(C.byteToString(p.pmem.Plp, (*C.uchar)(cdata), C.size_t(len(string(b))))) < 0 {
			err = errors.New("Log could not be appended in pmem")
		}
	}
	// Update total size of log
	p.pmem.Size = p.pmem.Size + len(b)
	return len(b), err
}

// InitiatePmemLogPool initiates a log pool
func (p *Pmemwriter) InitiatePmemLogPool(path string) (err error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	plp := C.pmemlog_create(cpath, C.POOL_SIZE, C.uint(fileutil.PrivateFileMode))
	if plp == nil {
		plp = C.pmemlog_open(cpath)
	}
	if plp == nil {
		err = errors.New("Failed to open pmem file")
	}
	p.pmem.Plp = plp
	return err
}

// GetLogPool fetches the the log pool
func (p *Pmemwriter) GetLogPool() (pmem *Pmem) {
	pmem = &p.pmem
	return pmem
}
