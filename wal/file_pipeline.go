// Copyright 2016 The etcd Authors
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

package wal

import (
	"fmt"
	"os"
	"path/filepath"

	"go.etcd.io/etcd/pkg/fileutil"
	"go.etcd.io/etcd/pkg/pmemutil"

	"go.uber.org/zap"
)

// filePipeline pipelines allocating disk space
type filePipeline struct {
	lg *zap.Logger

	// dir to put files
	dir string
	// size of files to make, in bytes
	size int64
	// count number of files generated
	count      int
	is_pmem    bool
	plp        pmemutil.Pmemlogpool
	pmemwriter *pmemutil.Pmemwriter

	filec chan *fileutil.LockedFile
	errc  chan error
	donec chan struct{}
}

func newFilePipeline(lg *zap.Logger, dir string, fileSize int64) *filePipeline {
	// Check if the current location is in pmem
	is_pmem := false
	is_pmem, _ = pmemutil.IsPmemTrue(dir)
	is_pmem = true
	/*if err != nil {
		return nil, errors.New("Temporary file in pmem could not be removed")
	}*/

	fp := &filePipeline{
		lg:      lg,
		dir:     dir,
		is_pmem: is_pmem,
		size:    fileSize,
		filec:   make(chan *fileutil.LockedFile),
		errc:    make(chan error, 1),
		donec:   make(chan struct{}),
	}
	go fp.run()
	return fp
}

// Open returns a fresh file for writing. Rename the file before calling
// Open again or there will be file collisions.
func (fp *filePipeline) Open() (f *fileutil.LockedFile, err error) {
	select {
	case f = <-fp.filec:
	case err = <-fp.errc:
	}
	return f, err
}

func (fp *filePipeline) Close() error {
	close(fp.donec)
	return <-fp.errc
}

func (fp *filePipeline) alloc() (f *fileutil.LockedFile, err error) {

	// count % 2 so this file isn't the same as the one last published
	fpath := filepath.Join(fp.dir, fmt.Sprintf("%d.tmp", fp.count%2))

	if !fp.is_pmem {
		if f, err = fileutil.LockFile(fpath, os.O_CREATE|os.O_WRONLY, fileutil.PrivateFileMode); err != nil {
			return nil, err
		}
		if err = fileutil.Preallocate(f.File, fp.size, true); err != nil {
			if fp.lg != nil {
				fp.lg.Warn("failed to preallocate space when creating a new WAL", zap.Int64("size", fp.size), zap.Error(err))
			} else {
				plog.Errorf("failed to allocate space when creating new wal file (%v)", err)
			}
			f.Close()
			return nil, err
		}
	} else {
		fp.pmemwriter = pmemutil.Newpmemwriter()
		err = fp.pmemwriter.InitiatePmemLogPool(fpath, fp.size)
		if err != nil {
			if fp.lg != nil {
				fp.lg.Warn(
					"failed to create an initial WAL file in pmem",
					zap.String("path", fpath),
					zap.Error(err),
				)
			}
			return nil, err
		}
		fp.plp = fp.pmemwriter.GetLogPool()
		//fp.pmemwriter = pw
		// TODO Very hacky way - the file is probably locked twice, must be fixed
		f, err = fileutil.LockFile(fpath, os.O_RDWR, fileutil.PrivateFileMode)
		if err != nil {
			if fp.lg != nil {
				fp.lg.Warn(
					"failed to flock an initial WAL file",
					zap.String("path", fpath),
					zap.Error(err),
				)
			}
			return nil, err
		}
	}
	fp.count++
	return f, nil
}

func (fp *filePipeline) run() {
	defer close(fp.errc)
	for {
		f, err := fp.alloc()
		if err != nil {
			fp.errc <- err
			return
		}
		select {
		case fp.filec <- f:
		case <-fp.donec:
			os.Remove(f.Name())
			f.Close()
			return
		}
	}
}
