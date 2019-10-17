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

// +build pmem

package wal

import (
	"fmt"
	"os"
	"path/filepath"

	"go.etcd.io/etcd/pkg/fileutil"
	"go.etcd.io/etcd/pkg/pmemutil"

	"go.uber.org/zap"
)

// pmemCollection encapsulates LockedFile and Pmemlogpool
type PmemCollection struct {
	plp pmemutil.Pmemlogpool
	l   fileutil.LockedFile
}

// filePipeline pipelines allocating disk space
type filePipeline struct {
	lg *zap.Logger

	// dir to put files
	dir string
	// size of files to make, in bytes
	size int64
	// count number of files generated
	count  int

	filec chan *PmemCollection
	errc  chan error
	donec chan struct{}
}

func newFilePipeline(lg *zap.Logger, dir string, fileSize int64) *filePipeline {
	fp := &filePipeline{
		lg:     lg,
		dir:    dir,
		size:   fileSize,
		filec:  make(chan *PmemCollection),
		errc:   make(chan error, 1),
		donec:  make(chan struct{}),
	}
	go fp.run()
	return fp
}

// Open returns a fresh file for writing. Rename the file before calling
// Open again or there will be file collisions.
func (fp *filePipeline) Open() (f *PmemCollection, err error) {
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

func (fp *filePipeline) alloc() (p *PmemCollection, err error) {

	// count % 2 so this file isn't the same as the one last published
	fpath := filepath.Join(fp.dir, fmt.Sprintf("%d.tmp", fp.count%2))

	var plp pmemutil.Pmemlogpool
		if !fileutil.Exist(fpath) {
			plp, err = pmemutil.InitiatePmemLogPool(fpath, fp.size)
		if err != nil {
			if fp.lg != nil {
				fp.lg.Warn(
					"failed to create an new WAL file in pmem",
					zap.String("path", fpath),
					zap.Error(err),
				)
			}
			return nil, err
		    }
                }

		// TODO Very hacky way - the file is probably locked twice, must be fixed
		f, err := fileutil.LockFile(fpath, os.O_WRONLY, fileutil.PrivateFileMode)
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

	fp.count++

	p = &PmemCollection{
		plp: plp,
		l: *f,
	}
	return p, nil
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
			os.Remove(f.l.Name())
				f.l.Close()
			return
		}
	}
}
