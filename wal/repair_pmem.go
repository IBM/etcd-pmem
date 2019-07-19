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

// +build pmem

package wal

import (
	"io"
	"os"
	"path/filepath"

	"go.etcd.io/etcd/pkg/fileutil"
	"go.etcd.io/etcd/pkg/pmemutil"
	"go.etcd.io/etcd/wal/walpb"

	"go.uber.org/zap"
)

// Repair tries to repair ErrUnexpectedEOF in the
// last wal file by truncating.
func Repair(lg *zap.Logger, dirpath string) bool {
	f, err := openLast(lg, dirpath)
	if err != nil {
		return false
	}
	defer f.Close()

	if lg != nil {
		lg.Info("repairing", zap.String("path", f.Name()))
	} else {
		plog.Noticef("repairing %v", f.Name())
	}

	rec := &walpb.Record{}

	// Check if the last file opened is in pmem
	pmemaware, err := pmemutil.IsPmemTrue(dirpath)
	if err != nil {
		return false
	}

	var decoder *decoder
	if pmemaware {
		pr := pmemutil.OpenForRead(f.Name())
		decoder = newDecoder(pr)
	} else {
		decoder = newDecoder(f)
	}

	for {
		lastOffset := decoder.lastOffset()
		err := decoder.decode(rec)
		switch err {
		case nil:
			// update crc of the decoder when necessary
			switch rec.Type {
			case crcType:
				crc := decoder.crc.Sum32()
				// current crc of decoder must match the crc of the record.
				// do no need to match 0 crc, since the decoder is a new one at this case.
				if crc != 0 && rec.Validate(crc) != nil {
					return false
				}
				decoder.updateCRC(rec.Crc)
			}
			continue

		case io.EOF:
			if lg != nil {
				lg.Info("repaired", zap.String("path", f.Name()), zap.Error(io.EOF))
			}
			return true

		case io.ErrUnexpectedEOF:
			var bf io.Writer
			if pmemaware {
				// Do nothing here
			} else {
				bf, bferr := os.Create(f.Name() + ".broken")
				if bferr != nil {
					if lg != nil {
						lg.Warn("failed to create backup file", zap.String("path", f.Name()+".broken"), zap.Error(bferr))
					} else {
						plog.Errorf("could not repair %v, failed to create backup file", f.Name())
					}
					return false
				}
				defer bf.Close()
			}

			if _, err = f.Seek(0, io.SeekStart); err != nil {
				if lg != nil {
					lg.Warn("failed to read file", zap.String("path", f.Name()), zap.Error(err))
				} else {
					plog.Errorf("could not repair %v, failed to read file", f.Name())
				}
				return false
			}

			if pmemaware {
				// TODO Throw error when copy is failing on pmem instead of abrupt termination
				pmemutil.Copy(f.Name(), f.Name()+".broken")
				/*if _, err = pmemutil.Copy(f.Name(), f.Name()+".broken"); err != nil {
					if lg != nil {
						lg.Warn("failed to copy in pmem", zap.String("from", f.Name()+".broken"), zap.String("to", f.Name()), zap.Error(err))
					} else {
						plog.Errorf("could not repair %v, failed to copy file in pmem", f.Name())
					}
					return false
				}*/
				if err = pmemutil.Resize(f.Name(), lastOffset); err != nil {
					if lg != nil {
						lg.Warn("failed to truncate pmem file", zap.String("path", f.Name()), zap.Error(err))
					} else {
						plog.Errorf("could not repair %v, failed to truncate pmem file", f.Name())
					}
					return false
				}
			} else {
				if _, err = io.Copy(bf, f); err != nil {
					if lg != nil {
						lg.Warn("failed to copy", zap.String("from", f.Name()+".broken"), zap.String("to", f.Name()), zap.Error(err))
					} else {
						plog.Errorf("could not repair %v, failed to copy file", f.Name())
					}
					return false
				}

				if err = f.Truncate(lastOffset); err != nil {
					if lg != nil {
						lg.Warn("failed to truncate", zap.String("path", f.Name()), zap.Error(err))
					} else {
						plog.Errorf("could not repair %v, failed to truncate file", f.Name())
					}
					return false
				}

				if err = fileutil.Fsync(f.File); err != nil {
					if lg != nil {
						lg.Warn("failed to fsync", zap.String("path", f.Name()), zap.Error(err))
					} else {
						plog.Errorf("could not repair %v, failed to sync file", f.Name())
					}
					return false
				}
			}

			if lg != nil {
				lg.Info("repaired", zap.String("path", f.Name()), zap.Error(io.ErrUnexpectedEOF))
			}
			return true

		default:
			if lg != nil {
				lg.Warn("failed to repair", zap.String("path", f.Name()), zap.Error(err))
			} else {
				plog.Errorf("could not repair error (%v)", err)
			}
			return false
		}
	}
}

// openLast opens the last wal file for read and write.
func openLast(lg *zap.Logger, dirpath string) (*fileutil.LockedFile, error) {
	names, err := readWALNames(lg, dirpath)
	if err != nil {
		return nil, err
	}
	last := filepath.Join(dirpath, names[len(names)-1])
	return fileutil.LockFile(last, os.O_RDWR, fileutil.PrivateFileMode)
}
