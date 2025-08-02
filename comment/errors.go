/*
Copyright 2025 Nemo(shengyi) Lv

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package comment

import (
	"errors"
	"io"
)

var (
	ErrKeyIsEmpty            = errors.New("key is empty")
	ErrKeyNotFound           = errors.New("key not found")
	ErrDataFileNotFound      = errors.New("data file not found")
	ErrDataDirectoryCorrupt  = errors.New("database directory maybe corrupt")
	ErrMergeIsProgress       = errors.New("merge is progress, try again later")
	ErrDatabaseIsUsing       = errors.New("the database directory is using by another process")
	ErrInvalidMergeRatio     = errors.New("invalid merge ratio, must between 0 and 1")
	ErrMergeRatioUnreached   = errors.New("the merge ratio do not reach the option")
	ErrNoEnoughSpaceForMerge = errors.New("no enough disk space for merge")
	ErrNotMergeFinishedFile  = errors.New("not merge finished file")
)

var (
	ErrDirPathIsEmpty         = errors.New("database dir path is empty")
	ErrFileSizeIsLessThanZero = errors.New("file size must be greater than zero")
)

var (
	// ErrCannotCommitAClosedTx is returned when the tx committing a closed tx
	ErrCannotCommitAClosedTx = errors.New("can not commit a closed tx")

	// ErrCannotRollbackACommittingTx is returned when the tx rollback a committing tx
	ErrCannotRollbackACommittingTx = errors.New("can not rollback a committing tx")

	// ErrCannotRollbackAClosedTx is returned when the tx rollback a closed tx
	ErrCannotRollbackAClosedTx = errors.New("can not rollback a closed tx")

	// ErrDBClosed is returned when database is closed.
	ErrDBClosed = errors.New("database is closed")

	ErrTxClosed = errors.New("tx is closed")

	ErrExceedMaxBatchNum = errors.New("ErrExceedMaxBatchNum")
)

var (
	ErrInvalidCRC = errors.New("invalid CRC value, datafile maybe corrupted")
	EOF           = io.EOF
)
