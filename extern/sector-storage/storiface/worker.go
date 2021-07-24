package storiface

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ipfs/go-cid"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/extern/sector-storage/sealtasks"
)

type WorkerInfo struct {
	Hostname string

	// IgnoreResources indicates whether the worker's available resources should
	// be used ignored (true) or used (false) for the purposes of scheduling and
	// task assignment. Only supported on local workers. Used for testing.
	// Default should be false (zero value, i.e. resources taken into account).
	IgnoreResources bool
	// Resources       WorkerResources
	Resources       WorkerResources
	TaskResourcesLk sync.Mutex
	TaskNumber      TaskConfig //限制进程PC1	数量

}

type WorkerResources struct {
	MemPhysical uint64
	MemSwap     uint64

	MemReserved uint64 // Used by system / other processes

	CPUs uint64 // Logical cores
	GPUs []string
}

type WorkerStats struct {
	Info    WorkerInfo
	Enabled bool

	MemUsedMin uint64
	MemUsedMax uint64
	GpuUsed    bool   // nolint
	CpuUse     uint64 // nolint
}

const (
	RWRetWait  = -1
	RWReturned = -2
	RWRetDone  = -3
)

type WorkerJob struct {
	ID     CallID
	Sector abi.SectorID
	Task   sealtasks.TaskType

	// 1+ - assigned
	// 0  - running
	// -1 - ret-wait
	// -2 - returned
	// -3 - ret-done
	RunWait int
	Start   time.Time

	Hostname string `json:",omitempty"` // optional, set for ret-wait jobs
}

type CallID struct {
	Sector abi.SectorID
	ID     uuid.UUID
}

func (c CallID) String() string {
	return fmt.Sprintf("%d-%d-%s", c.Sector.Miner, c.Sector.Number, c.ID)
}

var _ fmt.Stringer = &CallID{}

var UndefCall CallID

type WorkerCalls interface {
	AddPiece(ctx context.Context, sector storage.SectorRef, pieceSizes []abi.UnpaddedPieceSize, newPieceSize abi.UnpaddedPieceSize, pieceData storage.Data) (CallID, error)
	SealPreCommit1(ctx context.Context, sector storage.SectorRef, ticket abi.SealRandomness, pieces []abi.PieceInfo) (CallID, error)
	SealPreCommit2(ctx context.Context, sector storage.SectorRef, pc1o storage.PreCommit1Out) (CallID, error)
	SealCommit1(ctx context.Context, sector storage.SectorRef, ticket abi.SealRandomness, seed abi.InteractiveSealRandomness, pieces []abi.PieceInfo, cids storage.SectorCids) (CallID, error)
	SealCommit2(ctx context.Context, sector storage.SectorRef, c1o storage.Commit1Out) (CallID, error)
	FinalizeSector(ctx context.Context, sector storage.SectorRef, keepUnsealed []storage.Range) (CallID, error)
	ReleaseUnsealed(ctx context.Context, sector storage.SectorRef, safeToFree []storage.Range) (CallID, error)
	MoveStorage(ctx context.Context, sector storage.SectorRef, types SectorFileType) (CallID, error)
	UnsealPiece(context.Context, storage.SectorRef, UnpaddedByteIndex, abi.UnpaddedPieceSize, abi.SealRandomness, cid.Cid) (CallID, error)
	Fetch(context.Context, storage.SectorRef, SectorFileType, PathType, AcquireMode) (CallID, error)
}

type ErrorCode int

const (
	ErrUnknown ErrorCode = iota
)

const (
	// Temp Errors
	ErrTempUnknown ErrorCode = iota + 100
	ErrTempWorkerRestart
	ErrTempAllocateSpace
)

type CallError struct {
	Code    ErrorCode
	Message string
	sub     error
}

func (c *CallError) Error() string {
	return fmt.Sprintf("storage call error %d: %s", c.Code, c.Message)
}

func (c *CallError) Unwrap() error {
	if c.sub != nil {
		return c.sub
	}

	return errors.New(c.Message)
}

func Err(code ErrorCode, sub error) *CallError {
	return &CallError{
		Code:    code,
		Message: sub.Error(),

		sub: sub,
	}
}

type WorkerReturn interface {
	ReturnAddPiece(ctx context.Context, callID CallID, pi abi.PieceInfo, err *CallError) error
	ReturnSealPreCommit1(ctx context.Context, callID CallID, p1o storage.PreCommit1Out, err *CallError) error
	ReturnSealPreCommit2(ctx context.Context, callID CallID, sealed storage.SectorCids, err *CallError) error
	ReturnSealCommit1(ctx context.Context, callID CallID, out storage.Commit1Out, err *CallError) error
	ReturnSealCommit2(ctx context.Context, callID CallID, proof storage.Proof, err *CallError) error
	ReturnFinalizeSector(ctx context.Context, callID CallID, err *CallError) error
	ReturnReleaseUnsealed(ctx context.Context, callID CallID, err *CallError) error
	ReturnMoveStorage(ctx context.Context, callID CallID, err *CallError) error
	ReturnUnsealPiece(ctx context.Context, callID CallID, err *CallError) error
	ReturnReadPiece(ctx context.Context, callID CallID, ok bool, err *CallError) error
	ReturnFetch(ctx context.Context, callID CallID, err *CallError) error
}

//下面都是Dai加的内容
type TaskConfig struct {
	LimitPC1Count int  //限制的PC1总数
	RunPC1Count   int  //正在跑的PC1数量
	IsRunningAP   bool //是否正在跑ap
}

//空闲的任务数量
func (w *WorkerInfo) GetFreeTaskNumber(phaseTaskType sealtasks.TaskType) int {
	w.TaskResourcesLk.Lock()
	defer w.TaskResourcesLk.Unlock()
	switch phaseTaskType {
	case sealtasks.TTAddPiece:
		if w.TaskNumber.IsRunningAP {
			return 0
		} else {
			return 1
		}
	case sealtasks.TTPreCommit1:
		return w.TaskNumber.LimitPC1Count - w.TaskNumber.RunPC1Count
	}
	return 0
}

//接受任务时+1
func (w *WorkerInfo) TaskAddOne(phaseTaskType sealtasks.TaskType) {
	w.TaskResourcesLk.Lock()
	defer w.TaskResourcesLk.Unlock()
	switch phaseTaskType {
	case sealtasks.TTAddPiece:
		w.TaskNumber.IsRunningAP = true
	case sealtasks.TTPreCommit1:
		w.TaskNumber.RunPC1Count++
	}
}

//完成任务时-1
func (w *WorkerInfo) TaskReduceOne(phaseTaskType sealtasks.TaskType) {
	w.TaskResourcesLk.Lock()
	defer w.TaskResourcesLk.Unlock()
	switch phaseTaskType {
	case sealtasks.TTAddPiece:
		w.TaskNumber.IsRunningAP = false
	case sealtasks.TTPreCommit1:
		if w.TaskNumber.RunPC1Count > 0 {
			w.TaskNumber.RunPC1Count--
		}
	}
}

func NewTaskConfig(LimitPC1Count int) TaskConfig {
	return TaskConfig{
		LimitPC1Count: LimitPC1Count,
		RunPC1Count:   0,
		IsRunningAP:   false,
	}
}
