package workers

import (
	"sync"
	"sync/atomic"

	"github.com/golang/glog"
	"github.com/lessos/lessgo/data/hissdb"

	"transfer-from-ssdb-to-ssdb/consts"
	"transfer-from-ssdb-to-ssdb/dba"
	"transfer-from-ssdb-to-ssdb/types"
)

func WorkerWritingToSSDB(wg *sync.WaitGroup, ssdbActionsHistory *hissdb.Connector, processedDataChan chan types.Event, batchSize int, recordCounter *int32, isReadonly bool) {
	defer wg.Done()

	pipe := dba.InitPipeline(ssdbActionsHistory)

	var err error

	for {
		event, ok := <-processedDataChan
		if !ok {
			err = pipe.ExecuteHistoryPipe(isReadonly)
			if err != nil {
				glog.Fatalf("pipe.ExecuteHistoryPipe() err: %v", err)
			}

			pipe.ClearPipe()

			break
		}

		pipe.AddHistoryPipeCmd(event.Key, event.TTL)

		atomic.AddInt32(recordCounter, 1)

		remainder := atomic.LoadInt32(recordCounter) % consts.Multiplay
		if remainder == 0 {
			glog.Infof("The script made %v records in the ssdb", atomic.LoadInt32(recordCounter))
		}

		if pipe.GetPipeLen() >= batchSize {
			err = pipe.ExecuteHistoryPipe(isReadonly)
			if err != nil {
				glog.Fatalf("pipe.ExecuteHistoryPipe() err: %v", err)
			}

			pipe.ClearPipe()
		}
	}
}
