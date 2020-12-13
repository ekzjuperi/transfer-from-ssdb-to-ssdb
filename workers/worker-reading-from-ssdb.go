package workers

import (
	"sync"

	"github.com/golang/glog"
	"github.com/lessos/lessgo/data/hissdb"
)

func WorkerReadingFromSSDB(wg *sync.WaitGroup, ssdbHistory *hissdb.Connector, rawDataChan chan string, batchSize int) {
	defer wg.Done()

	startKey := ""
	endKey := ""

	for {
		reply := ssdbHistory.Cmd("keys", startKey, endKey, batchSize)
		if reply.State != hissdb.ReplyOK {
			errMessage := "command ssdb keys %v %v return error %v"
			glog.Errorf(errMessage, startKey, endKey, reply.State)

			break
		}

		keys := reply.Data
		if len(keys) == 0 {
			break
		}

		for _, key := range keys {
			rawDataChan <- key
		}

		startKey = keys[len(keys)-1]
	}

	close(rawDataChan)
}
