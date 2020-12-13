package workers

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"

	"transfer-from-ssdb-to-ssdb/consts"
	"transfer-from-ssdb-to-ssdb/dba"
	"transfer-from-ssdb-to-ssdb/types"
)

func WorkerParsingKeys(wg *sync.WaitGroup, rawDataChan chan string, processedDataChan chan types.Event, counterKeysReadyForWriting *int32) {
	defer wg.Done()

	for {
		key, ok := <-rawDataChan
		if !ok {
			glog.Info("rawDataChan close")

			close(processedDataChan)

			break
		}

		now := time.Now().UTC().Unix()

		data, err := dba.ParseSerieHistoryStatKey(key)
		if err != nil {
			errMessage := "kvdbs.ParseSerieHistoryStatKey(%v) error %v"
			glog.Errorf(errMessage, key, err)

			continue
		}

		if data.HistoryActionID != consts.SSDBHistoryOpenActionID && data.HistoryActionID != consts.SSDBHistoryClickActionID {
			continue
		}

		expirationTime := int(consts.TTLThreeMonths - (now - data.Timestamp)) // expirationTime = 90day - (CurrentTime - Timestamp)

		// If expirationTime is negative, then the event is older than 90 days
		if expirationTime < 0 {
			continue
		}

		event := types.Event{
			Key: key,
			TTL: expirationTime,
		}

		atomic.AddInt32(counterKeysReadyForWriting, 1)

		remainder := atomic.LoadInt32(counterKeysReadyForWriting) % consts.Multiplay
		if remainder == 0 {
			glog.Infof("The parsing-worker prepared for recording %v keys", atomic.LoadInt32(counterKeysReadyForWriting))
		}

		processedDataChan <- event
	}
}
