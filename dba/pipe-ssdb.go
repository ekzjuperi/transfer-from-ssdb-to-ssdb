package dba

import (
	"fmt"
	"sync"

	"github.com/lessos/lessgo/data/hissdb"

	"transfer-from-ssdb-to-ssdb/consts"
)

// Pipeline struct for work SSDB pipeline.
type Pipeline struct {
	rwmutex       *sync.RWMutex     // RW Mutex for pipelineCount
	connector     *hissdb.Connector // Connection handler for SSDB
	batch         *hissdb.Batch     // Pipeline handler for SSDB
	pipelineCount int               // Pipeline count
}

// InitPipeline init Pipeline wrapper struct.
func InitPipeline(ssdb *hissdb.Connector) *Pipeline {
	return &Pipeline{
		connector:     ssdb,
		batch:         ssdb.Batch(),
		rwmutex:       new(sync.RWMutex),
		pipelineCount: 0,
	}
}

// AddHistoryPipeCmd add command in pipeline.
func (o *Pipeline) AddHistoryPipeCmd(key string, ttl int) {
	o.rwmutex.Lock()
	o.batch.Cmd("setx", key, consts.ValueForHistoryDatabaseSet, ttl)
	o.pipelineCount++
	o.rwmutex.Unlock()
}

// ExecuteHistoryPipe execute pipeline with commands.
func (o *Pipeline) ExecuteHistoryPipe(isReadonly bool) (err error) {
	o.rwmutex.Lock()
	defer o.rwmutex.Unlock()

	if !isReadonly {
		ssdbResponses, err := o.batch.Exec()
		if err != nil {
			return err
		}

		for _, response := range ssdbResponses {
			if response.State != hissdb.ReplyOK {
				return fmt.Errorf("SSDB response is %s", response.State)
			}
		}
	}

	o.pipelineCount = 0

	return nil
}

// ClearPipe discard pipeline.
func (o *Pipeline) ClearPipe() {
	o.rwmutex.Lock()
	defer o.rwmutex.Unlock()

	o.batch = o.connector.Batch()
	o.pipelineCount = 0
}

// GetPipeLen get pipeline length.
func (o *Pipeline) GetPipeLen() int {
	o.rwmutex.RLock()
	defer o.rwmutex.RUnlock()

	return o.pipelineCount
}
