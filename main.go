package main

import (
	"flag"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/lessos/lessgo/data/hissdb"

	"transfer-from-ssdb-to-ssdb/config"
	"transfer-from-ssdb-to-ssdb/consts"
	"transfer-from-ssdb-to-ssdb/types"
	"transfer-from-ssdb-to-ssdb/workers"
)

var (
	isReadonly                                                                                   bool
	recordCounter, counterKeysReadyForWriting                                                    int32
	ssdbBatchSize, workersCounterForParsingHistorySSDBKeys, workersCounterForWritingInActionSSDB int
)

func main() {
	flag.IntVar(&ssdbBatchSize, "b", 2000, "SSDB batch size")
	flag.IntVar(&workersCounterForParsingHistorySSDBKeys, "wp", 6, "workers count for parsing SSDB keys")
	flag.IntVar(&workersCounterForWritingInActionSSDB, "ww", 6, "workers count for writing in action ssdb")
	flag.BoolVar(&isReadonly, "r", false, "readonly script execution")
	flag.Parse()

	glog.Infoln("Script started")

	beginTime := time.Now()

	// Get configuration
	cfg, err := config.GetConfig()
	if err != nil {
		glog.Fatalf("config.GetConfig() error: %v", err)
		return
	}

	glog.Infoln(cfg)

	// Get SSDBHistory connector
	ssdbHistoryCli, err := hissdb.NewConnector(hissdb.Config{
		Host:    cfg.SSDBHistoryConfig.Host,
		Port:    cfg.SSDBHistoryConfig.Port,
		Timeout: cfg.SSDBHistoryConfig.Timeout,
		MaxConn: cfg.SSDBHistoryConfig.MaxConnections,
	})
	if err != nil {
		glog.Fatalf("ssdb.NewConnector() Error:%v", err)
	}
	defer ssdbHistoryCli.Close()

	// Get SSDBActionsHistory connector
	ssdbCliActionsHistory, err := hissdb.NewConnector(hissdb.Config{
		Host:    cfg.SSDBActionsHistoryConfig.Host,
		Port:    cfg.SSDBActionsHistoryConfig.Port,
		Timeout: cfg.SSDBActionsHistoryConfig.Timeout,
		MaxConn: cfg.SSDBActionsHistoryConfig.MaxConnections,
	})
	if err != nil {
		glog.Fatalf("Get ssdbCliActionHistory Error:%v", err)
	}
	defer ssdbCliActionsHistory.Close()

	rawDataChan := make(chan string, consts.LenForRawDataChannel)
	processedDataChan := make(chan types.Event, consts.LenForProcessedDataChan)

	wgReadAndWriteWorkers := &sync.WaitGroup{}
	wgParsingWorkers := &sync.WaitGroup{}

	// Start scan worker
	go workers.WorkerReadingFromSSDB(wgReadAndWriteWorkers, ssdbHistoryCli, rawDataChan, ssdbBatchSize)

	wgReadAndWriteWorkers.Add(1)

	for i := 0; i < workersCounterForParsingHistorySSDBKeys; i++ {
		wgParsingWorkers.Add(1)

		go workers.WorkerParsingKeys(wgParsingWorkers, rawDataChan, processedDataChan, &counterKeysReadyForWriting)
	}

	for i := 0; i < workersCounterForWritingInActionSSDB; i++ {
		wgReadAndWriteWorkers.Add(1)

		go workers.WorkerWritingToSSDB(wgReadAndWriteWorkers, ssdbCliActionsHistory, processedDataChan, ssdbBatchSize, &recordCounter, isReadonly)
	}

	wgParsingWorkers.Wait()

	close(processedDataChan)

	wgReadAndWriteWorkers.Wait()

	glog.Infof("Keys from ssdb that have passed all conditions: %v\n", counterKeysReadyForWriting)
	glog.Infof("Total keys recorded to ssdbActionHistory: %v\n", recordCounter)
	glog.Infof("Script done in: %v\n", time.Since(beginTime))

}
