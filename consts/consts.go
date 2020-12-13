package consts

const (
	TTLThreeMonths int64 = 3600 * 24 * 90

	ValueForHistoryDatabaseSet = 1      // value for key in SSDBActionHistory
	LenForRawDataChannel       = 3000   // channel for keys from SSDBHistory
	LenForProcessedDataChan    = 300    // channel for keys ready for writing to the SSDBActionHistory
	Multiplay                  = 100000 // variable required to display script statistics

	SerieHistoryStatProfileElementIndex = 0
	SerieHistoryStatPartnerElementIndex = 1
	SerieHistoryStatSiteElementIndex    = 2
	SerieHistoryStatSerieElementIndex   = 3
	SerieHistoryStatContentElementIndex = 4
	SerieHistoryStatActionElementIndex  = 5
	SerieHistoryStatTsElementIndex      = 6
	SerieHistoryStatKeyPartsLenght      = 7

	SerieHistoryStatKeySeparator = "_"

	SSDBHistoryOpenActionID     = 1 // ID Open for SSDB History database
	SSDBHistoryClickActionID    = 2 // ID Click for SSDB History database
)
