package dba

import (
	"fmt"
	"strconv"
	"strings"
	"transfer-from-ssdb-to-ssdb/consts"
)

// SerieHistoryStatKey
type SerieHistoryStatKey struct {
	ProfileID       int64
	PartnerID       int
	SiteID          int
	SerieID         int
	ContentID       int64
	HistoryActionID int
	Timestamp       int64
}

// ParseSerieHistoryStatKey parse ssdb key to SerieHistoryStatKey
func ParseSerieHistoryStatKey(historyStatKey string) (*SerieHistoryStatKey, error) {
	keys := strings.Split(historyStatKey, consts.SerieHistoryStatKeySeparator)
	if len(keys) != consts.SerieHistoryStatKeyPartsLenght {
		return nil, fmt.Errorf("invalid SSDB key %v", historyStatKey)
	}

	// Checking ProfileID is correct
	profileID, err := strconv.ParseInt(keys[consts.SerieHistoryStatProfileElementIndex], 10, 64)
	if err != nil {
		var errMessage = "convert ProfileID %v error %v"
		return nil, fmt.Errorf(errMessage, keys[consts.SerieHistoryStatProfileElementIndex], err)
	}

	// Checking PartnerID is correct
	partnerID, err := strconv.Atoi(keys[consts.SerieHistoryStatPartnerElementIndex])
	if err != nil {
		var errMessage = "convert partnerID %v error %v"
		return nil, fmt.Errorf(errMessage, keys[consts.SerieHistoryStatPartnerElementIndex], err)
	}

	// Checking SiteID is correct
	siteID, err := strconv.Atoi(keys[consts.SerieHistoryStatSiteElementIndex])
	if err != nil {
		var errMessage = "convert siteID %v error %v"
		return nil, fmt.Errorf(errMessage, keys[consts.SerieHistoryStatSiteElementIndex], err)
	}

	// Checking SerieID is correct
	serieID, err := strconv.Atoi(keys[consts.SerieHistoryStatSerieElementIndex])
	if err != nil {
		var errMessage = "convert serieID %v error %v"
		return nil, fmt.Errorf(errMessage, keys[consts.SerieHistoryStatSerieElementIndex], err)

	}

	// Checking ContentID is correct
	contentID, err := strconv.ParseInt(keys[consts.SerieHistoryStatContentElementIndex], 10, 64)
	if err != nil {
		var errMessage = "convert ContentID %v error %v"
		return nil, fmt.Errorf(errMessage, keys[consts.SerieHistoryStatContentElementIndex], err)
	}

	// Checking HistoryActionID is correct
	historyActionID, err := strconv.Atoi(keys[consts.SerieHistoryStatActionElementIndex])
	if err != nil {
		var errMessage = "convert historyActionID %v error %v"
		return nil, fmt.Errorf(errMessage, keys[consts.SerieHistoryStatActionElementIndex], err)
	}

	// Checking timestamp is correct
	timestamp, err := strconv.ParseInt(keys[consts.SerieHistoryStatTsElementIndex], 10, 64)
	if err != nil {
		var errMessage = "convert Timestamp %v error %v"
		return nil, fmt.Errorf(errMessage, keys[consts.SerieHistoryStatTsElementIndex], err)
	}

	var serieHistoryStatKey = &SerieHistoryStatKey{
		ProfileID:       profileID,
		PartnerID:       partnerID,
		SiteID:          siteID,
		SerieID:         serieID,
		ContentID:       contentID,
		HistoryActionID: historyActionID,
		Timestamp:       timestamp,
	}

	return serieHistoryStatKey, nil
}
