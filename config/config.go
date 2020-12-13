package config

import (
	"fmt"

	xc "xr/configuration"
)

// SSDBHistoryToSSDBActionHistory describes service configuration, retrieved from Consul.
type SSDBHistoryToSSDBActionHistory struct {
	SSDBHistoryConfig        // Embedding ssdb config
	SSDBActionsHistoryConfig // Embedding action history ssdb config
}

// GetConfig function returns SSDBHistoryToSSDBActionHistory from Consul.
func GetConfig() (*SSDBHistoryToSSDBActionHistory, error) {
	config := &SSDBHistoryToSSDBActionHistory{}
	// Setup Consul connection.
	err := xc.Setup("")
	if err != nil {
		return nil, fmt.Errorf("xc.Setup('') error: %v", err)
	}

	// Fill SSDBToSSDB service config from Consul.
	err = xc.Fill(config)
	if err != nil {
		return nil, fmt.Errorf("xc.Fill(%v) error: %v", config, err)
	}

	return config, nil
}

// String function provides human-readable representation of service configuration.
func (o *SSDBHistoryToSSDBActionHistory) String() string {
	return fmt.Sprintf(`SSDBToSSDB:
		SSDBConfig:
			Host: %v
			Port: %v
		SSDBActionsHistoryConfig:
			Host: %v
			Port: %v`,
		o.SSDBHistoryConfig.Host,
		o.SSDBHistoryConfig.Port,
		o.SSDBActionsHistoryConfig.Host,
		o.SSDBActionsHistoryConfig.Port,
	)
}
