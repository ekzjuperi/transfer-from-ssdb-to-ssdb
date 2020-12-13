package config

import (
	"fmt"
)

// SSDBActionsHistoryConfig сonfiguration for history ssdb.
type SSDBActionsHistoryConfig SsdbConfig

// String get string configure from history ssdb.
func (o SSDBActionsHistoryConfig) String() string {
	return fmt.Sprintf(`
			MaxConnections: %v,
			Host: %v,
			Port: %v,
			Timeout: %v`,
		o.MaxConnections,
		o.Host,
		o.Port,
		o.Timeout)
}
