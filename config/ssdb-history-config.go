package config

import (
	"fmt"
)

// SsdbConfig configuration for statistics ssdb
type SsdbConfig struct {
	Host           string // Ssdb host
	Port           uint16 // Ssdb port
	Timeout        int    // Operation timeout in secs
	MaxConnections int    // Max reconnects number
}

// SSDBHistoryConfig сonfiguration for history ssdb.
type SSDBHistoryConfig SsdbConfig

// String get string configure from history ssdb.
func (o SSDBHistoryConfig) String() string {
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
