package broker

import "github.com/wangzw/peers/internal/config"

// ConfigKeys returns every configuration key in lexicographic order.
func ConfigKeys() []string { return config.KeyNames() }
