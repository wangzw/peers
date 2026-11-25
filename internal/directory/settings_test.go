package directory

import (
	"testing"
	"time"

	"github.com/wangzw/peers/internal/config"
)

// TestConfigSettingsDefaultsWhenNotLoaded checks the value every setting returns before the config is loaded.
func TestConfigSettingsDefaultsWhenNotLoaded(t *testing.T) {
	s := configSettings{}

	if s.Public() {
		t.Error("Public() = true, want false before Load")
	}
	if got := s.Alias(); got != "" {
		t.Errorf("Alias() = %q, want empty before Load", got)
	}
	if got := s.Advertised(); len(got) != 0 {
		t.Errorf("Advertised() = %v, want empty before Load", got)
	}
	if got, err := s.DataDir(); err == nil || got != "" {
		t.Errorf("DataDir() = %q, %v, want an error and an empty path before Load", got, err)
	}
	if got := s.ReconcileInterval(); got != 10*time.Minute {
		t.Errorf("ReconcileInterval() = %v, want 10m", got)
	}
	if got := s.MaxRecords(); got != 1000 {
		t.Errorf("MaxRecords() = %d, want 1000", got)
	}
	if got := s.MaxRecordsPerLink(); got != 250 {
		t.Errorf("MaxRecordsPerLink() = %d, want 250", got)
	}
	if got := s.MaxTopicsPerPublisher(); got != 16 {
		t.Errorf("MaxTopicsPerPublisher() = %d, want 16", got)
	}
}

// TestConfigSettingsReadsLiveValues checks that each call reads the current value through the seam.
func TestConfigSettingsReadsLiveValues(t *testing.T) {
	values := map[string]config.Value{
		"broker.public":         {Kind: config.KindBool, Bool: true},
		"directory.max_records": {Kind: config.KindInt, Int: 3},
	}
	get := func(key string) (config.Value, error) {
		v, ok := values[key]
		if !ok {
			return config.Value{}, config.ErrUnknownKey
		}
		return v, nil
	}
	addrs := []config.AdvertisedAddr{{Kind: config.AdvertisedKindDirect, Addr: "198.51.100.4:17443"}}
	s := configSettings{
		get: get,
		advertised: func() []config.AdvertisedAddr {
			return addrs
		},
	}

	if !s.Public() {
		t.Error("Public() = false, want true")
	}
	if got := s.MaxRecords(); got != 3 {
		t.Errorf("MaxRecords() = %d, want 3", got)
	}

	values["directory.max_records"] = config.Value{Kind: config.KindInt, Int: 5}
	if got := s.MaxRecords(); got != 5 {
		t.Errorf("MaxRecords() after change = %d, want 5", got)
	}

	got := s.Advertised()
	if len(got) != 1 || got[0].Addr != addrs[0].Addr {
		t.Errorf("Advertised() = %v, want %v", got, addrs)
	}
	addrs[0].Addr = "203.0.113.9:17443"
	got[0].Addr = "mutated"
	if again := s.Advertised(); again[0].Addr != "203.0.113.9:17443" {
		t.Errorf("Advertised() aliases the source slice: got %q", again[0].Addr)
	}

	emptyPaths := configSettings{paths: func() config.PathSet { return config.PathSet{} }}
	if _, err := emptyPaths.DataDir(); err == nil {
		t.Error("DataDir() with an empty DataDir path = nil error, want an error")
	}

	withDir := configSettings{paths: func() config.PathSet { return config.PathSet{DataDir: "/var/lib/peers"} }}
	if dir, err := withDir.DataDir(); err != nil || dir != "/var/lib/peers" {
		t.Errorf("DataDir() = %q, %v, want /var/lib/peers", dir, err)
	}
}
