package directory

import (
	"errors"
	"time"

	"github.com/wangzw/peers/internal/config"
)

type configSettings struct {
	get        func(string) (config.Value, error)
	advertised func() []config.AdvertisedAddr
	paths      func() config.PathSet
}

var _ Settings = configSettings{}

func (s configSettings) value(key string) (config.Value, error) {
	if s.get != nil {
		return s.get(key)
	}
	return config.Get(key)
}

func (s configSettings) Public() bool {
	v, err := s.value("broker.public")
	if err != nil {
		return false
	}
	return v.Bool
}

func (s configSettings) Alias() string {
	v, err := s.value("broker.alias")
	if err != nil {
		return ""
	}
	return v.String
}

func (s configSettings) Advertised() []config.AdvertisedAddr {
	src := s.advertised
	if src == nil {
		src = config.Advertised
	}
	addrs := src()
	if addrs == nil {
		return nil
	}
	return append([]config.AdvertisedAddr(nil), addrs...)
}

func (s configSettings) DataDir() (string, error) {
	paths := s.paths
	if paths == nil {
		paths = config.Paths
	}
	dir := paths().DataDir
	if dir == "" {
		return "", errors.New("the configuration has no data directory")
	}
	return dir, nil
}

func (s configSettings) ReconcileInterval() time.Duration {
	v, err := s.value("directory.reconcile_interval")
	if err != nil {
		return 10 * time.Minute
	}
	return v.Duration
}

func (s configSettings) MaxRecords() int {
	v, err := s.value("directory.max_records")
	if err != nil {
		return 1000
	}
	return int(v.Int)
}

func (s configSettings) MaxRecordsPerLink() int {
	v, err := s.value("directory.max_records_per_link")
	if err != nil {
		return 250
	}
	return int(v.Int)
}

func (s configSettings) MaxTopicsPerPublisher() int {
	v, err := s.value("directory.max_topics_per_publisher")
	if err != nil {
		return 16
	}
	return int(v.Int)
}
