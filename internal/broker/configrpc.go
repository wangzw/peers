package broker

import (
	"context"
	"errors"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
)

// GetConfig returns configuration values. An empty key returns every key in
// the order All reports; a non-empty key returns exactly that entry. Secret
// values are reported only as set or unset. Access is enforced by the
// interceptor, not here.
func (b *Broker) GetConfig(ctx context.Context, req *localpb.GetConfigRequest) (*localpb.GetConfigResponse, error) {
	cfg := b.deps.Config
	key := req.GetKey()
	if key == "" {
		entries := cfg.All()
		out := make([]*localpb.ConfigEntry, 0, len(entries))
		for _, e := range entries {
			out = append(out, &localpb.ConfigEntry{Key: e.Key, Value: toConfigValue(e.Value)})
		}
		return &localpb.GetConfigResponse{Entries: out}, nil
	}

	v, err := cfg.Get(key)
	if err != nil {
		return nil, toStatus(configValueError(key, err))
	}
	return &localpb.GetConfigResponse{Entries: []*localpb.ConfigEntry{
		{Key: key, Value: toConfigValue(v)},
	}}, nil
}

func configValueError(key string, err error) *Error {
	if errors.Is(err, config.ErrUnknownKey) {
		return Errorf(localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
			"unknown configuration key %q", key)
	}
	return Errorf(configErrorCode(err), "%s", err.Error())
}

// SetConfig parses and applies one configuration change. The initiator is
// resolved first, before any key is read. Secret keys and broker.listen, while
// this run is fixed by --listen, are refused before the value is parsed. Access
// is enforced by the interceptor, not here.
func (b *Broker) SetConfig(ctx context.Context, req *localpb.SetConfigRequest) (*localpb.SetConfigResponse, error) {
	cfg := b.deps.Config

	actor, err := b.auditActor(ctx, req.GetAgentId())
	if err != nil {
		return nil, toStatus(err)
	}

	key := req.GetKey()
	if key == "" {
		return nil, toStatus(Errorf(localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT, "key is required"))
	}

	current, err := cfg.Get(key)
	if err != nil {
		return nil, toStatus(configValueError(key, err))
	}
	if current.Kind == config.KindSecret {
		return nil, toStatus(Errorf(localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
			"%s can only be set in the configuration file %s", key, cfg.Paths().ConfigFile))
	}

	if key == "broker.listen" && b.deps.ListenOverride != "" {
		return nil, toStatus(Errorf(localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
			"broker.listen cannot be changed while this Broker runs with --listen %s; restart it without --listen to change the listen address",
			b.deps.ListenOverride))
	}

	v, err := cfg.ParseValue(key, req.GetValue())
	if err != nil {
		return nil, toStatus(Errorf(configErrorCode(err), "%s", err.Error()))
	}

	old, err := cfg.Set(actor, key, v)
	if err != nil {
		return nil, toStatus(Errorf(configErrorCode(err), "%s", err.Error()))
	}

	return &localpb.SetConfigResponse{
		OldValue: toConfigValue(old),
		NewValue: toConfigValue(v),
	}, nil
}
