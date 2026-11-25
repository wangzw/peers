package broker

import (
	"context"
	"errors"

	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/pairing"
	"github.com/wangzw/peers/internal/topic"
)

// topicAssembler installs the topic registry; it must run before Run binds the
// listener, because topic.New registers frame handlers.
type topicAssembler struct {
	self link.Fingerprint

	pairingFingerprint func(*pairing.Pairing) link.Fingerprint
}

var (
	_ topic.Transport = (*link.Link)(nil)
	_ Assembler       = topicAssembler{}
)

func (topicAssembler) Name() string { return "topic" }

func (a topicAssembler) selfFingerprint(asm *Assembly) (link.Fingerprint, error) {
	if !a.self.IsZero() {
		return a.self, nil
	}
	if asm.Pairing == nil {
		return link.Fingerprint{}, errors.New("topic needs the pairing built by the pairing and link assembler")
	}
	if a.pairingFingerprint != nil {
		return a.pairingFingerprint(asm.Pairing), nil
	}
	return asm.Pairing.Fingerprint(), nil
}

// Assemble constructs the registry and returns no component; the registry works
// as soon as the link runs.
func (a topicAssembler) Assemble(_ context.Context, _ ConfigSource, deps *Deps, asm *Assembly) ([]Component, error) {
	if asm.Link == nil {
		return nil, errors.New("topic needs the link built by the pairing and link assembler")
	}
	self, err := a.selfFingerprint(asm)
	if err != nil {
		return nil, err
	}
	r := topic.New(asm.Link, topic.Options{Logger: deps.Logger, Self: self})
	deps.Topics = NewTopicTable(r)
	return nil, nil
}
