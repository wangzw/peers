package broker

import (
	"context"
	"errors"

	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/pairing"
	"github.com/wangzw/peers/internal/topic"
)

// topicAssembler builds the topic registry over the link that the pairing and
// link assembler created and installs it as Deps.Topics. topic.New registers
// the registry's frame handlers and link subscription, so it must run before
// Run binds the listener.
type topicAssembler struct {
	// self overrides the fingerprint normally read from asm.Pairing. Unit
	// tests set it because building a pairing identity needs the real
	// configuration; production leaves it zero.
	self link.Fingerprint

	// pairingFingerprint reads the fingerprint a pairing presents on the link.
	// Production leaves it nil, so selfFingerprint calls
	// (*pairing.Pairing).Fingerprint. Unit tests replace it for the same reason
	// self exists: a real identity needs the real configuration.
	pairingFingerprint func(*pairing.Pairing) link.Fingerprint
}

var (
	_ topic.Transport = (*link.Link)(nil)
	_ Assembler       = topicAssembler{}
)

// Name is the assembler's name in startup errors.
func (topicAssembler) Name() string { return "topic" }

// selfFingerprint returns the fingerprint the registry labels its digests
// with. a.self wins when set; otherwise it is the pairing's fingerprint, the
// value the peer sees on the link.
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

// Assemble constructs the registry. It returns no component: the registry
// starts working as soon as link starts, and Run owns link's lifecycle.
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
