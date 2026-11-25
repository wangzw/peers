package broker

import (
	"context"

	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/pairing"
)

// Assembly carries the upstream instances earlier assemblers built; a later
// assembler reads or replaces them.
type Assembly struct {
	// Pairing is the pairing instance the peer assembler opened.
	Pairing *pairing.Pairing
	// Link is the link built by the peer assembler.
	Link *link.Link
	// Runner drives the link lifecycle at startup and shutdown.
	Runner LinkRunner
	// LinkProtocolVersion is the link protocol version; 0 means linkpb.ProtocolVersion.
	LinkProtocolVersion uint32

	// System reports that Run runs the system form.
	System bool

	// Access is the access checker the system assembler builds; nil means
	// allowAllAccess in user mode.
	Access accessChecker

	configAssembled bool
	effectiveAccess accessChecker
}

// Assembler builds one piece of upstream state before the Broker is
// constructed. Assemblers run in order and share the Assembly and Deps, so a
// later one can read or replace what an earlier one built.
type Assembler interface {
	// Name identifies the assembler in an error message.
	Name() string
	// Assemble builds its components and state and returns the components Run
	// starts; it may replace fields in deps before New copies them.
	Assemble(ctx context.Context, cfg ConfigSource, deps *Deps, asm *Assembly) ([]Component, error)
}

// DefaultAssemblers returns the assemblers Run uses when
// RunOptions.Assemblers is nil. The order must not change: the directory reads
// the topic registry's public topic set, and configWiring must stay last.
func DefaultAssemblers() []Assembler {
	return []Assembler{newPeerAssembler(), topicAssembler{}, &directoryAssembler{}, newDeliveryAssembler(), &configWiring{}}
}
