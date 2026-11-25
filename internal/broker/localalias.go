package broker

// localAliasChanged pushes the current address of every registered agent to each
// of its registration streams and records it as the announced address, so a
// later recompute does not push it again. The topic table is not called, since
// its roster entries carry no alias, and connections still waiting for
// registration receive nothing.
func (b *Broker) localAliasChanged() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, a := range b.agents {
		b.pushAgentChangedLocked(a)
	}
}
