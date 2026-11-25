package main

import (
	"example.com/fx/internal/core"
	"example.com/fx/internal/mixed"
	"example.com/fx/internal/nearonly"
	"example.com/fx/internal/pbonly"
	"example.com/fx/internal/testkit"
	"example.com/fx/scripts/support"
	testutil "example.com/fx/test"
)

func main() {
	ch := make(chan int)
	close(ch)
	_ = core.Pick(3)
	_ = core.Literal(1)
	_ = core.Drain(ch)
	_ = core.Kind(1)
	_ = mixed.Value()
	_ = nearonly.Value()
	_ = pbonly.Value()
	_ = testkit.Fixture()
	_ = support.Value()
	_ = testutil.Value()
}
