package core

import "testing"

func TestPick(t *testing.T) {
	if got := Pick(3); got != "positive-small..." {
		t.Fatalf("Pick(3) = %q", got)
	}
}

func TestLiteral(t *testing.T) {
	if got := Literal(1); got != 2 {
		t.Fatalf("Literal(1) = %d", got)
	}
}

func TestDrain(t *testing.T) {
	ch := make(chan int)
	close(ch)
	if got := Drain(ch); got != 0 {
		t.Fatalf("Drain(closed) = %d", got)
	}
}

func TestKind(t *testing.T) {
	if got := Kind(1); got != "int" {
		t.Fatalf("Kind(1) = %q", got)
	}
}
