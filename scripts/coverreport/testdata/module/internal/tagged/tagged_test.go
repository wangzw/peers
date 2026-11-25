package tagged

import "testing"

func TestValue(t *testing.T) {
	if Value() != 10 {
		t.Fatal("Value")
	}
}
