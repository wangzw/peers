package nearonly

import "testing"

func TestValue(t *testing.T) {
	if Value() != 8 {
		t.Fatal("Value")
	}
}
