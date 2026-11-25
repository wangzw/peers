//go:build peers_testenv

package tagged

func Extra(v int) int {
	x := v + 1
	return x
}
