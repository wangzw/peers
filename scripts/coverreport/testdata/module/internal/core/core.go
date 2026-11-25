package core

func Pick(n int) string {
	s := "zero"
	if n < 0 {
		s = "negative"
	} else if n == 0 {
		s = "zero"
	} else {
		s = "positive"
	}
	switch {
	case n > 100:
		s += "-big"
	case n > 10:
		s += "-medium"
	default:
		s += "-small"
	}
loop:
	for i := 0; i < n; i++ {
		if i == 7 {
			break loop
		}
		s = s + "."
	}
	return s
}

func Literal(n int) int {
	add := func(v int) int {
		return v + 1
	}
	{
		n = add(n)
	}
	return n
}

func Drain(ch <-chan int) int {
	select {
	case v := <-ch:
		return v
	default:
		return -1
	}
}

func Kind(v any) string {
	switch v.(type) {
	case int:
		return "int"
	default:
		return "other"
	}
}

func Unused() int {
	x := 0
	x++
	return x
}
