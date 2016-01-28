package tnt

import "testing"

func BenchmarkCallWithDefer(b *testing.B) {
	nothingWithDefer := func(a, b int) (result int) {
		defer func() {
		}()
		result = a + b
		return
	}

	for n := 0; n < b.N; n++ {
		nothingWithDefer(1, 2)
	}
}

func BenchmarkCallWithRecover(b *testing.B) {
	nothingWithRecover := func(a, b int) (result int) {
		defer func() {
			if r := recover(); r != nil {
				result = 0
			}
		}()
		result = a + b
		return
	}

	for n := 0; n < b.N; n++ {
		nothingWithRecover(1, 2)
	}
}

func BenchmarkCallWithFinishFunction(b *testing.B) {
	nothingWithFinishFunction := func(a, b int) (result int) {
		finish := func() {}
		result = a + b
		finish()
		return
	}

	for n := 0; n < b.N; n++ {
		nothingWithFinishFunction(1, 2)
	}
}

func BenchmarkCallWithoutRecover(b *testing.B) {
	nothingWithoutRecover := func(a, b int) (result int) {
		result = a + b
		return
	}

	for n := 0; n < b.N; n++ {
		nothingWithoutRecover(1, 2)
	}
}
