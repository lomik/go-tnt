package tnt

import "testing"

func BenchmarkSelectPack(b *testing.B) {
	// run the Fib function b.N times
	for n := 0; n < b.N; n++ {
		query := &Select{
			Values: Tuple{PackL(11), PackL(12)},
			Space:  10,
			Offset: 13,
			Limit:  14,
			Index:  15,
		}
		query.Pack()
	}
}
