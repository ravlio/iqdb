package test

import (
	"testing"
	"time"
)

func rng(a map[int]int) {
	println(1)
	for k,v:=range a {
		println(k,v)
	}
}
func TestA(t *testing.T) {
	println(1)
	a := map[int]int{}

	for i := 0; i < 10000; i++ {
		a[i] = i
	}

	for i := 0; i < 10; i++ {
		go rng(a)
	}

	time.Sleep(time.Second*5)
}

