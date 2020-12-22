package mydynamotest

import (
	"fmt"
	"mydynamo"
	"testing"
)

func TestBasicVectorClock(t *testing.T) {
	t.Logf("Starting TestBasicVectorClock")

	//create two vector clocks
	clock1 := mydynamo.NewVectorClock()
	clock2 := mydynamo.NewVectorClock()

	//Test for equality
	if !clock1.Equals(clock2) {
		t.Fail()
		t.Logf("Vector Clocks were not equal")
	}

}

func TestLessVectorClock(t *testing.T) {
	t.Logf("Starting TestLessClock")

	//create two vector clocks
	clock1 := mydynamo.NewVectorClock()
	clock2 := mydynamo.NewVectorClock()

	// create clock map
	map1 := map[string]int{
		"A": 1,
		"C": 1,
	}

	map2 := map[string]int{
		"A": 0,
		"D": 0,
	}

	clock1.VectorClock = map1
	clock2.VectorClock = map2

	//Test for equality
	if !clock2.LessThan(clock1) {
		t.Fail()
		t.Logf("should be less than")
	}

}

func TestEqualVectorClock(t *testing.T) {
	t.Logf("Starting TestLessClock")

	//create two vector clocks
	clock1 := mydynamo.NewVectorClock()
	clock2 := mydynamo.NewVectorClock()

	// create clock map
	map1 := map[string]int{
		"A": 1,
		"B": 1,
		"C": 1,
	}

	map2 := map[string]int{
		"A": 1,
		"B": 1,
	}

	clock1.VectorClock = map1
	clock2.VectorClock = map2

	//Test for equality
	if clock2.Equals(clock1) {
		t.Fail()
		t.Logf("should be equal")
	}

}

func TestConcurVectorClock(t *testing.T) {
	t.Logf("Starting TestConcurClock")

	//create two vector clocks
	clock1 := mydynamo.NewVectorClock()
	clock2 := mydynamo.NewVectorClock()

	// create clock map
	map1 := map[string]int{
		"A": 1,
		"B": 1,
	}

	map2 := map[string]int{
		"A": 1,
		"B": 1,
	}

	clock1.VectorClock = map1
	clock2.VectorClock = map2

	//Test for equality
	if clock2.Concurrent(clock1) {
		t.Fail()
		t.Logf("should be unconcur")
	}

}

func TestCombineVectorClock(t *testing.T) {
	t.Logf("Starting TestConcurClock")

	//create two vector clocks
	clock1 := mydynamo.NewVectorClock()
	clock2 := mydynamo.NewVectorClock()

	// create clock map
	map1 := map[string]int{
		"A": 2,
		"B": 1,
	}

	map2 := map[string]int{
		"A": 2,
		"C": 1,
	}

	clock1.VectorClock = map1
	clock2.VectorClock = map2

	clock3 := mydynamo.NewVectorClock()
	//clock3.NodeId = "A"
	arr := []mydynamo.VectorClock{clock1, clock2}

	clock3.Combine(arr)
	res := map[string]int{
		"A": 3,
		"B": 1,
		"C": 1,
	}

	//Test for equality
	if fmt.Sprint(clock3.VectorClock) != fmt.Sprint(res) {
		t.Fail()
		t.Logf("should be concur")
	}

}
