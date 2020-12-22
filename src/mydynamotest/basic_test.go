package mydynamotest

import (
	"log"
	"mydynamo"
	"testing"
	"time"
)

func TestBasicPut(t *testing.T) {
	t.Logf("Starting basic Put test")

	//Test initialization
	//Note that in the code below, dynamo servers will use the config file located in src/mydynamotest
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)

	//starts the Dynamo nodes, and get ready to kill them when done with the test
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	//Wait for the nodes to finish spinning up.
	time.Sleep(3 * time.Second)
	log.Println("finish wait for spinninng up servers")
	<-ready

	//Create a client that connects to the first server
	//This assumes that the config file specifies 8080 as the starting port
	time.Sleep(3 * time.Second)
	clientInstance := MakeConnectedClient(8080)

	//Put a value on key "s1"
	res := clientInstance.Put(PutFreshContext("s1", []byte("abcde")))
	if res == false {
		t.Fail()
		t.Logf("TestBasicPut: Returned false")
	}

	//Get the value back, and check if we successfully retrieved the correct value
	gotValuePtr := clientInstance.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicPut: Returned nil")
	}

	gotValue := *gotValuePtr
	t.Log("got value: ", gotValue)
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestBasicPut: Failed to get value")
	}
}

// 2 2 2
func TestPutTwoAndGet(t *testing.T) {
	t.Logf("Starting basic Put test")

	//Test initialization
	//Note that in the code below, dynamo servers will use the config file located in src/mydynamotest
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)

	//starts the Dynamo nodes, and get ready to kill them when done with the test
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	//Wait for the nodes to finish spinning up.
	time.Sleep(3 * time.Second)
	log.Println("finish wait for spinninng up servers")
	<-ready

	//Create a client that connects to the first server
	//This assumes that the config file specifies 8080 as the starting port
	time.Sleep(3 * time.Second)
	clientInstance := MakeConnectedClient(8080)

	//Put a value on key "s1"
	res := clientInstance.Put(PutFreshContext("s1", []byte("abcde")))
	if res == false {
		t.Fail()
		t.Logf("TestBasicPut: Returned false")
	}

	clientInstance2 := MakeConnectedClient(8081)
	//Put a value on key "s1"
	res = clientInstance2.Put(PutFreshContext("s1", []byte("hijkf")))
	if res == false {
		t.Fail()
		t.Logf("TestBasicPut: Returned false")
	}

	//Get the value back, and check if we successfully retrieved the correct value
	gotValuePtr := clientInstance.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicPut: Returned nil")
	}

	gotValue := *gotValuePtr
	t.Log("got value: ", gotValue)
	if len(gotValue.EntryList) != 2 ||
		!valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) ||
		!valuesEqual(gotValue.EntryList[1].Value, []byte("hijkf")) {
		t.Fail()
		t.Logf("TestBasicPut: Failed to get value")
	}
}

// 2 2 2
func TestPutManyAndGet(t *testing.T) {
	t.Logf("Starting basic Put test")

	//Test initialization
	//Note that in the code below, dynamo servers will use the config file located in src/mydynamotest
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)

	//starts the Dynamo nodes, and get ready to kill them when done with the test
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	//Wait for the nodes to finish spinning up.
	time.Sleep(3 * time.Second)
	log.Println("finish wait for spinninng up servers")
	<-ready

	//Create a client that connects to the first server
	//This assumes that the config file specifies 8080 as the starting port
	time.Sleep(3 * time.Second)
	clientInstance := MakeConnectedClient(8080)

	//Put a value on key "s1"
	res := clientInstance.Put(PutFreshContext("s1", []byte("abcde")))
	if res == false {
		t.Fail()
		t.Logf("TestBasicPut: Returned false")
	}
	//Put a value on key "s1"
	clock := map[string]int{
		"0": 1,
	}
	res = clientInstance.Put(PutContextWithClock("s1", []byte("hijkf"), clock))
	if res == false {
		t.Fail()
		t.Logf("TestBasicPut: Returned false")
	}

	//Get the value back, and check if we successfully retrieved the correct value
	gotValuePtr := clientInstance.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicPut: Returned nil")
	}

	gotValue := *gotValuePtr
	t.Log("got value: ", gotValue)
	if len(gotValue.EntryList) != 1 ||
		!valuesEqual(gotValue.EntryList[0].Value, []byte("hijkf")) {
		t.Fail()
		t.Logf("TestBasicPut: Failed to get value")
	}
}

// 2 2 2
func TestPutReplace(t *testing.T) {
	t.Logf("Starting basic Put test")

	//Test initialization
	//Note that in the code below, dynamo servers will use the config file located in src/mydynamotest
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)

	//starts the Dynamo nodes, and get ready to kill them when done with the test
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	//Wait for the nodes to finish spinning up.
	time.Sleep(2 * time.Second)
	log.Println("finish wait for spinninng up servers")
	<-ready

	//Create a client that connects to the first server
	//This assumes that the config file specifies 8080 as the starting port
	time.Sleep(2 * time.Second)
	clientInstance := MakeConnectedClient(8080)

	//Put a value on key "s1"
	res := clientInstance.Put(PutFreshContext("s1", []byte("abcde")))
	if res == false {
		t.Fail()
		t.Logf("TestBasicPut: Returned false")
	}

	clientInstance2 := MakeConnectedClient(8081)
	//Put a value on key "s1"
	//Put a value on key "s1"
	clock := map[string]int{
		"0": 1,
		"1": 1,
	}
	res = clientInstance2.Put(PutContextWithClock("s1", []byte("hijkf"), clock))
	if res == false {
		t.Fail()
		t.Logf("TestBasicPut: Returned false")
	}

	//Get the value back, and check if we successfully retrieved the correct value
	gotValuePtr := clientInstance.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicPut: Returned nil")
	}

	gotValue := *gotValuePtr
	t.Log("got value: ", gotValue)
	if len(gotValue.EntryList) != 1 ||
		!valuesEqual(gotValue.EntryList[0].Value, []byte("hijkf")) {
		t.Fail()
		t.Logf("TestBasicPut: Failed to get value")
	}
}

// 4 1 4
func TestGetConcur(t *testing.T) {
	t.Logf("Starting basic Put test")

	//Test initialization
	//Note that in the code below, dynamo servers will use the config file located in src/mydynamotest
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)

	//starts the Dynamo nodes, and get ready to kill them when done with the test
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	//Wait for the nodes to finish spinning up.
	time.Sleep(2 * time.Second)
	log.Println("finish wait for spinninng up servers")
	<-ready

	//Create a client that connects to the first server
	//This assumes that the config file specifies 8080 as the starting port
	time.Sleep(2 * time.Second)

	// A
	clientInstance := MakeConnectedClient(8080)
	clock := map[string]int{
		"1": 1,
	}
	clientInstance.Put(PutFreshContext("s1", []byte("abcde")))

	// B
	clientInstance2 := MakeConnectedClient(8081)
	clock = map[string]int{
		"1": 1,
	}
	clientInstance2.Put(PutContextWithClock("s1", []byte("abcde"), clock))

	// C
	clientInstance3 := MakeConnectedClient(8082)
	clock = map[string]int{
		"2": 0,
	}
	clientInstance3.Put(PutContextWithClock("s1", []byte("hijkf"), clock))

	// D
	clientInstance4 := MakeConnectedClient(8083)
	clock = map[string]int{
		"1": 0,
	}
	clientInstance4.Put(PutContextWithClock("s1", []byte("12345"), clock))

	//Get the value back, and check if we successfully retrieved the correct value
	gotValuePtr := clientInstance.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicPut: Returned nil")
	}

	gotValue := *gotValuePtr
	t.Log("got value: ", gotValue)
	if len(gotValue.EntryList) != 4 {
		t.Fail()
		t.Logf("TestBasicPut: Failed to get value")
	}
}

func TestGetConflict(t *testing.T) {
	t.Logf("Starting basic Put test")

	//Test initialization
	//Note that in the code below, dynamo servers will use the config file located in src/mydynamotest
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)

	//starts the Dynamo nodes, and get ready to kill them when done with the test
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	//Wait for the nodes to finish spinning up.
	time.Sleep(2 * time.Second)
	log.Println("finish wait for spinninng up servers")
	<-ready

	//Create a client that connects to the first server
	//This assumes that the config file specifies 8080 as the starting port
	time.Sleep(2 * time.Second)

	// A
	clientInstance := MakeConnectedClient(8080)
	clock := map[string]int{
		"0": 1,
		"1": 1,
	}
	clientInstance.Put(PutFreshContext("s1", []byte("abcde")))

	// B
	clientInstance2 := MakeConnectedClient(8081)
	clock = map[string]int{
		"0": 2,
		"1": 0,
	}
	clientInstance2.Put(PutContextWithClock("s1", []byte("abcde"), clock))

	// C
	clientInstance3 := MakeConnectedClient(8082)
	clock = map[string]int{
		"2": 0,
	}
	clientInstance3.Put(PutContextWithClock("s1", []byte("hijkf"), clock))

	// D
	clientInstance4 := MakeConnectedClient(8083)
	clock = map[string]int{
		"0": 1,
		"3": -1,
	}
	clientInstance4.Put(PutContextWithClock("s1", []byte("12345"), clock))

	//Get the value back, and check if we successfully retrieved the correct value
	gotValuePtr := clientInstance.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicPut: Returned nil")
	}

	gotValue := *gotValuePtr
	t.Log("got value: ", gotValue)
	if len(gotValue.EntryList) != 2 {
		t.Fail()
		t.Logf("TestBasicPut: Failed to get value")
	}
}

// 3 3 3
func TestPutMultiple(t *testing.T) {
	t.Logf("Starting basic Put test")

	//Test initialization
	//Note that in the code below, dynamo servers will use the config file located in src/mydynamotest
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)

	//starts the Dynamo nodes, and get ready to kill them when done with the test
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	//Wait for the nodes to finish spinning up.
	time.Sleep(2 * time.Second)
	log.Println("finish wait for spinninng up servers")
	<-ready

	//Create a client that connects to the first server
	//This assumes that the config file specifies 8080 as the starting port
	time.Sleep(2 * time.Second)

	// A
	clientInstance := MakeConnectedClient(8080)
	clock := map[string]int{
		"0": 1,
	}
	clientInstance.Put(PutFreshContext("s1", []byte("abcde")))

	// B
	clientInstance2 := MakeConnectedClient(8081)
	clock = map[string]int{
		"0": 2,
		"1": -1,
	}
	clientInstance2.Put(PutContextWithClock("s1", []byte("abcde"), clock))

	// C
	clientInstance3 := MakeConnectedClient(8082)
	clock = map[string]int{
		"2": -1,
	}
	clientInstance3.Put(PutContextWithClock("s1", []byte("hijkf"), clock))

	//Get the value back, and check if we successfully retrieved the correct value
	gotValuePtr := clientInstance.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicPut: Returned nil")
	}

	gotValue := *gotValuePtr
	t.Log("got value: ", gotValue)
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("TestBasicPut: Failed to get value")
	}
}

// 2 2 2
func TestCrash(t *testing.T) {
	t.Logf("Starting basic Put test")

	//Test initialization
	//Note that in the code below, dynamo servers will use the config file located in src/mydynamotest
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)

	//starts the Dynamo nodes, and get ready to kill them when done with the test
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	//Wait for the nodes to finish spinning up.
	time.Sleep(2 * time.Second)
	log.Println("finish wait for spinninng up servers")
	<-ready

	//Create a client that connects to the first server
	//This assumes that the config file specifies 8080 as the starting port
	time.Sleep(2 * time.Second)

	// A
	clientInstance := MakeConnectedClient(8080)
	clock := map[string]int{
		"0": 1,
	}
	clientInstance.Put(PutContextWithClock("s1", []byte("abcde"), clock))

	// B
	clientInstance2 := MakeConnectedClient(8081)
	clock = map[string]int{
		"0": 1,
		"1": 0,
	}
	go clientInstance2.Crash(5)
	clientInstance2.Put(PutContextWithClock("s1", []byte("hijkf"), clock))

	//Get the value back, and check if we successfully retrieved the correct value
	gotValuePtr := clientInstance.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicPut: Returned nil")
	}

	gotValue := *gotValuePtr
	t.Log("got value: ", gotValue)
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("TestBasicPut: Failed to get value")
	}
}

// 1 1 2
func TestSimpleGossip(t *testing.T) {
	t.Logf("Starting basic Put test")

	//Test initialization
	//Note that in the code below, dynamo servers will use the config file located in src/mydynamotest
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)

	//starts the Dynamo nodes, and get ready to kill them when done with the test
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	//Wait for the nodes to finish spinning up.
	time.Sleep(2 * time.Second)
	log.Println("finish wait for spinninng up servers")
	<-ready

	//Create a client that connects to the first server
	//This assumes that the config file specifies 8080 as the starting port
	time.Sleep(2 * time.Second)

	// A
	clientInstance := MakeConnectedClient(8080)
	clock := map[string]int{
		"0": 1,
	}
	clientInstance.Put(PutContextWithClock("s1", []byte("abcde"), clock))
	clientInstance.Gossip()

	// B
	clientInstance2 := MakeConnectedClient(8081)

	//Get the value back, and check if we successfully retrieved the correct value
	gotValuePtr := clientInstance2.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicPut: Returned nil")
	}

	gotValue := *gotValuePtr
	t.Log("got value: ", gotValue)
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("TestBasicPut: Failed to get value")
	}
}

// 1 2 4
func TestPutQueryNum(t *testing.T) {
	t.Logf("Starting basic Put test")

	//Test initialization
	//Note that in the code below, dynamo servers will use the config file located in src/mydynamotest
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)

	//starts the Dynamo nodes, and get ready to kill them when done with the test
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	//Wait for the nodes to finish spinning up.
	time.Sleep(2 * time.Second)
	log.Println("finish wait for spinninng up servers")
	<-ready

	//Create a client that connects to the first server
	//This assumes that the config file specifies 8080 as the starting port
	time.Sleep(2 * time.Second)

	// A
	clientInstance := MakeConnectedClient(8080)
	clock := map[string]int{
		"0": 1,
	}
	clientInstance.Put(PutFreshContext("s1", []byte("abcde")))

	// B
	clientInstance2 := MakeConnectedClient(8081)
	clock = map[string]int{
		"0": 0,
		"1": 0,
	}
	clientInstance2.Put(PutContextWithClock("s1", []byte("abcde"), clock))

	// C
	clientInstance4 := MakeConnectedClient(8083)

	//Get the value back, and check if we successfully retrieved the correct value
	gotValuePtr := clientInstance4.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicPut: Returned nil")
	}

	gotValue := *gotValuePtr
	t.Log("got value: ", gotValue)
	if len(gotValue.EntryList) != 0 {
		t.Fail()
		t.Logf("TestBasicPut: Failed to get value")
	}

	// gossip will propagate values
	clientInstance = MakeConnectedClient(8080)
	clientInstance.Gossip()
	clientInstance2.Gossip()
	gotValuePtr = clientInstance4.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicPut: Returned nil")
	}

	gotValue = *gotValuePtr
	t.Log("got value: ", gotValue)
	if len(gotValue.EntryList) != 2 {
		t.Fail()
		t.Logf("TestBasicPut: Failed to get value")
	}
}

// 2 2 2
func TestCrash2(t *testing.T) {
	t.Logf("Starting basic Put test")

	//Test initialization
	//Note that in the code below, dynamo servers will use the config file located in src/mydynamotest
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)

	//starts the Dynamo nodes, and get ready to kill them when done with the test
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	//Wait for the nodes to finish spinning up.
	time.Sleep(2 * time.Second)
	log.Println("finish wait for spinninng up servers")
	<-ready

	//Create a client that connects to the first server
	//This assumes that the config file specifies 8080 as the starting port
	time.Sleep(2 * time.Second)

	// A B
	clientInstance := MakeConnectedClient(8080)
	clientInstance2 := MakeConnectedClient(8081)
	go clientInstance2.Crash(2)
	clock := map[string]int{
		"0": 1,
	}
	res := clientInstance.Put(PutContextWithClock("s1", []byte("abcde"), clock))

	if res {
		t.Fail()
		t.Logf("TestBasicPut: Returned succ")
	}

	//Get the value back, and check if we successfully retrieved the correct value
	gotValuePtr := clientInstance.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicPut: Returned nil")
	}

	gotValue := *gotValuePtr
	t.Log("got value: ", gotValue)
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("TestBasicPut: Failed to get value")
	}

	time.Sleep(2 * time.Second)
	clientInstance.Gossip()
	gotValuePtr = clientInstance2.Get("s1")
	gotValue = *gotValuePtr
	t.Log("got value: ", gotValue)
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("TestBasicPut: Failed to get value")
	}
}

// 3 3 3
func TestQuarum(t *testing.T) {
	t.Logf("Starting basic Put test")

	//Test initialization
	//Note that in the code below, dynamo servers will use the config file located in src/mydynamotest
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)

	//starts the Dynamo nodes, and get ready to kill them when done with the test
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	//Wait for the nodes to finish spinning up.
	time.Sleep(2 * time.Second)
	log.Println("finish wait for spinninng up servers")
	<-ready

	//Create a client that connects to the first server
	//This assumes that the config file specifies 8080 as the starting port
	time.Sleep(2 * time.Second)

	// B
	clientInstance2 := MakeConnectedClient(8081)
	clock := map[string]int{
		"0": 1,
		"1": 0,
	}
	clientInstance2.Put(PutContextWithClock("s1", []byte("abcde"), clock))

	// A
	clientInstance := MakeConnectedClient(8080)
	clock = map[string]int{
		"0": 1,
	}
	go clientInstance2.Crash(5)
	res := clientInstance.Put(PutFreshContext("s1", []byte("hijkf")))

	//Get the value back, and check if we successfully retrieved the correct value
	if res {
		t.Fail()
		t.Logf("TestBasicPut: Returned nil")
	}

	gotValuePtr := clientInstance.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestBasicPut: Returned nil")
	}

	gotValue := *gotValuePtr
	t.Log("got value: ", gotValue)
	if len(gotValue.EntryList) != 1 {
		t.Fail()
		t.Logf("TestBasicPut: Failed to get value")
	}
}

func TestPutW2(t *testing.T) {
	t.Logf("Starting PutW2 test")
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	time.Sleep(3 * time.Second)
	<-ready

	clientInstance0 := MakeConnectedClient(8080)
	clientInstance1 := MakeConnectedClient(8081)
	clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
	gotValuePtr := clientInstance1.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestPutW2: Failed to get")
	}
	gotValue := *gotValuePtr
	t.Log("got value: ", gotValue)
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestPutW2: Failed to get value")
	}

}

func TestGossip(t *testing.T) {
	t.Logf("Starting Gossip test")
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	time.Sleep(3 * time.Second)
	<-ready
	time.Sleep(1 * time.Second)

	clientInstance0 := MakeConnectedClient(8080)
	clientInstance1 := MakeConnectedClient(8081)
	clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
	clientInstance0.Gossip()
	gotValuePtr := clientInstance1.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestGossip: Failed to get")
	}
	gotValue := *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestGossip: Failed to get value")
	}

}

func TestMultipleKeys(t *testing.T) {
	t.Logf("Starting MultipleKeys test")
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	time.Sleep(3 * time.Second)
	<-ready
	time.Sleep(1 * time.Second)
	clientInstance0 := MakeConnectedClient(8080)
	clientInstance1 := MakeConnectedClient(8081)
	clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
	clientInstance0.Gossip()
	gotValuePtr := clientInstance0.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestMultipleKeys: Failed to get")
	}
	gotValue := *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestMultipleKeys: Failed to get value")
	}

	gotValuePtr = clientInstance1.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestMultipleKeys: Failed to get")
	}
	gotValue = *gotValuePtr

	clientInstance1.Put(mydynamo.NewPutArgs("s1", gotValue.EntryList[0].Context, []byte("efghi")))
	gotValuePtr = clientInstance1.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestMultipleKeys: Failed to get")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("efghi")) {
		t.Fail()
		t.Logf("TestMultipleKeys: Failed to get value")
	}
}

func TestDynamoPaper(t *testing.T) {
	t.Logf("DynamoPaper test")
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	time.Sleep(3 * time.Second)
	<-ready

	clientInstance0 := MakeConnectedClient(8080)
	clientInstance1 := MakeConnectedClient(8081)
	clientInstance2 := MakeConnectedClient(8082)

	clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
	gotValuePtr := clientInstance0.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestDynamoPaper: Failed to get first value")
	}

	gotValue := *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestDynamoPaper: First value doesn't match")
	}
	clientInstance0.Put(mydynamo.NewPutArgs("s1", gotValue.EntryList[0].Context, []byte("bcdef")))
	gotValuePtr = clientInstance0.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestDynamoPaper: Failed to get second value")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("bcdef")) {
		t.Fail()
		t.Logf("TestDynamoPaper: Second value doesn't match")
	}

	clientInstance0.Gossip()
	clientInstance1.Put(mydynamo.NewPutArgs("s1", gotValue.EntryList[0].Context, []byte("cdefg")))
	clientInstance2.Put(mydynamo.NewPutArgs("s1", gotValue.EntryList[0].Context, []byte("defgh")))
	gotValuePtr = clientInstance1.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestDynamoPaper: Failed to get third value")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("cdefg")) {
		t.Fail()
		t.Logf("TestDynamoPaper: Third value doesn't match")
	}
	gotValuePtr = clientInstance2.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestDynamoPaper: Failed to get fourth value")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("defgh")) {
		t.Fail()
		t.Logf("TestDynamoPaper: Fourth value doesn't match")
	}
	clientInstance1.Gossip()
	clientInstance2.Gossip()
	gotValuePtr = clientInstance0.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestDynamoPaper: Failed to get fifth value")
	}
	gotValue = *gotValuePtr
	clockList := make([]mydynamo.VectorClock, 0)
	for _, a := range gotValue.EntryList {
		clockList = append(clockList, a.Context.Clock)
	}
	clockList[0].Combine(clockList)
	combinedClock := clockList[0]
	combinedContext := mydynamo.Context{
		Clock: combinedClock,
	}
	clientInstance0.Put(mydynamo.NewPutArgs("s1", combinedContext, []byte("zyxw")))
	gotValuePtr = clientInstance0.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestDynamoPaper: Failed to get sixth value")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("zyxw")) {
		t.Fail()
		t.Logf("TestDynamoPaper: Sixth value doesn't match")
	}
}

func TestInvalidPut(t *testing.T) {
	t.Logf("Starting repeated Put test")
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	time.Sleep(3 * time.Second)
	<-ready
	clientInstance := MakeConnectedClient(8080)

	clientInstance.Put(PutFreshContext("s1", []byte("abcde")))
	clientInstance.Put(PutFreshContext("s1", []byte("efghi")))
	gotValue := clientInstance.Get("s1")
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestInvalidPut: Got wrong value")
	}
}

func TestGossipW2(t *testing.T) {
	t.Logf("Starting GossipW2 test")
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	time.Sleep(3 * time.Second)
	<-ready

	clientInstance0 := MakeConnectedClient(8080)
	clientInstance1 := MakeConnectedClient(8081)
	clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
	clientInstance0.Gossip()
	gotValuePtr := clientInstance1.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestGossipW2: Failed to get first element")
	}
	gotValue := *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestGossipW2: Failed to get value")
	}
	clientInstance1.Put(mydynamo.NewPutArgs("s1", gotValue.EntryList[0].Context, []byte("efghi")))

	gotValuePtr = clientInstance1.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestGossipW2: Failed to get")
	}
	gotValue = *gotValuePtr

	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("efghi")) {
		t.Fail()
		t.Logf("GossipW2: Failed to get value")
	}
	gotValuePtr = clientInstance0.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestGossipW2: Failed to get")
	}
	gotValue = *gotValuePtr

	if (len(gotValue.EntryList) != 1) || !valuesEqual(gotValue.EntryList[0].Value, []byte("efghi")) {
		t.Fail()
		t.Logf("GossipW2: Failed to get value")
	}
}

func TestReplaceMultipleVersions(t *testing.T) {
	t.Logf("Starting ReplaceMultipleVersions test")
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	time.Sleep(3 * time.Second)
	<-ready

	clientInstance0 := MakeConnectedClient(8080)
	clientInstance1 := MakeConnectedClient(8081)
	clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
	clientInstance1.Put(PutFreshContext("s1", []byte("efghi")))
	clientInstance0.Gossip()
	gotValuePtr := clientInstance1.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestReplaceMultipleVersions: Failed to get")
	}

	gotValue := *gotValuePtr
	clockList := make([]mydynamo.VectorClock, 0)
	for _, a := range gotValue.EntryList {
		clockList = append(clockList, a.Context.Clock)
	}
	clockList[0].Combine(clockList)
	combinedClock := clockList[0]
	combinedContext := mydynamo.Context{
		Clock: combinedClock,
	}
	clientInstance1.Put(mydynamo.NewPutArgs("s1", combinedContext, []byte("zxyw")))
	gotValuePtr = nil
	gotValuePtr = clientInstance1.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestReplaceMultipleVersions: Failed to get")
	}

	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("zxyw")) {
		t.Fail()
		t.Logf("testReplaceMultipleVersions: Values don't match")
	}
}

func TestConsistent(t *testing.T) {
	t.Logf("Starting Consistent test")
	cmd := InitDynamoServer("./myconfig.ini")
	ready := make(chan bool)
	go StartDynamoServer(cmd, ready)
	defer KillDynamoServer(cmd)

	time.Sleep(3 * time.Second)
	<-ready

	clientInstance0 := MakeConnectedClient(8080)
	clientInstance1 := MakeConnectedClient(8081)
	clientInstance2 := MakeConnectedClient(8082)
	clientInstance3 := MakeConnectedClient(8083)
	clientInstance4 := MakeConnectedClient(8084)

	clientInstance0.Put(PutFreshContext("s1", []byte("abcde")))
	gotValuePtr := clientInstance1.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestConsistent: Failed to get")
	}
	gotValue := *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("abcde")) {
		t.Fail()
		t.Logf("TestConsistent: Failed to get value")
	}

	clientInstance3.Put(mydynamo.NewPutArgs("s1", gotValue.EntryList[0].Context, []byte("zyxw")))
	clientInstance0.Crash(3)
	clientInstance1.Crash(3)
	clientInstance4.Crash(3)
	gotValuePtr = clientInstance2.Get("s1")
	if gotValuePtr == nil {
		t.Fail()
		t.Logf("TestConsistent: Failed to get")
	}
	gotValue = *gotValuePtr
	if len(gotValue.EntryList) != 1 || !valuesEqual(gotValue.EntryList[0].Value, []byte("zyxw")) {
		t.Fail()
		t.Logf("TestConsistent: Failed to get value")
	}
}
