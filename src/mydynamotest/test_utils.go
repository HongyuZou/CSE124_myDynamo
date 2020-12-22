package mydynamotest

import (
	"log"
	"mydynamo"
	"os"
	"os/exec"
	"strconv"
)

//Creats a command that will start Dynamo nodes based on the config file specified
// by config_path
func InitDynamoServer(config_path string) *exec.Cmd {
	serverCmd := exec.Command("DynamoCoordinator", config_path)
	serverCmd.Stderr = os.Stderr
	serverCmd.Stdout = os.Stdout
	return serverCmd
}

//Executes the command generated by InitDynamoServer
func StartDynamoServer(server *exec.Cmd, ready chan bool) {
	err := server.Start()
	if err != nil {
		log.Println("err in spinning up servers")
		log.Println(err)
	}
	log.Println("finished in spinning up servers")
	ready <- true
}

//Kills the Dynamo servers generated by InitDynamoServer
func KillDynamoServer(server *exec.Cmd) {
	_ = server.Process.Kill()
	exec.Command("pkill SurfstoreServerExec*")
}

//Creates a client that connects to the given port
//A client instance returned by this function is ready to use
func MakeConnectedClient(port int) *mydynamo.RPCClient {
	clientInstance := mydynamo.NewDynamoRPCClient("localhost:" + strconv.Itoa(port))
	clientInstance.RpcConnect()
	return clientInstance
}

//Creates a PutArgs with the associated key and value, but a Context corresponding
//to a new VectorClock
func PutFreshContext(key string, value []byte) mydynamo.PutArgs {
	return mydynamo.NewPutArgs(key, mydynamo.NewContext(mydynamo.NewVectorClock()), value)
}

func PutContextWithClock(key string, value []byte, clock map[string]int) mydynamo.PutArgs {
	vectorClock := mydynamo.NewVectorClock()
	vectorClock.VectorClock = clock
	return mydynamo.NewPutArgs(key, mydynamo.NewContext(vectorClock), value)
}

//Tests if the contents of two byte arrays are equal
func valuesEqual(v1 []byte, v2 []byte) bool {
	if len(v1) != len(v2) {
		return false
	}
	for idx, b := range v1 {
		if b != v2[idx] {
			return false
		}
	}
	return true
}
