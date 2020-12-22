package mydynamo

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

type DynamoServer struct {
	/*------------Dynamo-specific-------------*/
	wValue         int                      //Number of nodes to write to on each Put
	rValue         int                      //Number of nodes to read from on each Get
	preferenceList []DynamoNode             //Ordered list of other Dynamo nodes to perform operations o
	selfNode       DynamoNode               //This node's address and port info
	nodeID         string                   //ID of this node
	storage        map[string][]ObjectEntry // concurrent
	crashed        bool
}

func (s *DynamoServer) SendPreferenceList(incomingList []DynamoNode, _ *Empty) error {
	s.preferenceList = incomingList
	return nil
}

// Forces server to gossip, put current servers all key/value to other servers
// As this method takes no arguments, we must use the Empty placeholder
func (s *DynamoServer) Gossip(_ Empty, _ *Empty) error {
	for _, node := range s.preferenceList {
		clientInstance := NewDynamoRPCClient(node.Address + ":" + node.Port)
		clientInstance.RpcConnect()

		for key, entry := range s.storage {
			for _, object := range entry {
				clientInstance.PutLocal(NewPutArgs(key, object.Context, object.Value))
			}
		}
	}
	return nil
}

//Makes server unavailable for some seconds
func (s *DynamoServer) Crash(seconds int, success *bool) error {
	s.crashed = true
	time.Sleep(time.Duration(seconds) * time.Second)
	s.crashed = false
	*success = true
	return nil
}

// Put a file to this server
func (s *DynamoServer) PutLocal(value PutArgs, result *bool) error {
	if s.crashed {
		*result = false
		return errors.New("Crashed")
	}

	// increase clock value, value -> clock
	vectorClock := value.Context.Clock
	key := value.Key
	storeValue := value.Value
	newObject := ObjectEntry{
		Context: value.Context,
		Value:   storeValue,
	}

	bigger := false
	concur := true
	for idx := 0; idx < len(s.storage[key]); idx++ {
		obj := s.storage[key][idx]

		if obj.Context.Clock.LessThan(vectorClock) {
			bigger = true
			s.storage[key] = remove(s.storage[key], idx)
			idx--
		}

		if !obj.Context.Clock.Concurrent(vectorClock) || 
		   obj.Context.Clock.Equals(vectorClock) {
			concur = false
		}

		if vectorClock.LessThan(obj.Context.Clock) {
			*result = false
			return nil
		}
	}

	if bigger || concur {
		s.storage[key] = append(s.storage[key], newObject)
		*result = true
		return nil
	}

	*result = false
	return nil

}

// Put a file to this server and W other servers
func (s *DynamoServer) Put(value PutArgs, result *bool) error {
	// first put to local storage
	value.Context.Clock.Increment(s.nodeID)
	var res bool
	err := s.PutLocal(value, &res)
	if err != nil {
		return err
	}
	log.Println("put local res: ", res)

	// then replicate to w - 1 servers, server don't response?
	cnt := 0
	for i := 0; i < len(s.preferenceList); i++ {
		// reached quarom, break
		if cnt == s.wValue-1 {
			break
		}

		// meet self, skip
		node := s.preferenceList[i]
		if node.Address == s.selfNode.Address && node.Port == s.selfNode.Port {
			continue
		}

		clientInstance := NewDynamoRPCClient(node.Address + ":" + node.Port)
		err := clientInstance.RpcConnect()
		if err == nil {
			succ := clientInstance.PutLocal(value)
			log.Println("put on other node: ", succ, node.Address, node.Port, cnt, s.wValue)
			if !succ {
				continue
			}
			
			cnt++
		}
	}
	log.Println("put res: ", cnt == s.wValue-1, cnt)
	*result = (cnt == s.wValue-1)
	return nil
}

//Get a file from this server, matched with R other servers
func (s *DynamoServer) GetLocal(key string, result *DynamoResult) error {
	if s.crashed {
		*result = DynamoResult{}
		return errors.New("Crashed")
	}

	objects := s.storage[key]
	*result = DynamoResult{
		EntryList: objects,
	}

	return nil
}

//Get a file from this server, matched with R other servers
func (s *DynamoServer) Get(key string, result *DynamoResult) error {
	tempRes := DynamoResult{
		EntryList: []ObjectEntry{},
	}
	err := s.GetLocal(key, &tempRes)
	if err != nil {
		return err
	}

	// make call to r - 1 servers
	// then replicate to w - 1 servers, server don't response?
	cnt := 0
	for i := 0; i < len(s.preferenceList); i++ {
		// reached quarom, break
		if cnt == s.rValue-1 {
			break
		}

		// skip over self
		node := s.preferenceList[i]
		if node.Address == s.selfNode.Address && node.Port == s.selfNode.Port {
			continue
		}

		clientInstance := NewDynamoRPCClient(node.Address + ":" + node.Port)
		err := clientInstance.RpcConnect()
		if err == nil {
			otherResult := clientInstance.GetLocal(key)
			
			// get fail, continue
			if otherResult == nil {
				continue
			}
			cnt++
			log.Println("get other result", *otherResult, node.Address +":" + node.Port)
			for _, otherObj := range otherResult.EntryList {
				bigger := false
				concur := true
				for idx := 0; idx < len(tempRes.EntryList); idx++ {
					obj := tempRes.EntryList[idx]

					if obj.Context.Clock.LessThan(otherObj.Context.Clock) {
						bigger = true
						tempRes.EntryList = remove(tempRes.EntryList, idx)
						idx--
					}

					if !obj.Context.Clock.Concurrent(otherObj.Context.Clock) || 
					   obj.Context.Clock.Equals(otherObj.Context.Clock){
						concur = false
					}

					if otherObj.Context.Clock.LessThan(obj.Context.Clock) {
						continue
					}
				}

				if bigger || concur {
					tempRes.EntryList = append(tempRes.EntryList, otherObj)
				}
			}
		}
	}
	*result = tempRes
	return nil
}

/* Belows are functions that implement server boot up and initialization */
func NewDynamoServer(w int, r int, hostAddr string, hostPort string, id string) DynamoServer {
	preferenceList := make([]DynamoNode, 0)
	selfNodeInfo := DynamoNode{
		Address: hostAddr,
		Port:    hostPort,
	}
	return DynamoServer{
		wValue:         w,
		rValue:         r,
		preferenceList: preferenceList,
		selfNode:       selfNodeInfo,
		nodeID:         id,
		storage: 	make(map[string][]ObjectEntry),
	}
}

func ServeDynamoServer(dynamoServer DynamoServer) error {
	rpcServer := rpc.NewServer()
	e := rpcServer.RegisterName("MyDynamo", &dynamoServer)
	if e != nil {
		log.Println(DYNAMO_SERVER, "Server Can't start During Name Registration")
		return e
	}

	log.Println(DYNAMO_SERVER, "Successfully Registered the RPC Interfaces")

	l, e := net.Listen("tcp", dynamoServer.selfNode.Address+":"+dynamoServer.selfNode.Port)
	if e != nil {
		log.Println(DYNAMO_SERVER, "Server Can't start During Port Listening")
		return e
	}

	log.Println(DYNAMO_SERVER, "Successfully Listening to Target Port ", dynamoServer.selfNode.Address+":"+dynamoServer.selfNode.Port)
	log.Println(DYNAMO_SERVER, "Serving Server Now")

	return http.Serve(l, rpcServer)
}
