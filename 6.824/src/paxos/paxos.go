package paxos

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"syscall"
	"time"
)

type Paxos struct {
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int
	// Your data here.
	instances    map[int]InstanceStatus // keep track of instances
	acceptors    map[int]Acceptor       // keep track of accepters
	proposers    map[int]Proposer       // keep track of proposers
	done         int
	maxN         int
	acceptorLock sync.Mutex
	proposerLock sync.Mutex
	instanceLock sync.Mutex

	completed []int
}

type InstanceStatus struct {
	value  interface{}
	status string
}

type PrepareRequestArg struct {
	Seq int
	N   int
}

type AcceptRequestArg struct {
	N     int
	Value interface{}
	Seq   int
}

type PrepareRequestReply struct {
	OK               bool
	HighestAccept    int
	HighestAcceptVal interface{}
	HighestPrepareN  int
}

type AcceptRequestReply struct {
	OK bool
}

type DecideRequestReply struct {
	N            int
	RemovableSeq int
}

type DecideRequestArg struct {
	Seq   int
	Value interface{}
}

type Acceptor struct {
	HighestPrepare   int
	HighestAccept    int
	HighestAcceptVal interface{}
}

type Proposer struct {
	N int
}

func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {

		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}
	return false
}

func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	px.instanceLock.Lock()
	defer px.instanceLock.Unlock()

	// if current round identifier is less than or equal to the current round identifier do nothing
	if px.done >= seq {
		return
	}

	// if value has already been proposed
	instance, ok := px.instances[seq]
	if !ok {
		// if proposal does not exist, create it with current round identifier
		instance = InstanceStatus{}
		instance.status = "Initiated"
		instance.value = v
		px.instances[seq] = instance

		go px.PaxosRoutine(seq, v) // start Paxos algorithm
	}

}

func (px *Paxos) SendPrepare(seq int, v interface{}, n int) (int, interface{}, int) {
	promise_count := 0                   //keep count of promises
	highestAcceptNumber := math.MinInt64 // highest round identifier to be accepted so far, set to MinInt64 by default to accept any value at first
	highestSeen := n                     // highest round identifier is set to n, which keeps track of the proposal round identifiers
	var HighestAcceptVal interface{} = nil
	for _, peer := range px.peers {
		// setting up prepare request arguments to pass into prepare function
		prepareRequestReply := PrepareRequestReply{}
		prepareRequestArg := PrepareRequestArg{}
		prepareRequestArg.N = n
		prepareRequestArg.Seq = seq
		ok := false
		if peer == px.peers[px.me] { // if prepare is sent to current server, just run function locally
			px.Prepare(&prepareRequestArg, &prepareRequestReply)
			ok = true
		} else { // if prepare is sent to other servers other than itself, must run function through rpc call
			ok = call(peer, "Paxos.Prepare", &prepareRequestArg, &prepareRequestReply)
		} //
		if ok {
			if prepareRequestReply.OK { // if the acceptor responded with a promise
				// increment promise_count by one
				promise_count += 1
				//update the highest accepted round identifier along with its value
				if highestAcceptNumber < prepareRequestReply.HighestAccept {
					highestAcceptNumber = prepareRequestReply.HighestAccept
					HighestAcceptVal = prepareRequestReply.HighestAcceptVal
				}
			}
			// if the acceptor has seen a high round identifier, then we have to update the highest seen round identifier
			if highestSeen < prepareRequestReply.HighestPrepareN {
				highestSeen = prepareRequestReply.HighestPrepareN
			}

		}
	}
	return promise_count, HighestAcceptVal, highestSeen
}

func (px *Paxos) SendAccept(seq int, n int, HighestAcceptVal interface{}, accept_count int) int {
	for _, peer := range px.peers {
		acceptRequestArg := AcceptRequestArg{}
		acceptRequestArg.N = n
		acceptRequestArg.Seq = seq
		acceptRequestArg.Value = HighestAcceptVal
		acceptRequestReply := AcceptRequestReply{}
		ok := false
		if peer == px.peers[px.me] {
			px.Accept(&acceptRequestArg, &acceptRequestReply)
			ok = true
		} else {
			ok = call(peer, "Paxos.Accept", &acceptRequestArg, &acceptRequestReply)
		}
		if ok {
			if acceptRequestReply.OK {
				accept_count += 1
			}
		}
	}
	return accept_count
}

func (px *Paxos) SendDecide(seq int, HighestAcceptVal interface{}) {
	for i, peer := range px.peers {
		decideRequestArg := DecideRequestArg{}
		decideRequestArg.Seq = seq
		decideRequestArg.Value = HighestAcceptVal
		decideRequestReply := DecideRequestReply{}
		ok := false
		if peer == px.peers[px.me] {
			px.Decide(&decideRequestArg, &decideRequestReply)
			ok = true
		} else {
			ok = call(peer, "Paxos.Decide", &decideRequestArg, &decideRequestReply)
		}
		if ok {
			px.instanceLock.Lock()
			px.completed[i] = decideRequestReply.RemovableSeq
			px.instanceLock.Unlock()

		}

	}
}

func (px *Paxos) PaxosRoutine(seq int, v interface{}) {
	px.instanceLock.Lock()
	instance := px.instances[seq]
	px.instanceLock.Unlock()

	n := 0

	//if current proposal has not been decided on, and the paxos server is live
	for instance.status != "Decided" && !px.dead {

		promise_count, HighestAcceptVal, highestSeen := px.SendPrepare(seq, v, n)
		px.proposerLock.Lock()
		//check if proposer for current value exists in list of proposers
		_, ok := px.proposers[seq]
		if !ok {
			// add new proposer with current round identifier to list
			px.proposers[seq] = Proposer{px.me}
		}
		n = px.proposers[seq].N // update proposer round identifier
		px.proposerLock.Unlock()

		majority := len(px.peers)/2 + 1
		// if promise_count >= majority , we can move on to Accept stage
		if promise_count >= majority {
			instance = px.SetPrepared(seq)

			if HighestAcceptVal == nil {
				HighestAcceptVal = instance.value
			}

			accept_count := 0

			accept_count = px.SendAccept(seq, n, HighestAcceptVal, accept_count)

			// if accept_count >= majority, the value is then accepted
			if accept_count >= majority {
				//set current proposal to accepted and broadcast to other acceptors
				px.SetAccept(seq)
				px.SendDecide(seq, HighestAcceptVal)

				px.instanceLock.Lock()
				instance = px.instances[seq]

				px.instanceLock.Unlock()

			} else {
				//create proposer with higher round identifier
				px.proposerLock.Lock()
				proposer := px.proposers[seq]
				proposer.N = highestSeen + (px.me) + 1
				px.proposers[seq] = proposer
				px.proposerLock.Unlock()

				time.Sleep(time.Microsecond * 100)

			}

		} else {
			//create proposer with higher round identifier
			px.proposerLock.Lock()
			proposer := px.proposers[seq]
			proposer.N = highestSeen + (px.me) + 1
			px.proposers[seq] = proposer
			px.proposerLock.Unlock()

			time.Sleep(time.Microsecond * 100)
		}

	}

}

func (px *Paxos) Prepare(prepareRequestArg *PrepareRequestArg, prepareRequestReply *PrepareRequestReply) error {
	px.acceptorLock.Lock()
	defer px.acceptorLock.Unlock()

	acceptor, ok := px.acceptors[prepareRequestArg.Seq]
	//if acceptor does not exist for the current round, initialize one
	if !ok {
		acceptor = Acceptor{}
		acceptor.HighestAccept = -1
		acceptor.HighestPrepare = -1
		acceptor.HighestAcceptVal = nil
	}

	n := prepareRequestArg.N
	//if current proposal has a round identifier than all previous ones
	if acceptor.HighestPrepare < n {
		//update highest round identifier and return a "promise"
		acceptor.HighestPrepare = n
		px.acceptors[prepareRequestArg.Seq] = acceptor
		prepareRequestReply.OK = true
		prepareRequestReply.HighestAccept = acceptor.HighestAccept
		prepareRequestReply.HighestAcceptVal = acceptor.HighestAcceptVal
		prepareRequestReply.HighestPrepareN = acceptor.HighestPrepare
	} else {
		//otherwise , reject proposal
		prepareRequestReply.OK = false
		prepareRequestReply.HighestPrepareN = acceptor.HighestPrepare
	}
	return nil
}

func (px *Paxos) SetPrepared(seq int) InstanceStatus {
	// sets the status of current instance to "prepared" meaning prepared to be sent out as a proposal
	px.instanceLock.Lock()
	defer px.instanceLock.Unlock()

	instance := px.instances[seq]
	instance.status = "Prepared"
	return instance
}

func (px *Paxos) Accept(acceptRequestArg *AcceptRequestArg, acceptRequestReply *AcceptRequestReply) error {
	px.acceptorLock.Lock()
	defer px.acceptorLock.Unlock()
	acceptor, exist := px.acceptors[acceptRequestArg.Seq]

	// if an acceptor does not exist for the current round, create one with default values
	if !exist {
		acceptor = Acceptor{}
		acceptor.HighestAccept = -1
		acceptor.HighestPrepare = -1
		acceptor.HighestAcceptVal = nil
	}

	n := acceptRequestArg.N
	//if round identifier is higher than previous proposals
	if n >= acceptor.HighestPrepare {
		//accept propsal
		acceptor.HighestPrepare = n
		acceptor.HighestAccept = n
		acceptor.HighestAcceptVal = acceptRequestArg.Value
		px.acceptors[acceptRequestArg.Seq] = acceptor
		acceptRequestReply.OK = true
	} else {
		//otherwise deny proposal
		acceptRequestReply.OK = false
	}
	return nil
}

func (px *Paxos) SetAccept(seq int) InstanceStatus {
	//set current instance to accepted, and ready to move on to decide stage
	px.instanceLock.Lock()
	defer px.instanceLock.Unlock()

	instance := px.instances[seq]
	instance.status = "Accepted"
	return instance
}

func (px *Paxos) Decide(decideRequestArg *DecideRequestArg, decideRequestReply *DecideRequestReply) error {
	px.instanceLock.Lock()
	defer px.instanceLock.Unlock()

	instance, exist := px.instances[decideRequestArg.Seq]

	//decide on current proposal
	if exist {
		instance.status = "Decided"
		instance.value = decideRequestArg.Value
		px.instances[decideRequestArg.Seq] = instance
	} else {
		ins := InstanceStatus{}
		ins.status = "Decided"
		ins.value = decideRequestArg.Value
		px.instances[decideRequestArg.Seq] = ins
	}

	// keep track of which older proposals can be removed
	decideRequestReply.RemovableSeq = px.HighestRemoveableSeq()

	decideRequestReply.N = 0
	return nil
}

func (px *Paxos) Done(seq int) {
	// Your code here.
	px.instanceLock.Lock()
	defer px.instanceLock.Unlock()

	if seq < 0 || seq <= px.done {
		return
	}

	px.done = seq

}

func (px *Paxos) Max() int {
	// Your code here.
	px.instanceLock.Lock()
	defer px.instanceLock.Unlock()

	maxN := -1
	for key := range px.instances {
		if maxN < key {
			maxN = key
		}
	}
	return maxN
}

func (px *Paxos) HighestRemoveableSeq() int {

	maxN := px.maxN

	//iterate instances to check which values have been decided on
	for key := range px.instances {
		if px.instances[key].status == "Decided" && key <= px.done {
			if maxN < key {
				maxN = key
			}
		}
	}

	i := 0

	for i = 0; i <= maxN; i++ {
		instance, ok := px.instances[i]
		if ok && instance.status == "Decided" || (!ok) {
			continue
		} else {
			break
		}
	}

	px.maxN = i - 1

	return px.maxN

}

func (px *Paxos) Min() int {
	px.instanceLock.Lock()
	defer px.instanceLock.Unlock()

	min := math.MaxInt64
	for i := range px.completed {
		if px.completed[i] < min {
			min = px.completed[i]
		}
	}

	// clean up previous instances
	for i := 0; i <= min; i++ {
		_, exist := px.instances[i]
		if exist {
			delete(px.instances, i)
		}
	}

	px.proposerLock.Lock()

	//clean up previous proposers
	for i := 0; i <= min; i++ {
		_, exist := px.proposers[i]
		if exist {
			delete(px.proposers, i)
		}
	}
	px.proposerLock.Unlock()

	px.acceptorLock.Lock()

	//clean up previous acceptors
	for i := 0; i <= min; i++ {
		_, exist := px.acceptors[i]
		if exist {
			delete(px.acceptors, i)
		}
	}
	px.acceptorLock.Unlock()

	return min + 1
}

func (px *Paxos) Status(seq int) (bool, interface{}) {
	// Your code here.
	px.instanceLock.Lock()
	status, exist := px.instances[seq]
	px.instanceLock.Unlock()

	if exist {
		return status.status == "Decided", status.value
	} else {
		return false, nil
	}

}

func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me
	px.dead = false

	// Your initialization code here.
	px.instances = map[int]InstanceStatus{}
	px.acceptors = map[int]Acceptor{}
	px.proposers = map[int]Proposer{}
	px.completed = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		px.completed[i] = -1
	}
	px.done = -1
	px.maxN = -1

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
