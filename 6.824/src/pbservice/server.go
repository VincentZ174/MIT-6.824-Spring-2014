package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"
import "strconv"

// Debugging
const Debug = 0

type PBServer struct {
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk
	done       sync.WaitGroup
	finish     chan interface{}
	// Your declarations here.
	mu         sync.Mutex
	keyValue   map[string]string
	prevValues map[int64]string
	view       viewservice.View
}

func (pb *PBServer) RestoreSnapshot(args *SnapshotArgs, reply *SnapshotReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.me == pb.view.Backup { // if current server is backup
		pb.keyValue = args.StoredVals   // update keyValue map
		pb.prevValues = args.PrevValues // update previously stored values
		reply.Status = OK
		return nil
	}
	reply.Status = ErrWrongServer
	return nil
}

func (pb *PBServer) doPut(args *PutArgs, reply *PutReply) {
	if args.DoHash { //get previous value for hashing
		reply.Prev = pb.keyValue[args.Key]
		pb.prevValues[args.Id] = reply.Prev // save value
		hashval := hash(pb.keyValue[args.Key] + args.Value)
		pb.keyValue[args.Key] = strconv.Itoa(int(hashval)) // insert new hashed val into map
	} else {
		pb.keyValue[args.Key] = args.Value
		pb.prevValues[args.Id] = args.Value
	}
}

func (pb *PBServer) RedirectPut(args *PutArgs, reply *PutReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.me != pb.view.Backup {
		reply.Status = ErrWrongServer
	} else {
		_, ok := pb.prevValues[args.Id]
		if !ok {
			pb.doPut(args, reply) // insert key, value pair if it does not exist
		}
		reply.Status = OK
	}
	return nil
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.me != pb.view.Primary {
		reply.Status = ErrWrongServer
		return nil
	}
	value, ok := pb.prevValues[args.Id]
	if ok { // if value already exists
		if args.DoHash { // get value for hashing
			reply.Prev = value
		}
		//otherwise do nothing and just set reply to OK
		reply.Status = OK
		return nil
	}

	if pb.view.Backup != "" { // if backup exists
		ok := call(pb.view.Backup, "PBServer.RedirectPut", args, reply) // forward updates to backup
		if !ok || reply.Status != OK {
			reply.Status = ErrWrongServer
			return nil
		}
	}

	pb.doPut(args, reply)
	reply.Status = OK

	return nil
}

func (pb *PBServer) RedirectGet(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.me != pb.view.Backup { // if current server is not the backup
		reply.Status = ErrWrongServer
	} else { // if current server is backup
		v, ok := pb.prevValues[args.Id] // grab saved value
		if !ok {                        // if value does not exist
			reply.Value = pb.keyValue[args.Key]  // grab value from keyValue
			pb.prevValues[args.Id] = reply.Value // store value
			reply.Status = OK
			return nil
		}
		reply.Value = v
		reply.Status = OK
	}
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.me != pb.view.Primary { // current server not primary
		reply.Status = ErrWrongServer
		return nil
	}

	if value, ok := pb.prevValues[args.Id]; ok { //current operation already saved
		reply.Value = value // set value to value of operation
		reply.Status = OK   // mark as successful
		return nil
	}

	if pb.view.Backup != "" { // current server is primary and a backup exists
		ok := call(pb.view.Backup, "PBServer.RedirectGet", args, reply) // check if backup has correct value
		if !ok || reply.Status != OK {                                  // operation fails
			reply.Status = ErrWrongServer
			return nil
		} else { // operation is successful
			pb.prevValues[args.Id] = reply.Value // save value based on Id
			return nil
		}
	} else { // current server is primary but no backup exists
		reply.Status = OK
		reply.Value = pb.keyValue[args.Key]
		pb.prevValues[args.Id] = reply.Value
		return nil
	}
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	v, _ := pb.vs.Ping(pb.view.Viewnum) // grab current view

	if pb.view.Viewnum != v.Viewnum { // current view has been changed
		preView := pb.view            // store view
		pb.view = v                   // set new view to current view
		if pb.me == pb.view.Primary { // if current server is primary
			if pb.view.Backup != "" { // has a backup
				snapshotArgs := SnapshotArgs{pb.keyValue, pb.prevValues} // grab snapshot
				snapshotReply := SnapshotReply{}
				ok := call(v.Backup, "PBServer.RestoreSnapshot", &snapshotArgs, &snapshotReply)
				if !ok || snapshotReply.Status != OK { // copy from backup fails
					pb.view = preView // go back to previous view
				}
			}
		}
	}

}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.finish = make(chan interface{})
	// Your pb.* initializations here.
	pb.view = viewservice.View{}
	pb.prevValues = make(map[int64]string)
	pb.keyValue = make(map[string]string)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		//DPrintf("%s: wait until all request are done\n", pb.me)
		pb.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}
