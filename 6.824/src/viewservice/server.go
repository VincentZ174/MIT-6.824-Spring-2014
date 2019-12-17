package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here.
	lastPing map[string]time.Time
	ack      bool
	view     View
	idle     string
}

func (vs *ViewServer) viewChanger(Primary string, Backup string) {
	vs.view = View{vs.view.Viewnum + 1, Primary, Backup}
	vs.ack = false
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()
	server := args.Me
	viewNum := args.Viewnum

	vs.lastPing[server] = time.Now()

	if vs.view.Primary == server {
		if viewNum == 0 { // If Primary gets restarted, backup takes over
			vs.viewChanger(vs.view.Backup, "")
		} else if vs.view.Viewnum == viewNum {
			vs.ack = true
		}
	} else if server != vs.view.Backup {
		if vs.view.Viewnum == 0 { // Start primary for the first time
			vs.viewChanger(server, "")
		} else {
			//assign new server as idle server
			vs.idle = server
		}
	}

	reply.View = vs.view

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()
	reply.View = vs.view

	return nil
}

func (vs *ViewServer) deadServer(expectedTime time.Time) bool {
	now := time.Now()
	return now.Sub(expectedTime) >= DeadPings*PingInterval
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()
	if vs.deadServer(vs.lastPing[vs.idle]) {
		vs.idle = ""
	}
	if vs.deadServer(vs.lastPing[vs.view.Backup]) {
		if vs.ack {
			vs.viewChanger(vs.view.Primary, vs.idle)
			vs.idle = ""
		}
	}
	if vs.deadServer(vs.lastPing[vs.view.Primary]) {
		if vs.ack {
			vs.viewChanger(vs.view.Backup, vs.idle)
			vs.idle = ""
		}
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.view = View{Primary: "", Backup: "", Viewnum: 0}
	vs.lastPing = make(map[string]time.Time)
	vs.ack = false
	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
