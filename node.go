package dht

import (
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/stvp/rendezvous"
)

const (
	defaultCheckInterval = 5 * time.Second
)

// Node is a single node in a distributed hash table that is coordinated using
// services registered in Consul. Key membership by nodes is calculated using
// rendezvous hashing to ensure even distribution and minimal key membership
// changes when a node fails or leaves.
type Node struct {
	// Consul service config
	serviceName string
	serviceID   string

	// Consul agent client
	consul *api.Client

	// Consul HTTP health check server
	checkURL      string
	checkListener net.Listener
	checkServer   *http.Server

	// Hash table
	hashMutex sync.Mutex
	hashTable *rendezvous.Table
	waitIndex uint64

	stop chan bool
}

// Join creates a new Node for a given service name and adds it to the
// distributed hash table identified by the given name. The given id should be
// unique among all other nodes with the same name.
func Join(name, id string) (node *Node, err error) {
	node = &Node{
		serviceName: name,
		serviceID:   id,
		stop:        make(chan bool),
	}

	node.consul, err = api.NewClient(api.DefaultConfig())

	if err == nil {
		node.checkListener, node.checkServer, err = newCheckListenerAndServer()
	}

	if err == nil {
		err = node.register()
	}

	if err == nil {
		// This also initializes node.hashTable. waitAndLoadState will not block
		// here because node.waitIndex is 0.
		err = node.waitAndLoadState()
	}

	if err == nil {
		go node.pollState()
	}

	return node, err
}

func newCheckListenerAndServer() (listener net.Listener, server *http.Server, err error) {
	listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return listener, nil, err
	}

	server = &http.Server{
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
		Handler: http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
			fmt.Fprintf(resp, "OK")
		}),
	}

	// When the listener is closed, this goroutine returns.
	go server.Serve(listener)

	return listener, server, err
}

func (n *Node) register() (err error) {
	err = n.consul.Agent().ServiceRegister(&api.AgentServiceRegistration{
		Name: n.serviceName,
		ID:   n.serviceID,
		Check: &api.AgentServiceCheck{
			HTTP:     fmt.Sprintf("http://%s", n.checkListener.Addr().String()),
			Interval: defaultCheckInterval.String(),
		},
	})
	return err
}

// waitAndLoadState blocks until the service catalog changes or until the
// agent's timeout is reached.
func (n *Node) waitAndLoadState() (err error) {
	opts := &api.QueryOptions{WaitIndex: n.waitIndex}
	services, meta, err := n.consul.Catalog().Service(n.serviceName, "", opts)
	if err != nil {
		return err
	}

	ids := make([]string, len(services))
	for i, service := range services {
		ids[i] = service.ServiceID
	}

	// Sanity check
	found := false
	for _, id := range ids {
		if id == n.serviceID {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("%s is not in the %s service list: %v", n.serviceID, n.serviceName, ids)
	}

	n.hashMutex.Lock()
	n.hashTable = rendezvous.New(ids)
	n.waitIndex = meta.LastIndex
	n.hashMutex.Unlock()

	return nil
}

func (n *Node) pollState() {
	var err error

	for {
		select {
		case <-n.stop:
			return
		default:
			// TODO how should we surface errors?
			err = n.waitAndLoadState()
			if err != nil {
				// Don't flood the agent with requests if it's returning errors.
				time.Sleep(2 * time.Second)
			}
		}
	}
}

// Owns returns true if the given key belongs to this Node in the distributed
// hash table.
func (n *Node) Owns(key string) bool {
	return n.hashTable.Get(key) == n.serviceID
}

// Leave removes this Node from the distributed hash table by de-registering it
// from Consul. Once Leave is called, the Node should be discarded.
func (n *Node) Leave() (err error) {
	close(n.stop) // stop polling for state
	err = n.consul.Agent().ServiceDeregister(n.serviceID)
	n.checkListener.Close() // stop the health check http server
	return err
}
