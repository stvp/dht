package dht

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/stvp/rendezvous"
)

const (
	checkInterval = 5 * time.Second
	pollWait      = time.Second
)

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

// Node is a single node in a distributed hash table, coordinated using
// services registered in Consul. Key membership is determined using rendezvous
// hashing to ensure even distribution of keys and minimal key membership
// changes when a Node fails or otherwise leaves the hash table.
//
// Errors encountered when making blocking GET requests to the Consul agent API
// are logged using the log package.
type Node struct {
	// Consul
	serviceName string
	serviceID   string
	consul      *api.Client

	// HTTP health check server
	checkURL      string
	checkListener net.Listener
	checkServer   *http.Server

	// Hash table
	hashTable *rendezvous.Table
	waitIndex uint64

	// Graceful shutdown
	stop chan bool
}

// Join creates a new Node and adds it to the distributed hash table specified
// by the given name. The given id should be unique among all Nodes in the hash
// table.
func Join(name, id string) (node *Node, err error) {
	node = &Node{
		serviceName: name,
		serviceID:   id,
		stop:        make(chan bool),
	}

	node.consul, err = api.NewClient(api.DefaultConfig())
	if err != nil {
		return nil, fmt.Errorf("dht: can't create Consul API client: %s", err)
	}

	node.checkListener, node.checkServer, err = newCheckListenerAndServer()
	if err != nil {
		return nil, fmt.Errorf("dht: can't start HTTP server: %s", err)
	}

	err = node.register()
	if err != nil {
		return nil, fmt.Errorf("dht: can't register %s service: %s", node.serviceName, err)
	}

	err = node.update()
	if err != nil {
		return nil, fmt.Errorf("dht: can't fetch %s services list: %s", node.serviceName, err)
	}

	go node.poll()

	return node, nil
}

func (n *Node) register() (err error) {
	err = n.consul.Agent().ServiceRegister(&api.AgentServiceRegistration{
		Name: n.serviceName,
		ID:   n.serviceID,
		Check: &api.AgentServiceCheck{
			HTTP:     fmt.Sprintf("http://%s", n.checkListener.Addr().String()),
			Interval: checkInterval.String(),
		},
	})
	return err
}

func (n *Node) poll() {
	var err error

	for {
		select {
		case <-n.stop:
			return
		case <-time.After(pollWait):
			err = n.update()
			if err != nil {
				log.Printf("[dht %s %s] error: %s", n.serviceName, n.serviceID, err)
			}
		}
	}
}

// update blocks until the service list changes or until the Consul agent's
// timeout is reached (10 minutes by default).
func (n *Node) update() (err error) {
	opts := &api.QueryOptions{WaitIndex: n.waitIndex}
	serviceEntries, meta, err := n.consul.Health().Service(n.serviceName, "", true, opts)
	if err != nil {
		return err
	}

	ids := make([]string, len(serviceEntries))
	for i, entry := range serviceEntries {
		ids[i] = entry.Service.ID
	}

	n.hashTable = rendezvous.New(ids)
	n.waitIndex = meta.LastIndex

	return nil
}

// Member returns true if the given key belongs to this Node in the distributed
// hash table.
func (n *Node) Member(key string) bool {
	return n.hashTable.Get(key) == n.serviceID
}

// Leave removes the Node from the distributed hash table by de-registering it
// from Consul. Once Leave is called, the Node should be discarded. An error is
// returned if the Node is unable to successfully deregister itself from
// Consul. In that case, Consul's health check for the Node will fail and
// require manual cleanup.
func (n *Node) Leave() (err error) {
	close(n.stop) // stop polling for state
	err = n.consul.Agent().ServiceDeregister(n.serviceID)
	n.checkListener.Close() // stop the health check http server
	return err
}
