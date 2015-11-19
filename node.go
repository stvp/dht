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
	defaultCheckInterval = 5 * time.Second
	defaultPollInterval  = time.Second
)

// Node is a single node in a distributed hash table that is coordinated using
// services registered in Consul. Key membership is calculated using rendezvous
// hashing to ensure even distribution of keys and minimal key membership
// changes when a Node fails or otherwise leaves the hash table.
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

// Join creates a new Node for a given service name and adds it to the
// distributed hash table identified by the given name. The given id should be
// unique among all Nodes in the hash table.
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
		// Load the initial hash table state. updateState will not block
		// indefinitely because node.waitIndex is 0.
		err = node.updateState()
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

func (n *Node) updateState() (err error) {
	opts := &api.QueryOptions{WaitIndex: n.waitIndex}
	services, meta, err := n.consul.Catalog().Service(n.serviceName, "", opts)

	if err != nil {
		return err
	}

	ids, err := n.safeIDs(services)
	if err != nil {
		return err
	}

	n.hashTable = rendezvous.New(ids)
	n.waitIndex = meta.LastIndex

	return nil
}

func (n *Node) safeIDs(services []*api.CatalogService) (ids []string, err error) {
	ids = make([]string, len(services))

	var found bool
	for i, service := range services {
		ids[i] = service.ServiceID
		if service.ServiceID == n.serviceID {
			found = true
		}
	}

	// Sanity check to ensure we're still a member of the hash table.
	if !found {
		err = fmt.Errorf("%s is not in the %s service list: %v", n.serviceID, n.serviceName, ids)
	}

	return ids, err
}

func (n *Node) pollState() {
	var err error

	for {
		select {
		case <-n.stop:
			return
		case <-time.After(defaultPollInterval):
			err = n.updateState()
			if err != nil {
				log.Printf("[dht %s %s] error: %s", n.serviceName, n.serviceID, err.Error())
			}
		}
	}
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
