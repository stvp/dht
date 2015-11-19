package dht

import (
	"net/url"
	"strconv"
	"testing"

	"github.com/hashicorp/consul/api"
	"github.com/stvp/tempconsul"
)

func apiClient() (client *api.Client) {
	client, _ = api.NewClient(api.DefaultConfig())
	return client
}

func startConsul() (server *tempconsul.Server, err error) {
	server = &tempconsul.Server{}
	return server, server.Start()
}

func servicesCount(name string) (count int, err error) {
	services, _, err := apiClient().Catalog().Service(name, "", nil)
	return len(services), err
}

func TestJoinLeave(t *testing.T) {
	// No Consul agent
	_, err := Join("test", "a")
	_, ok := err.(*url.Error)
	if !ok {
		t.Errorf("expected url.Error, got: %#v", err)
	}

	// Start Consul agent
	server, err := startConsul()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Term()

	// Valid join
	node, err := Join("test", "a")
	if err != nil {
		t.Fatal(err)
	}

	count, err := servicesCount("test")
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("expected 1 service registered, got %d", count)
	}

	// Leave
	err = node.Leave()
	if err != nil {
		t.Error(err)
	}

	count, err = servicesCount("test")
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("expected 0 service registered, got %d", count)
	}
}

func TestOwns(t *testing.T) {
	server, err := startConsul()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Term()

	n := 3
	nodes := make([]*Node, n)
	for i := 0; i < n; i++ {
		nodes[i], err = Join("test", strconv.Itoa(i))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Ensure nodes have the latest state
	for _, node := range nodes {
		err := node.updateState()
		if err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		key  string
		owns []bool
	}{
		{"", []bool{true, false, false}},
		{"a", []bool{true, false, false}},
		{"b", []bool{false, false, true}},
		{"d9edf13e917c4f0f66be0e80cc30060e", []bool{false, true, false}},
		{"a2a9538886f1df96be9e5b52b14b404a", []bool{false, false, true}},
	}

	for _, test := range tests {
		for i, node := range nodes {
			expect := test.owns[i]
			got := node.Owns(test.key)
			if got != expect {
				t.Errorf("nodes[%d].Owns(%#v): expected %v, got %v", i, test.key, expect, got)
			}
		}
	}

	// Clean up
	for _, node := range nodes {
		err = node.Leave()
		if err != nil {
			t.Error(err)
		}
	}
}
