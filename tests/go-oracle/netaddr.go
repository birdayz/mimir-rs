package main

import (
	"encoding/json"
	"sort"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/netutil"
	"github.com/grafana/dskit/ring"
	sockaddr "github.com/hashicorp/go-sockaddr"
)

// net-addr-detect: instance address auto-detection on this host, via the exact upstream
// functions (dskit netutil / ring.GetInstanceAddr, go-sockaddr GetPrivateIP).
type netAddrDetectReq struct {
	// Interface names for GetInstanceAddr; nil = the computed default
	// (PrivateNetworkInterfacesWithFallback([eth0 en0])).
	Names       []string `json:"names"`
	EnableInet6 bool     `json:"enable_inet6"`
}

type netAddrPrivIf struct {
	Name string `json:"name"`
	IP   string `json:"ip"`
}

type netAddrDetectResp struct {
	DefaultInterfaceNames []string        `json:"default_interface_names"`
	InstanceAddr          string          `json:"instance_addr"`
	InstanceAddrErr       string          `json:"instance_addr_err"`
	FirstAddrAllInet6     string          `json:"first_addr_all_inet6"`
	FirstAddrAllInet6Err  string          `json:"first_addr_all_inet6_err"`
	PrivateInterfaces     []netAddrPrivIf `json:"private_interfaces"`
	PrivateIP             string          `json:"private_ip"`
	PrivateIPErr          string          `json:"private_ip_err"`
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func cmdNetAddrDetect(input []byte) (any, error) {
	var req netAddrDetectReq
	if err := json.Unmarshal(input, &req); err != nil {
		return nil, err
	}
	logger := log.NewNopLogger()
	resp := netAddrDetectResp{
		DefaultInterfaceNames: netutil.PrivateNetworkInterfacesWithFallback([]string{"eth0", "en0"}, logger),
		PrivateInterfaces:     []netAddrPrivIf{},
	}
	names := req.Names
	if names == nil {
		names = resp.DefaultInterfaceNames
	}
	var err error
	resp.InstanceAddr, err = ring.GetInstanceAddr("", names, logger, req.EnableInet6)
	resp.InstanceAddrErr = errString(err)
	resp.FirstAddrAllInet6, err = netutil.GetFirstAddressOf(nil, logger, true)
	resp.FirstAddrAllInet6Err = errString(err)
	ifs, err := sockaddr.GetPrivateInterfaces()
	if err != nil {
		return nil, err
	}
	for _, i := range ifs {
		resp.PrivateInterfaces = append(resp.PrivateInterfaces, netAddrPrivIf{Name: i.Name, IP: (*sockaddr.ToIPAddr(i.SockAddr)).NetIP().String()})
	}
	resp.PrivateIP, err = sockaddr.GetPrivateIP()
	resp.PrivateIPErr = errString(err)
	return resp, nil
}

// go-sort: Go's sort.Sort (pdqsort, unstable) over integer keys with Less = keys[i] < keys[j];
// returns the resulting permutation of original indices.
type goSortReq struct {
	Keys []int `json:"keys"`
}

type goSortItems struct {
	keys []int
	idx  []int
}

func (s *goSortItems) Len() int           { return len(s.keys) }
func (s *goSortItems) Less(i, j int) bool { return s.keys[i] < s.keys[j] }
func (s *goSortItems) Swap(i, j int) {
	s.keys[i], s.keys[j] = s.keys[j], s.keys[i]
	s.idx[i], s.idx[j] = s.idx[j], s.idx[i]
}

func cmdGoSort(input []byte) (any, error) {
	var req goSortReq
	if err := json.Unmarshal(input, &req); err != nil {
		return nil, err
	}
	items := &goSortItems{keys: req.Keys, idx: make([]int, len(req.Keys))}
	for i := range items.idx {
		items.idx[i] = i
	}
	sort.Sort(items)
	return map[string][]int{"perm": items.idx}, nil
}
