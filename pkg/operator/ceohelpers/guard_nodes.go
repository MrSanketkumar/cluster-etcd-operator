package ceohelpers

import (
	"context"
	"fmt"
	"net"
	"net/url"

	machinelisters "github.com/openshift/client-go/machine/listers/machine/v1beta1"
	"github.com/openshift/cluster-etcd-operator/pkg/etcdcli"
	"github.com/openshift/library-go/pkg/operator/staticpod/controller/guard"
	"github.com/openshift/library-go/pkg/operator/v1helpers"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
)

// NewRetiredMachineGuardNodeFilter excludes only Machine-backed Nodes that are
// deleting, have released the etcd hook, and no longer appear in live membership
// (including learners). New, unhealthy, and temporarily restarting members stay
// protected. Non-Machine-API consumers do not install this filter.
func NewRetiredMachineGuardNodeFilter(
	operatorClient v1helpers.StaticPodOperatorClient,
	memberClient etcdcli.MemberLister,
	machineLister machinelisters.MachineLister,
	machineSelector labels.Selector,
) guard.NodeFilterFunc {
	return func(ctx context.Context, nodes []*corev1.Node) ([]*corev1.Node, error) {
		machines, err := machineLister.List(machineSelector)
		if err != nil {
			return nil, fmt.Errorf("list Machines for guard retirement: %w", err)
		}
		candidates := map[string]sets.Set[string]{}
		for _, machine := range machines {
			if machine.DeletionTimestamp == nil || HasMachineDeletionHook(machine) || machine.Status.NodeRef == nil {
				continue
			}
			for _, node := range nodes {
				if node.Name != machine.Status.NodeRef.Name {
					continue
				}
				if machine.Status.NodeRef.UID == "" || node.UID == "" {
					return nil, fmt.Errorf("cannot establish retiring Node %q identity without both NodeRef and Node UIDs", node.Name)
				}
				if machine.Status.NodeRef.UID != node.UID {
					return nil, fmt.Errorf("Machine NodeRef identity does not match Node %q", node.Name)
				}
				if _, exists := candidates[node.Name]; exists {
					return nil, fmt.Errorf("multiple deleting Machines refer to Node %q", node.Name)
				}
				ips, err := internalIPs(append(append([]corev1.NodeAddress(nil), node.Status.Addresses...), machine.Status.Addresses...))
				if err != nil || ips.Len() == 0 {
					return nil, fmt.Errorf("cannot establish internal IP identity for retiring Node %q", node.Name)
				}
				candidates[node.Name] = ips
			}
		}
		if len(candidates) == 0 {
			return nodes, nil
		}

		desired, err := ReadDesiredControlPlaneReplicasCount(operatorClient)
		if err != nil {
			return nil, err
		}
		// Do not change bootstrap, single-node, two-node/fencing, or unknown
		// topology behavior. Retirement requires a complete supported quorum.
		if desired != 3 && desired != 5 {
			return nodes, nil
		}
		members, err := memberClient.MemberList(ctx)
		if err != nil {
			return nil, fmt.Errorf("read live membership for guard retirement: %w", err)
		}
		allMemberIPs := sets.New[string]()
		var votingMemberIPs []sets.Set[string]
		for _, member := range members {
			if member == nil || member.ID == 0 || len(member.PeerURLs) == 0 {
				return nil, fmt.Errorf("live membership contains an incomplete member")
			}
			ips := sets.New[string]()
			for _, peer := range member.PeerURLs {
				parsed, err := url.Parse(peer)
				if err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" || parsed.Port() == "" {
					return nil, fmt.Errorf("live membership contains an invalid peer URL")
				}
				ip := net.ParseIP(parsed.Hostname())
				if ip == nil {
					return nil, fmt.Errorf("live membership contains a peer without an IP address")
				}
				ips.Insert(ip.String())
				allMemberIPs.Insert(ip.String())
			}
			if !member.IsLearner {
				votingMemberIPs = append(votingMemberIPs, ips)
			}
		}
		if len(votingMemberIPs) < desired {
			return nil, fmt.Errorf("guard retirement requires at least %d live voters, got %d", desired, len(votingMemberIPs))
		}

		var retained []*corev1.Node
		for _, node := range nodes {
			ips, candidate := candidates[node.Name]
			if !candidate || ips.Intersection(allMemberIPs).Len() != 0 {
				retained = append(retained, node)
			}
		}
		if len(retained) == len(nodes) {
			return nodes, nil
		}
		// Every voter must map uniquely to a protected Node. Unknown/new voters
		// must not cause a budget reduction based on an incomplete Node cache.
		mappedNodes := sets.New[string]()
		for _, memberIPs := range votingMemberIPs {
			matches := 0
			matchedNode := ""
			for _, node := range retained {
				ips, err := internalIPs(node.Status.Addresses)
				if err != nil {
					return nil, err
				}
				if ips.Intersection(memberIPs).Len() != 0 {
					matches++
					matchedNode = node.Name
				}
			}
			if matches != 1 {
				return nil, fmt.Errorf("live voter must map to exactly one protected Node, got %d matches", matches)
			}
			if mappedNodes.Has(matchedNode) {
				return nil, fmt.Errorf("multiple live voters map to one protected Node")
			}
			mappedNodes.Insert(matchedNode)
		}
		if len(retained) < len(votingMemberIPs) {
			return nil, fmt.Errorf("not enough protected Nodes for live voters")
		}
		return retained, nil
	}
}

func internalIPs(addresses []corev1.NodeAddress) (sets.Set[string], error) {
	ips := sets.New[string]()
	for _, address := range addresses {
		if address.Type != corev1.NodeInternalIP {
			continue
		}
		ip := net.ParseIP(address.Address)
		if ip == nil {
			return nil, fmt.Errorf("invalid Node InternalIP")
		}
		ips.Insert(ip.String())
	}
	return ips, nil
}
