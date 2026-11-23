package ceohelpers

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	machinev1 "github.com/openshift/api/machine/v1beta1"
	operatorv1 "github.com/openshift/api/operator/v1"
	machinelisters "github.com/openshift/client-go/machine/listers/machine/v1beta1"
	"github.com/openshift/library-go/pkg/operator/v1helpers"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
)

type guardMemberLister struct {
	members []*etcdserverpb.Member
	err     error
}

func (l guardMemberLister) MemberList(context.Context) ([]*etcdserverpb.Member, error) {
	return l.members, l.err
}

func (l guardMemberLister) VotingMemberList(context.Context) ([]*etcdserverpb.Member, error) {
	panic("guard retirement must use the full live member list, including learners")
}

func TestRetiredMachineGuardNodeFilter(t *testing.T) {
	for _, tc := range []struct {
		name        string
		desired     int
		mutate      func([]*corev1.Node, *machinev1.Machine, *guardMemberLister)
		wantRetired bool
		wantError   bool
		extraNode   bool
	}{
		{name: "retired member in a three voter cluster", desired: 3, wantRetired: true},
		{name: "retired member in a five voter cluster", desired: 5, wantRetired: true},
		{name: "additional pending Node is not excluded", desired: 3, wantRetired: true, extraNode: true},
		{name: "four real voters remain protected", desired: 3, mutate: func(nodes []*corev1.Node, _ *machinev1.Machine, l *guardMemberLister) {
			l.members = append(l.members, &etcdserverpb.Member{ID: 100, PeerURLs: []string{"https://192.0.2.10:2380"}})
		}},
		{name: "learner on outgoing node remains protected", desired: 3, mutate: func(_ []*corev1.Node, _ *machinev1.Machine, l *guardMemberLister) {
			l.members = append(l.members, &etcdserverpb.Member{ID: 100, IsLearner: true, PeerURLs: []string{"https://192.0.2.10:2380"}})
		}},
		{name: "hook still present", desired: 3, mutate: func(_ []*corev1.Node, m *machinev1.Machine, _ *guardMemberLister) {
			m.Spec.LifecycleHooks.PreDrain = []machinev1.LifecycleHook{{Name: MachineDeletionHookName, Owner: MachineDeletionHookOwner}}
		}},
		{name: "machine not deleting", desired: 3, mutate: func(_ []*corev1.Node, m *machinev1.Machine, _ *guardMemberLister) { m.DeletionTimestamp = nil }},
		{name: "missing NodeRef", desired: 3, mutate: func(_ []*corev1.Node, m *machinev1.Machine, _ *guardMemberLister) { m.Status.NodeRef = nil }},
		{name: "Node identity mismatch", desired: 3, wantError: true, mutate: func(_ []*corev1.Node, m *machinev1.Machine, _ *guardMemberLister) {
			m.Status.NodeRef.UID = "another-node"
		}},
		{name: "missing NodeRef UID", desired: 3, wantError: true, mutate: func(_ []*corev1.Node, m *machinev1.Machine, _ *guardMemberLister) {
			m.Status.NodeRef.UID = ""
		}},
		{name: "missing Node UID", desired: 3, wantError: true, mutate: func(nodes []*corev1.Node, _ *machinev1.Machine, _ *guardMemberLister) {
			nodes[0].UID = ""
		}},
		{name: "membership lookup fails", desired: 3, wantError: true, mutate: func(_ []*corev1.Node, _ *machinev1.Machine, l *guardMemberLister) {
			l.err = errors.New("membership unavailable")
		}},
		{name: "empty membership", desired: 3, wantError: true, mutate: func(_ []*corev1.Node, _ *machinev1.Machine, l *guardMemberLister) { l.members = nil }},
		{name: "insufficient voters during bootstrap", desired: 3, wantError: true, mutate: func(_ []*corev1.Node, _ *machinev1.Machine, l *guardMemberLister) { l.members = l.members[:2] }},
		{name: "nil member", desired: 3, wantError: true, mutate: func(_ []*corev1.Node, _ *machinev1.Machine, l *guardMemberLister) { l.members = append(l.members, nil) }},
		{name: "member without ID", desired: 3, wantError: true, mutate: func(_ []*corev1.Node, _ *machinev1.Machine, l *guardMemberLister) { l.members[0].ID = 0 }},
		{name: "two voters map to the same Node", desired: 3, wantError: true, mutate: func(_ []*corev1.Node, _ *machinev1.Machine, l *guardMemberLister) {
			l.members[1].PeerURLs = l.members[0].PeerURLs
		}},
		{name: "member without peer identity", desired: 3, wantError: true, mutate: func(_ []*corev1.Node, _ *machinev1.Machine, l *guardMemberLister) { l.members[0].PeerURLs = nil }},
		{name: "non-IP peer identity", desired: 3, wantError: true, mutate: func(_ []*corev1.Node, _ *machinev1.Machine, l *guardMemberLister) {
			l.members[0].PeerURLs = []string{"https://member.example.invalid:2380"}
		}},
		{name: "malformed peer URL", desired: 3, wantError: true, mutate: func(_ []*corev1.Node, _ *machinev1.Machine, l *guardMemberLister) {
			l.members[0].PeerURLs = []string{"://broken"}
		}},
		{name: "unknown voter is not in Node cache", desired: 3, wantError: true, mutate: func(_ []*corev1.Node, _ *machinev1.Machine, l *guardMemberLister) {
			l.members[0].PeerURLs = []string{"https://192.0.2.200:2380"}
		}},
		{name: "missing retiring IP identity", desired: 3, wantError: true, mutate: func(nodes []*corev1.Node, m *machinev1.Machine, _ *guardMemberLister) {
			nodes[0].Status.Addresses = nil
			m.Status.Addresses = nil
		}},
		{name: "unknown desired topology", desired: 0},
		{name: "single node topology unchanged", desired: 1},
		{name: "two node topology unchanged", desired: 2},
		{name: "unhealthy active member remains protected", desired: 3, wantRetired: true, mutate: func(nodes []*corev1.Node, _ *machinev1.Machine, _ *guardMemberLister) {
			nodes[1].Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}
		}},
		{name: "dual stack learner identity prevents retirement", desired: 3, mutate: func(nodes []*corev1.Node, m *machinev1.Machine, l *guardMemberLister) {
			v6 := corev1.NodeAddress{Type: corev1.NodeInternalIP, Address: "2001:db8::10"}
			nodes[0].Status.Addresses = append(nodes[0].Status.Addresses, v6)
			m.Status.Addresses = append(m.Status.Addresses, v6)
			l.members = append(l.members, &etcdserverpb.Member{ID: 100, IsLearner: true, PeerURLs: []string{"https://[2001:0db8:0:0:0:0:0:10]:2380"}})
		}},
		{name: "IPv6 retired identity", desired: 3, wantRetired: true, mutate: func(nodes []*corev1.Node, m *machinev1.Machine, l *guardMemberLister) {
			for i, node := range nodes {
				address := fmt.Sprintf("2001:db8::%x", i+16)
				node.Status.Addresses = []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: address}}
				if i == 0 {
					m.Status.Addresses = node.Status.Addresses
				} else {
					l.members[i-1].PeerURLs = []string{fmt.Sprintf("https://[%s]:2380", address)}
				}
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			voters := tc.desired
			if voters < 3 {
				voters = 3
			}
			var nodes []*corev1.Node
			lister := guardMemberLister{}
			nodeCount := voters + 1
			if tc.extraNode {
				nodeCount++
			}
			for i := 0; i < nodeCount; i++ {
				ip := fmt.Sprintf("192.0.2.%d", i+10)
				nodes = append(nodes, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("master-%d", i), UID: types.UID(fmt.Sprintf("node-%d", i))}, Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: ip}}}})
				if i > 0 && i <= voters {
					lister.members = append(lister.members, &etcdserverpb.Member{ID: uint64(i), PeerURLs: []string{fmt.Sprintf("https://%s:2380", ip)}})
				}
			}
			now := metav1.Now()
			machine := &machinev1.Machine{ObjectMeta: metav1.ObjectMeta{Name: "retiring", DeletionTimestamp: &now}, Status: machinev1.MachineStatus{NodeRef: &corev1.ObjectReference{Name: nodes[0].Name, UID: nodes[0].UID}, Addresses: nodes[0].Status.Addresses}}
			if tc.mutate != nil {
				tc.mutate(nodes, machine, &lister)
			}
			index := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
			if err := index.Add(machine); err != nil {
				t.Fatal(err)
			}
			operator := v1helpers.NewFakeStaticPodOperatorClient(&operatorv1.StaticPodOperatorSpec{OperatorSpec: operatorv1.OperatorSpec{ObservedConfig: runtime.RawExtension{Raw: []byte(fmt.Sprintf(`{"controlPlane":{"replicas":%d}}`, tc.desired))}}}, &operatorv1.StaticPodOperatorStatus{}, nil, nil)
			filter := NewRetiredMachineGuardNodeFilter(operator, lister, machinelisters.NewMachineLister(index), labels.Everything())
			got, err := filter(context.Background(), nodes)
			if (err != nil) != tc.wantError {
				t.Fatalf("error = %v, wantError = %t", err, tc.wantError)
			}
			if err != nil {
				return
			}
			want := nodes
			if tc.wantRetired {
				want = nodes[1:]
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("protected Nodes differ: got %d, want %d", len(got), len(want))
			}
		})
	}
}
