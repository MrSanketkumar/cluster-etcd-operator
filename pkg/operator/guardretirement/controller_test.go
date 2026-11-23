package guardretirement

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	machinev1 "github.com/openshift/api/machine/v1beta1"
	operatorv1 "github.com/openshift/api/operator/v1"
	machinefake "github.com/openshift/client-go/machine/clientset/versioned/fake"
	machinelisters "github.com/openshift/client-go/machine/listers/machine/v1beta1"
	"github.com/openshift/cluster-etcd-operator/pkg/operator/ceohelpers"
	"github.com/openshift/library-go/pkg/operator/staticpod/controller/guard"
	"github.com/openshift/library-go/pkg/operator/v1helpers"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
)

type testMembers struct {
	members []*etcdserverpb.Member
	err     error
}

func (m *testMembers) MemberList(context.Context) ([]*etcdserverpb.Member, error) {
	return m.members, m.err
}
func (m *testMembers) VotingMemberList(context.Context) ([]*etcdserverpb.Member, error) {
	panic("retirement must read the full membership")
}

func TestRetirementPublicationAndRevocation(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"publish", "membership unavailable", "member returns", "learner returns", "hook restored", "Machine replaced", "Machine addresses changed", "Machine NodeRef changed", "Node update conflicts"} {
		t.Run(mode, func(t *testing.T) {
			c, kube, machines, members := publicationFixture(t)
			if err := c.Sync(ctx, nil); err != nil {
				t.Fatal(err)
			}
			node, err := kube.CoreV1().Nodes().Get(ctx, "node-0", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			var decision guard.RetirementDecision
			if err := json.Unmarshal([]byte(node.Annotations[guard.EtcdRetirementAnnotation]), &decision); err != nil {
				t.Fatal(err)
			}
			if err := decision.Validate(); err != nil || decision.NodeUID != node.UID || decision.MachineUID != "machine-0" || len(decision.RetainedNodes) != 3 {
				t.Fatalf("unexpected published decision: %+v, error=%v", decision, err)
			}
			if mode == "publish" {
				if err := c.Sync(ctx, nil); err != nil {
					t.Fatal(err)
				}
				return
			}
			switch mode {
			case "membership unavailable":
				members.err = errors.New("membership unavailable")
			case "member returns", "learner returns":
				members.members = append(members.members, &etcdserverpb.Member{ID: 9, Name: "node-0", IsLearner: mode == "learner returns", PeerURLs: []string{"https://192.0.2.10:2380"}})
			case "hook restored", "Machine replaced", "Machine addresses changed", "Machine NodeRef changed":
				machine, err := machines.MachineV1beta1().Machines("openshift-machine-api").Get(ctx, "machine-0", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				switch mode {
				case "hook restored":
					machine.Spec.LifecycleHooks.PreDrain = []machinev1.LifecycleHook{{Name: ceohelpers.MachineDeletionHookName, Owner: ceohelpers.MachineDeletionHookOwner}}
				case "Machine replaced":
					machine.UID = "another-machine"
				case "Machine addresses changed":
					machine.Status.Addresses[0].Address = "192.0.2.99"
				case "Machine NodeRef changed":
					machine.Status.NodeRef.UID = "different-node"
				}
				if _, err := machines.MachineV1beta1().Machines("openshift-machine-api").Update(ctx, machine, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
			case "Node update conflicts":
				members.err = errors.New("membership unavailable")
				kube.PrependReactor("update", "nodes", func(action clienttesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "nodes"}, "node-0", errors.New("concurrent update"))
				})
			}
			err = c.Sync(ctx, nil)
			if mode != "member returns" && mode != "learner returns" && err == nil {
				t.Fatal("expected uncertainty/conflict to report an error")
			}
			if (mode == "member returns" || mode == "learner returns") && err != nil {
				t.Fatal(err)
			}
			node, err = kube.CoreV1().Nodes().Get(ctx, "node-0", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if mode != "Node update conflicts" && node.Annotations[guard.EtcdRetirementAnnotation] != "" {
				t.Fatal("obsolete retirement annotation was not revoked")
			}
			// Conflicts are retried with fresh state; they are not forced.
			if mode == "Node update conflicts" {
				kube.ReactionChain = nil
				kube.AddReactor("*", "*", clienttesting.ObjectReaction(kube.Tracker()))
				if err := c.Sync(ctx, nil); err == nil {
					t.Fatal("membership error should still be reported while revoking")
				}
				node, err := kube.CoreV1().Nodes().Get(ctx, "node-0", metav1.GetOptions{})
				if err != nil || node.Annotations[guard.EtcdRetirementAnnotation] != "" {
					t.Fatalf("retry failed to revoke: %v", err)
				}
			}
		})
	}
}

func publicationFixture(t *testing.T) (*Controller, *fake.Clientset, *machinefake.Clientset, *testMembers) {
	t.Helper()
	var objects []runtime.Object
	members := &testMembers{}
	for i := 0; i < 4; i++ {
		name, address := fmt.Sprintf("node-%d", i), fmt.Sprintf("192.0.2.%d", i+10)
		objects = append(objects, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(fmt.Sprintf("node-uid-%d", i))},
			Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: address}}}})
		if i > 0 {
			members.members = append(members.members, &etcdserverpb.Member{ID: uint64(i), Name: name, PeerURLs: []string{fmt.Sprintf("https://%s:2380", address)}})
		}
	}
	now := metav1.Now()
	machine := &machinev1.Machine{ObjectMeta: metav1.ObjectMeta{Name: "machine-0", Namespace: "openshift-machine-api", UID: "machine-0", DeletionTimestamp: &now, Finalizers: []string{"test"}},
		Status: machinev1.MachineStatus{NodeRef: &corev1.ObjectReference{Name: "node-0", UID: "node-uid-0"}, Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "192.0.2.10"}}}}
	index := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	if err := index.Add(machine.DeepCopy()); err != nil {
		t.Fatal(err)
	}
	lister := machinelisters.NewMachineLister(index)
	machines := machinefake.NewSimpleClientset(machine.DeepCopy())
	kube := fake.NewSimpleClientset(objects...)
	operator := v1helpers.NewFakeStaticPodOperatorClient(&operatorv1.StaticPodOperatorSpec{OperatorSpec: operatorv1.OperatorSpec{ObservedConfig: runtime.RawExtension{Raw: []byte(`{"controlPlane":{"replicas":3}}`)}}}, &operatorv1.StaticPodOperatorStatus{}, nil, nil)
	c := &Controller{operator: operator, machines: lister, machineClient: machines.MachineV1beta1().Machines("openshift-machine-api"),
		kube: kube, nodeSelector: labels.Everything(), machineSelector: labels.Everything(),
		filter: ceohelpers.NewRetiredMachineGuardNodeFilter(operator, members, lister, labels.Everything())}
	return c, kube, machines, members
}
