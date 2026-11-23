package guardretirement

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	machineclient "github.com/openshift/client-go/machine/clientset/versioned/typed/machine/v1beta1"
	machinelisters "github.com/openshift/client-go/machine/listers/machine/v1beta1"
	"github.com/openshift/cluster-etcd-operator/pkg/etcdcli"
	"github.com/openshift/cluster-etcd-operator/pkg/operator/ceohelpers"
	"github.com/openshift/library-go/pkg/controller/factory"
	"github.com/openshift/library-go/pkg/operator/events"
	"github.com/openshift/library-go/pkg/operator/staticpod/controller/guard"
	"github.com/openshift/library-go/pkg/operator/v1helpers"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// Controller publishes a narrow permanent-retirement decision to guard owners.
// It does not delete other components' guards or update their PDBs.
type Controller struct {
	operator        v1helpers.StaticPodOperatorClient
	filter          guard.NodeFilterFunc
	machines        machinelisters.MachineLister
	machineClient   machineclient.MachineInterface
	kube            kubernetes.Interface
	nodeSelector    labels.Selector
	machineSelector labels.Selector
}

func NewController(operator v1helpers.StaticPodOperatorClient, members etcdcli.MemberLister,
	machineClient machineclient.MachineInterface, kube kubernetes.Interface,
	machineInformer, nodeInformer cache.SharedIndexInformer,
	machineSelector, nodeSelector labels.Selector, recorder events.Recorder,
) factory.Controller {
	machines := machinelisters.NewMachineLister(machineInformer.GetIndexer())
	c := &Controller{
		operator: operator, machines: machines, machineClient: machineClient,
		kube: kube, nodeSelector: nodeSelector, machineSelector: machineSelector,
		filter: ceohelpers.NewRetiredMachineGuardNodeFilter(operator, members, machines, machineSelector),
	}
	return factory.New().WithSync(c.Sync).WithSyncDegradedOnError(operator).
		WithInformers(machineInformer, nodeInformer, operator.Informer()).
		ResyncEvery(time.Minute).ToController("GuardRetirementController", recorder)
}

func (c *Controller) Sync(ctx context.Context, _ factory.SyncContext) error {
	list, err := c.kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: c.nodeSelector.String()})
	if err != nil {
		return err
	}
	nodes := make([]*corev1.Node, 0, len(list.Items))
	for i := range list.Items {
		nodes = append(nodes, &list.Items[i])
	}
	wanted, decisionErr := c.decisions(ctx, nodes)
	if decisionErr != nil {
		// Revoke prior decisions rather than leaving a reduced policy after
		// membership/identity verification becomes uncertain.
		wanted = nil
	}
	errs := []error{decisionErr}
	for _, node := range nodes {
		actual, err := c.kube.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if actual.UID == "" || actual.UID != node.UID {
			errs = append(errs, fmt.Errorf("Node identity changed while publishing guard retirement"))
			continue
		}
		value := wanted[node.Name]
		if actual.Annotations[guard.EtcdRetirementAnnotation] == value {
			continue
		}
		copy := actual.DeepCopy()
		if value == "" {
			delete(copy.Annotations, guard.EtcdRetirementAnnotation)
		} else {
			if copy.Annotations == nil {
				copy.Annotations = map[string]string{}
			}
			copy.Annotations[guard.EtcdRetirementAnnotation] = value
		}
		// ResourceVersion and UID are preserved. A concurrent Node update
		// conflicts and is retried by reconciliation; no spec is forced.
		if _, err := c.kube.CoreV1().Nodes().Update(ctx, copy, metav1.UpdateOptions{}); err != nil {
			errs = append(errs, err)
		}
	}
	return utilerrors.NewAggregate(errs)
}

func (c *Controller) decisions(ctx context.Context, nodes []*corev1.Node) (map[string]string, error) {
	retained, err := c.filter(ctx, nodes)
	if err != nil {
		return nil, err
	}
	if len(retained) == len(nodes) {
		return nil, nil
	}
	desired, err := ceohelpers.ReadDesiredControlPlaneReplicasCount(c.operator)
	if err != nil {
		return nil, err
	}
	protected := map[string]types.UID{}
	for _, node := range retained {
		if node.UID == "" {
			return nil, fmt.Errorf("retained Node lacks UID")
		}
		protected[node.Name] = node.UID
	}
	machines, err := c.machines.List(c.machineSelector)
	if err != nil {
		return nil, err
	}
	wanted := map[string]string{}
	for _, node := range nodes {
		if _, ok := protected[node.Name]; ok {
			continue
		}
		found := false
		for _, cached := range machines {
			if cached.Status.NodeRef == nil || cached.Status.NodeRef.Name != node.Name {
				continue
			}
			actual, err := c.machineClient.Get(ctx, cached.Name, metav1.GetOptions{})
			if err != nil {
				return nil, err
			}
			if cached.UID == "" || actual.UID != cached.UID || actual.DeletionTimestamp == nil ||
				ceohelpers.HasMachineDeletionHook(actual) || actual.Status.NodeRef == nil ||
				actual.Status.NodeRef.Name != node.Name || actual.Status.NodeRef.UID != node.UID ||
				!reflect.DeepEqual(actual.Status.Addresses, cached.Status.Addresses) {
				return nil, fmt.Errorf("Machine retirement identity/state changed before publication")
			}
			decision := guard.RetirementDecision{Version: 1, NodeUID: node.UID, MachineUID: actual.UID,
				DesiredReplicas: desired, RetainedNodes: protected}
			if err := decision.Validate(); err != nil {
				return nil, err
			}
			raw, err := json.Marshal(decision)
			if err != nil {
				return nil, err
			}
			wanted[node.Name] = string(raw)
			found = true
			break
		}
		if !found {
			return nil, fmt.Errorf("retiring Node lacks a matching Machine")
		}
	}
	return wanted, nil
}
