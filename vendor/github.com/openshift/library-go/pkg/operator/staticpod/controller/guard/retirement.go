package guard

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
)

// EtcdRetirementAnnotation is written only by the etcd operator after it has
// verified permanent Machine retirement and live membership. It is not a
// readiness indication or an expiring disruption reservation.
const EtcdRetirementAnnotation = "etcd.operator.openshift.io/guard-retirement"

// RetirementDecision binds retirement to exact Node/Machine identities and
// the retained Nodes verified by the producer. Version 1 supports HA 3/5 only.
// Machine deletion cannot be cancelled. Normal etcd scale-up must continue to
// exclude deleting Machines; manual membership overrides are outside this
// contract. Consumers do not infer membership from this record themselves.
type RetirementDecision struct {
	Version         int                  `json:"version"`
	NodeUID         types.UID            `json:"nodeUID"`
	MachineUID      types.UID            `json:"machineUID"`
	DesiredReplicas int                  `json:"desiredReplicas"`
	RetainedNodes   map[string]types.UID `json:"retainedNodes"`
}

func (d RetirementDecision) Validate() error {
	if d.Version != 1 || d.NodeUID == "" || d.MachineUID == "" || (d.DesiredReplicas != 3 && d.DesiredReplicas != 5) || len(d.RetainedNodes) < d.DesiredReplicas {
		return fmt.Errorf("incomplete or unsupported guard retirement decision")
	}
	seen := map[types.UID]bool{}
	for name, uid := range d.RetainedNodes {
		if name == "" || uid == "" || uid == d.NodeUID || seen[uid] {
			return fmt.Errorf("invalid retained Node identity in retirement decision")
		}
		seen[uid] = true
	}
	return nil
}

// NewEtcdRetirementNodeFilter opts a guard consumer into the etcd producer's
// retirement contract. Cached annotations only trigger verification: all
// selected Node identities and decisions are re-read through the live client.
// Errors cause GuardController to restore its unfiltered budget.
func NewEtcdRetirementNodeFilter(getter corev1client.NodesGetter) NodeFilterFunc {
	return func(ctx context.Context, nodes []*corev1.Node) ([]*corev1.Node, error) {
		requested := false
		for _, node := range nodes {
			requested = requested || node.Annotations[EtcdRetirementAnnotation] != ""
		}
		if !requested {
			return nodes, nil
		}
		live := map[string]*corev1.Node{}
		for _, node := range nodes {
			actual, err := getter.Nodes().Get(ctx, node.Name, metav1.GetOptions{})
			if err != nil {
				return nil, fmt.Errorf("verify guard retirement Node: %w", err)
			}
			if node.UID == "" || node.UID != actual.UID {
				return nil, fmt.Errorf("selected Node identity changed during guard retirement")
			}
			live[node.Name] = actual
		}
		retired := map[string]bool{}
		var decisions []RetirementDecision
		for _, node := range nodes {
			// An annotation not observed by the informer is deferred until its
			// next event; a revoked cached annotation is never honored.
			if node.Annotations[EtcdRetirementAnnotation] == "" {
				continue
			}
			raw := live[node.Name].Annotations[EtcdRetirementAnnotation]
			if raw == "" {
				continue
			}
			var decision RetirementDecision
			if err := json.Unmarshal([]byte(raw), &decision); err != nil {
				return nil, fmt.Errorf("invalid guard retirement annotation")
			}
			if err := decision.Validate(); err != nil {
				return nil, err
			}
			if decision.NodeUID != node.UID {
				return nil, fmt.Errorf("guard retirement decision targets a different Node UID")
			}
			retired[node.Name] = true
			decisions = append(decisions, decision)
		}
		for _, decision := range decisions {
			for name, uid := range decision.RetainedNodes {
				node := live[name]
				if node == nil || node.UID != uid || retired[name] {
					return nil, fmt.Errorf("retained Node set changed during guard retirement")
				}
			}
			if len(nodes)-len(retired) < decision.DesiredReplicas {
				return nil, fmt.Errorf("guard retirement would undercut the retained replica count")
			}
		}
		var retained []*corev1.Node
		for _, node := range nodes {
			if !retired[node.Name] {
				retained = append(retained, node)
			}
		}
		return retained, nil
	}
}
