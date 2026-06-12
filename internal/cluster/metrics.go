package cluster

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"ghidorah/internal/events"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	metricsclient "k8s.io/metrics/pkg/client/clientset/versioned"
)

const nodeMetricsInterval = 10 * time.Second

var nodeMetricsFallbackLogOnce sync.Once

func RunNodeMetricsPoller(ctx context.Context, clientset kubernetes.Interface, metricsClient metricsclient.Interface) error {
	ticker := time.NewTicker(nodeMetricsInterval)
	defer ticker.Stop()

	log.Println("node metrics poller started")
	defer log.Println("node metrics poller stopped")

	if err := publishNodeMetrics(ctx, clientset, metricsClient); err != nil {
		log.Printf("poll node metrics: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := publishNodeMetrics(ctx, clientset, metricsClient); err != nil {
				log.Printf("poll node metrics: %v", err)
			}
		}
	}
}

func publishNodeMetrics(ctx context.Context, clientset kubernetes.Interface, metricsClient metricsclient.Interface) error {
	nodes, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list nodes: %w", err)
	}

	metricsByNode := make(map[string]corev1.ResourceList, len(nodes.Items))
	nodeMetrics, err := metricsClient.MetricsV1beta1().NodeMetricses().List(ctx, metav1.ListOptions{})
	if err != nil {
		if !isExpectedNodeMetricsFallback(err) {
			return fmt.Errorf("list node metrics: %w", err)
		}

		nodeMetricsFallbackLogOnce.Do(func() {
			log.Printf("node metrics API unavailable; continuing with readiness/capacity fallback: %v", err)
		})
	} else {
		for _, item := range nodeMetrics.Items {
			metricsByNode[item.Name] = item.Usage
		}
	}

	for _, node := range nodes.Items {
		usage := metricsByNode[node.Name]

		publishMetricsEvent(ctx, events.ClusterEvent{
			Type:                "update",
			Resource:            "nodeMetrics",
			ResourceType:        "NodeMetrics",
			Name:                node.Name,
			Status:              nodeReadyStatus(node),
			CPUUsageMilli:       resourceMilliValue(usage, corev1.ResourceCPU),
			CPUCapacityMilli:    resourceMilliValue(node.Status.Capacity, corev1.ResourceCPU),
			MemoryUsageBytes:    resourceValue(usage, corev1.ResourceMemory),
			MemoryCapacityBytes: resourceValue(node.Status.Capacity, corev1.ResourceMemory),
		})
	}

	return nil
}

func nodeReadyStatus(node corev1.Node) string {
	for _, condition := range node.Status.Conditions {
		if condition.Type != corev1.NodeReady {
			continue
		}

		if condition.Status == corev1.ConditionTrue {
			return "Ready"
		}

		return "NotReady"
	}

	return "NotReady"
}

func resourceMilliValue(resources corev1.ResourceList, name corev1.ResourceName) int64 {
	quantity, ok := resources[name]
	if !ok {
		return 0
	}

	return quantity.MilliValue()
}

func resourceValue(resources corev1.ResourceList, name corev1.ResourceName) int64 {
	quantity, ok := resources[name]
	if !ok {
		return 0
	}

	return quantity.Value()
}

func publishMetricsEvent(ctx context.Context, event events.ClusterEvent) {
	select {
	case events.EventBus <- event:
	case <-ctx.Done():
	default:
		log.Printf(
			"drop metrics event: bus full resourceType=%s name=%s",
			event.ResourceType,
			event.Name,
		)
	}
}

func isExpectedNodeMetricsFallback(err error) bool {
	if apierrors.IsServiceUnavailable(err) || apierrors.IsServerTimeout(err) {
		return true
	}

	return strings.Contains(err.Error(), "the server is currently unable to handle the request")
}
