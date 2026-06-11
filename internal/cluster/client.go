package cluster

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	metricsclient "k8s.io/metrics/pkg/client/clientset/versioned"
)

// NewClientset returns a Kubernetes client configured for Ghidorah's current
// runtime environment.
func NewClientset() (kubernetes.Interface, error) {
	config, err := NewRESTConfig()
	if err != nil {
		return nil, err
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("build clientset: %w", err)
	}

	return clientset, nil
}

func NewMetricsClientset() (metricsclient.Interface, error) {
	config, err := NewRESTConfig()
	if err != nil {
		return nil, err
	}

	clientset, err := metricsclient.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("build metrics clientset: %w", err)
	}

	return clientset, nil
}

// NewRESTConfig prioritizes the local kubeconfig because Phase 1 is optimized
// for fast homelab development from an Apple Silicon workstation. If that path
// is unavailable or invalid, it falls back to the service-account based
// in-cluster config used once Ghidorah runs inside Kubernetes.
func NewRESTConfig() (*rest.Config, error) {
	localConfig, localErr := newLocalRESTConfig()
	if localErr == nil {
		return localConfig, nil
	}

	inClusterConfig, inClusterErr := rest.InClusterConfig()
	if inClusterErr == nil {
		return inClusterConfig, nil
	}

	return nil, fmt.Errorf(
		"load kubernetes config: %w",
		errors.Join(localErr, inClusterErr),
	)
}

func newLocalRESTConfig() (*rest.Config, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("resolve home directory: %w", err)
	}

	kubeconfig := filepath.Join(homeDir, ".kube", "config")
	if _, err := os.Stat(kubeconfig); err != nil {
		return nil, fmt.Errorf("read kubeconfig %q: %w", kubeconfig, err)
	}

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("build local kubeconfig %q: %w", kubeconfig, err)
	}

	return config, nil
}
