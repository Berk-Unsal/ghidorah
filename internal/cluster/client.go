package cluster

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	metricsclient "k8s.io/metrics/pkg/client/clientset/versioned"
)

const (
	kubeconfigEnv       = "KUBECONFIG"
	kubeHostOverrideEnv = "GHIDORAH_KUBE_HOST"
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

// NewRESTConfig prioritizes the local kubeconfig because Ghidorah is optimized
// for fast homelab development. If that path is unavailable or invalid, it
// falls back to the service-account based in-cluster config used once Ghidorah
// runs inside Kubernetes.
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
	kubeconfig, err := kubeconfigPath()
	if err != nil {
		return nil, err
	}

	if _, err := os.Stat(kubeconfig); err != nil {
		return nil, fmt.Errorf("read kubeconfig %q: %w", kubeconfig, err)
	}

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("build local kubeconfig %q: %w", kubeconfig, err)
	}

	if err := applyKubeHostOverride(config); err != nil {
		return nil, err
	}

	return config, nil
}

func kubeconfigPath() (string, error) {
	if kubeconfig := strings.TrimSpace(os.Getenv(kubeconfigEnv)); kubeconfig != "" {
		return kubeconfig, nil
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory: %w", err)
	}

	return filepath.Join(homeDir, ".kube", "config"), nil
}

func applyKubeHostOverride(config *rest.Config) error {
	override := strings.TrimSpace(os.Getenv(kubeHostOverrideEnv))
	if override == "" {
		return nil
	}

	serverURL, err := url.Parse(config.Host)
	if err != nil {
		return fmt.Errorf("parse kubernetes server URL %q: %w", config.Host, err)
	}

	originalHost := serverURL.Hostname()
	if !isLoopbackHost(originalHost) {
		return nil
	}

	rewrittenHost, err := rewriteHost(serverURL, override)
	if err != nil {
		return err
	}

	originalURL := serverURL.String()
	serverURL.Host = rewrittenHost
	config.Host = serverURL.String()

	if config.TLSClientConfig.ServerName == "" {
		config.TLSClientConfig.ServerName = originalHost
	}

	log.Printf("rewrote kubernetes server URL from %s to %s", originalURL, config.Host)

	return nil
}

func rewriteHost(serverURL *url.URL, override string) (string, error) {
	overrideURL, err := url.Parse("//" + strings.TrimPrefix(override, "//"))
	if err != nil || overrideURL.Hostname() == "" {
		return "", fmt.Errorf("parse %s %q: %w", kubeHostOverrideEnv, override, err)
	}

	port := overrideURL.Port()
	if port == "" {
		port = serverURL.Port()
	}

	if port == "" {
		return overrideURL.Hostname(), nil
	}

	return net.JoinHostPort(overrideURL.Hostname(), port), nil
}

func isLoopbackHost(host string) bool {
	normalized := strings.ToLower(strings.TrimSuffix(host, "."))
	if normalized == "localhost" {
		return true
	}

	ip := net.ParseIP(normalized)
	return ip != nil && ip.IsLoopback()
}
