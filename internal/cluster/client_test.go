package cluster

import (
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

func TestApplyKubeHostOverrideRewritesLoopbackServer(t *testing.T) {
	t.Setenv(kubeHostOverrideEnv, "host.docker.internal")

	config := &rest.Config{
		Host: "https://127.0.0.1:26443",
	}

	if err := applyKubeHostOverride(config); err != nil {
		t.Fatalf("applyKubeHostOverride returned error: %v", err)
	}

	if config.Host != "https://host.docker.internal:26443" {
		t.Fatalf("config.Host = %q, want %q", config.Host, "https://host.docker.internal:26443")
	}

	if config.TLSClientConfig.ServerName != "127.0.0.1" {
		t.Fatalf("ServerName = %q, want %q", config.TLSClientConfig.ServerName, "127.0.0.1")
	}
}

func TestApplyKubeHostOverrideLeavesRemoteServerAlone(t *testing.T) {
	t.Setenv(kubeHostOverrideEnv, "host.docker.internal")

	config := &rest.Config{
		Host: "https://cluster.example.test:6443",
	}

	if err := applyKubeHostOverride(config); err != nil {
		t.Fatalf("applyKubeHostOverride returned error: %v", err)
	}

	if config.Host != "https://cluster.example.test:6443" {
		t.Fatalf("config.Host = %q, want %q", config.Host, "https://cluster.example.test:6443")
	}

	if config.TLSClientConfig.ServerName != "" {
		t.Fatalf("ServerName = %q, want empty", config.TLSClientConfig.ServerName)
	}
}

func TestApplyKubeHostOverridePreservesConfiguredServerName(t *testing.T) {
	t.Setenv(kubeHostOverrideEnv, "host.docker.internal")

	config := &rest.Config{
		Host: "https://localhost:26443",
		TLSClientConfig: rest.TLSClientConfig{
			ServerName: "custom.local",
		},
	}

	if err := applyKubeHostOverride(config); err != nil {
		t.Fatalf("applyKubeHostOverride returned error: %v", err)
	}

	if config.Host != "https://host.docker.internal:26443" {
		t.Fatalf("config.Host = %q, want %q", config.Host, "https://host.docker.internal:26443")
	}

	if config.TLSClientConfig.ServerName != "custom.local" {
		t.Fatalf("ServerName = %q, want %q", config.TLSClientConfig.ServerName, "custom.local")
	}
}

func TestIsExpectedNodeMetricsFallback(t *testing.T) {
	err := apierrors.NewServiceUnavailable("the server is currently unable to handle the request")

	if !isExpectedNodeMetricsFallback(err) {
		t.Fatal("expected service unavailable metrics error to trigger fallback")
	}

	serverTimeout := apierrors.NewServerTimeout(schema.GroupResource{Group: "metrics.k8s.io", Resource: "nodes"}, "list", 1)
	if !isExpectedNodeMetricsFallback(serverTimeout) {
		t.Fatal("expected server timeout metrics error to trigger fallback")
	}
}
