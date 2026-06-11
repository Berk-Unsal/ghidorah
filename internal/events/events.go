package events

const eventBusSize = 256

// ClusterEvent is the transport shape Ghidorah streams from the cluster watch
// layer to delivery mechanisms such as SSE.
type ClusterEvent struct {
	Type                string         `json:"type"`
	Resource            string         `json:"resource"`
	ResourceType        string         `json:"resourceType"`
	Namespace           string         `json:"namespace,omitempty"`
	Name                string         `json:"name"`
	Phase               string         `json:"phase,omitempty"`
	Status              string         `json:"status,omitempty"`
	ServiceType         string         `json:"serviceType,omitempty"`
	ClusterIP           string         `json:"clusterIP,omitempty"`
	Hosts               []string       `json:"hosts,omitempty"`
	Routes              []IngressRoute `json:"routes,omitempty"`
	CPUUsageMilli       int64          `json:"cpuUsageMilli,omitempty"`
	CPUCapacityMilli    int64          `json:"cpuCapacityMilli,omitempty"`
	MemoryUsageBytes    int64          `json:"memoryUsageBytes,omitempty"`
	MemoryCapacityBytes int64          `json:"memoryCapacityBytes,omitempty"`
}

type IngressRoute struct {
	Host        string `json:"host"`
	Path        string `json:"path,omitempty"`
	ServiceName string `json:"serviceName"`
	ServicePort string `json:"servicePort"`
}

// EventBus is the lean Phase 2 boundary between Kubernetes informers and HTTP
// delivery. Channels are safe for concurrent sends and receives, so this can be
// shared directly without an additional mutex.
var EventBus = make(chan ClusterEvent, eventBusSize)
