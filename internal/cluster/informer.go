package cluster

import (
	"context"
	"fmt"
	"log"

	"ghidorah/internal/events"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// RunInformers starts Ghidorah's Kubernetes watch loop: shared informers
// scoped to the resource types currently powering the dashboard heads.
func RunInformers(ctx context.Context, clientset kubernetes.Interface) error {
	// SharedInformerFactory owns the LIST/WATCH connection to the Kubernetes
	// API and fans object changes out to resource-specific informers. A zero
	// resync period means Ghidorah only reacts to real watch events after the
	// initial list, keeping the spike lean and event-driven.
	factory := informers.NewSharedInformerFactory(clientset, 0)
	podInformer := factory.Core().V1().Pods().Informer()
	serviceInformer := factory.Core().V1().Services().Informer()
	ingressInformer := factory.Networking().V1().Ingresses().Informer()

	if _, err := podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod, ok := objectFromEvent[corev1.Pod](obj)
			if !ok {
				runtime.HandleError(fmt.Errorf("pod add event contained %T", obj))
				return
			}

			publishPodEvent(ctx, "add", pod)
		},
		UpdateFunc: func(_, newObj interface{}) {
			pod, ok := objectFromEvent[corev1.Pod](newObj)
			if !ok {
				runtime.HandleError(fmt.Errorf("pod update event contained %T", newObj))
				return
			}

			publishPodEvent(ctx, "update", pod)
		},
		DeleteFunc: func(obj interface{}) {
			pod, ok := objectFromEvent[corev1.Pod](obj)
			if !ok {
				runtime.HandleError(fmt.Errorf("pod delete event contained %T", obj))
				return
			}

			publishPodEvent(ctx, "delete", pod)
		},
	}); err != nil {
		return fmt.Errorf("register pod event handler: %w", err)
	}

	if _, err := serviceInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			service, ok := objectFromEvent[corev1.Service](obj)
			if !ok {
				runtime.HandleError(fmt.Errorf("service add event contained %T", obj))
				return
			}

			publishServiceEvent(ctx, "add", service)
		},
		UpdateFunc: func(_, newObj interface{}) {
			service, ok := objectFromEvent[corev1.Service](newObj)
			if !ok {
				runtime.HandleError(fmt.Errorf("service update event contained %T", newObj))
				return
			}

			publishServiceEvent(ctx, "update", service)
		},
		DeleteFunc: func(obj interface{}) {
			service, ok := objectFromEvent[corev1.Service](obj)
			if !ok {
				runtime.HandleError(fmt.Errorf("service delete event contained %T", obj))
				return
			}

			publishServiceEvent(ctx, "delete", service)
		},
	}); err != nil {
		return fmt.Errorf("register service event handler: %w", err)
	}

	if _, err := ingressInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			ingress, ok := objectFromEvent[networkingv1.Ingress](obj)
			if !ok {
				runtime.HandleError(fmt.Errorf("ingress add event contained %T", obj))
				return
			}

			publishIngressEvent(ctx, "add", ingress)
		},
		UpdateFunc: func(_, newObj interface{}) {
			ingress, ok := objectFromEvent[networkingv1.Ingress](newObj)
			if !ok {
				runtime.HandleError(fmt.Errorf("ingress update event contained %T", newObj))
				return
			}

			publishIngressEvent(ctx, "update", ingress)
		},
		DeleteFunc: func(obj interface{}) {
			ingress, ok := objectFromEvent[networkingv1.Ingress](obj)
			if !ok {
				runtime.HandleError(fmt.Errorf("ingress delete event contained %T", obj))
				return
			}

			publishIngressEvent(ctx, "delete", ingress)
		},
	}); err != nil {
		return fmt.Errorf("register ingress event handler: %w", err)
	}

	// Start launches each registered informer in its own goroutine. The
	// factory stops all of them when ctx is cancelled.
	factory.Start(ctx.Done())

	// Wait for the initial LIST to hydrate the local cache before declaring the
	// watcher ready. This prevents consumers from reading a half-empty cluster
	// snapshot once later phases expose SSE streams.
	if synced := cache.WaitForCacheSync(
		ctx.Done(),
		podInformer.HasSynced,
		serviceInformer.HasSynced,
		ingressInformer.HasSynced,
	); !synced {
		if ctx.Err() != nil {
			return nil
		}

		return fmt.Errorf("sync informer caches")
	}

	log.Println("cluster informers started")

	<-ctx.Done()
	log.Println("cluster informers stopped")

	return nil
}

func objectFromEvent[T any](obj interface{}) (*T, bool) {
	if typed, ok := obj.(*T); ok {
		return typed, true
	}

	tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
	if !ok {
		return nil, false
	}

	typed, ok := tombstone.Obj.(*T)
	return typed, ok
}

func publishPodEvent(ctx context.Context, eventType string, pod *corev1.Pod) {
	publishEvent(ctx, events.ClusterEvent{
		Type:         eventType,
		Resource:     "pod",
		ResourceType: "Pod",
		Namespace:    pod.Namespace,
		Name:         pod.Name,
		Phase:        string(pod.Status.Phase),
	})
}

func publishServiceEvent(ctx context.Context, eventType string, service *corev1.Service) {
	publishEvent(ctx, events.ClusterEvent{
		Type:         eventType,
		Resource:     "service",
		ResourceType: "Service",
		Namespace:    service.Namespace,
		Name:         service.Name,
		ServiceType:  string(service.Spec.Type),
		ClusterIP:    service.Spec.ClusterIP,
	})
}

func publishIngressEvent(ctx context.Context, eventType string, ingress *networkingv1.Ingress) {
	publishEvent(ctx, events.ClusterEvent{
		Type:         eventType,
		Resource:     "ingress",
		ResourceType: "Ingress",
		Namespace:    ingress.Namespace,
		Name:         ingress.Name,
		Hosts:        ingressHosts(ingress),
		Routes:       ingressRoutes(ingress),
	})
}

func publishEvent(ctx context.Context, event events.ClusterEvent) {
	select {
	case events.EventBus <- event:
	case <-ctx.Done():
	default:
		log.Printf(
			"drop cluster event: bus full type=%s resourceType=%s namespace=%s name=%s",
			event.Type,
			event.ResourceType,
			event.Namespace,
			event.Name,
		)
	}
}

func ingressHosts(ingress *networkingv1.Ingress) []string {
	hosts := make([]string, 0, len(ingress.Spec.Rules))

	for _, rule := range ingress.Spec.Rules {
		if rule.Host == "" {
			hosts = append(hosts, "*")
			continue
		}

		hosts = append(hosts, rule.Host)
	}

	return hosts
}

func ingressRoutes(ingress *networkingv1.Ingress) []events.IngressRoute {
	routes := make([]events.IngressRoute, 0)

	if ingress.Spec.DefaultBackend != nil {
		routes = appendBackendRoute(routes, "*", "", ingress.Spec.DefaultBackend)
	}

	for _, rule := range ingress.Spec.Rules {
		if rule.HTTP == nil {
			continue
		}

		host := rule.Host
		if host == "" {
			host = "*"
		}

		for _, path := range rule.HTTP.Paths {
			routes = appendBackendRoute(routes, host, path.Path, &path.Backend)
		}
	}

	return routes
}

func appendBackendRoute(routes []events.IngressRoute, host string, path string, backend *networkingv1.IngressBackend) []events.IngressRoute {
	if backend == nil || backend.Service == nil {
		return routes
	}

	return append(routes, events.IngressRoute{
		Host:        host,
		Path:        path,
		ServiceName: backend.Service.Name,
		ServicePort: serviceBackendPort(backend.Service.Port),
	})
}

func serviceBackendPort(port networkingv1.ServiceBackendPort) string {
	if port.Name != "" {
		return port.Name
	}

	if port.Number != 0 {
		return fmt.Sprintf("%d", port.Number)
	}

	return "unknown"
}
