<script lang="ts">
	import { onDestroy, onMount } from 'svelte';
	import { writable } from 'svelte/store';

	type IngressRoute = {
		host: string;
		path?: string;
		serviceName: string;
		servicePort: string;
	};

	type ClusterEvent = {
		type: string;
		resource: string;
		resourceType?: string;
		namespace?: string;
		name: string;
		phase?: string;
		status?: string;
		serviceType?: string;
		clusterIP?: string;
		hosts?: string[];
		routes?: IngressRoute[];
		cpuUsageMilli?: number;
		cpuCapacityMilli?: number;
		memoryUsageBytes?: number;
		memoryCapacityBytes?: number;
	};

	type PodState = {
		namespace: string;
		name: string;
		group: string;
		phase: string;
		updatedAt: number;
	};

	type PodGroup = {
		id: string;
		namespace: string;
		name: string;
		pods: PodState[];
	};

	type ServiceState = {
		namespace: string;
		name: string;
		serviceType: string;
		clusterIP: string;
		updatedAt: number;
	};

	type IngressState = {
		namespace: string;
		name: string;
		hosts: string[];
		routes: IngressRoute[];
		updatedAt: number;
	};

	type RouteRow = {
		id: string;
		namespace: string;
		ingressName: string;
		host: string;
		path: string;
		serviceName: string;
		servicePort: string;
		service?: ServiceState;
	};

	type NodeMetricsState = {
		name: string;
		status: string;
		cpuUsageMilli: number;
		cpuCapacityMilli: number;
		memoryUsageBytes: number;
		memoryCapacityBytes: number;
		updatedAt: number;
	};

	const streamURL = 'http://localhost:8042/api/stream';
	const podStore = writable<Map<string, PodState>>(new Map());
	const serviceStore = writable<Map<string, ServiceState>>(new Map());
	const ingressStore = writable<Map<string, IngressState>>(new Map());
	const nodeMetricsStore = writable<Map<string, NodeMetricsState>>(new Map());
	const replicaSetHash = /^[a-z0-9]{8,10}$/;
	const podSuffix = /^[a-z0-9]{5}$/;
	const ordinal = /^\d+$/;
	const hardwareBarWidth = 20;

	let status = 'CONNECTING';
	let lastEvent = 'NO SIGNAL';
	let inspectedPod = 'HOVER TARGET :: NONE';
	let source: EventSource | undefined;
	let mouseYPx = 0;

	$: podList = Array.from($podStore.values()).sort(comparePods);
	$: groups = groupPods(podList);
	$: userWorkloads = groups.filter((group) => !isSystemNamespace(group.namespace));
	$: systemWorkloads = groups.filter((group) => isSystemNamespace(group.namespace));
	$: runningCount = podList.filter((pod) => normalizePhase(pod.phase) === 'running').length;
	$: warningCount = podList.filter((pod) => normalizePhase(pod.phase) === 'pending').length;

	$: serviceList = Array.from($serviceStore.values()).sort(compareServices);
	$: ingressList = Array.from($ingressStore.values()).sort(compareIngresses);
	$: routeRows = buildRouteRows(ingressList, $serviceStore);
	$: routedServiceKeys = new Set(routeRows.map((route) => serviceKey(route.namespace, route.serviceName)));
	$: internalServices = serviceList.filter((service) => !routedServiceKeys.has(serviceKey(service.namespace, service.name)));
	$: userRoutes = routeRows.filter((route) => !isSystemNamespace(route.namespace));
	$: systemRoutes = routeRows.filter((route) => isSystemNamespace(route.namespace));
	$: userInternalServices = internalServices.filter((service) => !isSystemNamespace(service.namespace));
	$: systemInternalServices = internalServices.filter((service) => isSystemNamespace(service.namespace));
	$: nodeMetricsList = Array.from($nodeMetricsStore.values()).sort((a, b) => a.name.localeCompare(b.name));
	$: readyNodeCount = nodeMetricsList.filter((node) => node.status === 'Ready').length;
	$: notReadyNodeCount = nodeMetricsList.length - readyNodeCount;

	onMount(() => {
		mouseYPx = window.innerHeight / 2;

		source = new EventSource(streamURL);

		source.onopen = () => {
			status = 'LINKED';
		};

		source.onerror = () => {
			status = 'RETRYING';
		};

		source.onmessage = (message) => {
			try {
				applyClusterEvent(JSON.parse(message.data) as ClusterEvent);
			} catch {
				status = 'BAD PAYLOAD';
			}
		};

	});

	onDestroy(() => {
		source?.close();
	});

	function applyClusterEvent(event: ClusterEvent) {
		const type = event.type.toLowerCase();
		const resource = (event.resourceType ?? event.resource).toLowerCase();
		const key = event.namespace ? serviceKey(event.namespace, event.name) : event.name;

		lastEvent = `${type.toUpperCase()} ${resource.toUpperCase()} ${key}`;

		if (resource === 'pod') {
			applyPodEvent(type, key, event);
			return;
		}

		if (resource === 'service') {
			applyServiceEvent(type, key, event);
			return;
		}

		if (resource === 'ingress') {
			applyIngressEvent(type, key, event);
			return;
		}

		if (resource === 'nodemetrics') {
			applyNodeMetricsEvent(type, event);
		}
	}

	function applyPodEvent(type: string, key: string, event: ClusterEvent) {
		if (!event.namespace) {
			return;
		}

		const namespace = event.namespace;

		podStore.update((pods) => {
			const next = new Map(pods);

			if (type === 'delete') {
				next.delete(key);
				return next;
			}

			if (type === 'add' || type === 'update') {
				next.set(key, {
					namespace,
					name: event.name,
					group: deploymentName(event.name),
					phase: event.phase ?? 'Unknown',
					updatedAt: Date.now()
				});
			}

			return next;
		});
	}

	function applyServiceEvent(type: string, key: string, event: ClusterEvent) {
		if (!event.namespace) {
			return;
		}

		const namespace = event.namespace;

		serviceStore.update((services) => {
			const next = new Map(services);

			if (type === 'delete') {
				next.delete(key);
				return next;
			}

			if (type === 'add' || type === 'update') {
				next.set(key, {
					namespace,
					name: event.name,
					serviceType: event.serviceType ?? 'Unknown',
					clusterIP: event.clusterIP ?? 'None',
					updatedAt: Date.now()
				});
			}

			return next;
		});
	}

	function applyIngressEvent(type: string, key: string, event: ClusterEvent) {
		if (!event.namespace) {
			return;
		}

		const namespace = event.namespace;

		ingressStore.update((ingresses) => {
			const next = new Map(ingresses);

			if (type === 'delete') {
				next.delete(key);
				return next;
			}

			if (type === 'add' || type === 'update') {
				next.set(key, {
					namespace,
					name: event.name,
					hosts: event.hosts ?? [],
					routes: event.routes ?? [],
					updatedAt: Date.now()
				});
			}

			return next;
		});
	}

	function applyNodeMetricsEvent(type: string, event: ClusterEvent) {
		nodeMetricsStore.update((nodes) => {
			const next = new Map(nodes);

			if (type === 'delete') {
				next.delete(event.name);
				return next;
			}

			next.set(event.name, {
				name: event.name,
				status: event.status ?? 'NotReady',
				cpuUsageMilli: event.cpuUsageMilli ?? 0,
				cpuCapacityMilli: event.cpuCapacityMilli ?? 0,
				memoryUsageBytes: event.memoryUsageBytes ?? 0,
				memoryCapacityBytes: event.memoryCapacityBytes ?? 0,
				updatedAt: Date.now()
			});

			return next;
		});
	}

	function deploymentName(podName: string) {
		const parts = podName.split('-');
		const last = parts.at(-1) ?? '';
		const previous = parts.at(-2) ?? '';

		if (parts.length >= 3 && replicaSetHash.test(previous) && podSuffix.test(last)) {
			return parts.slice(0, -2).join('-');
		}

		if (parts.length >= 2 && ordinal.test(last)) {
			return parts.slice(0, -1).join('-');
		}

		return podName;
	}

	function groupPods(pods: PodState[]) {
		const grouped = new Map<string, PodGroup>();

		for (const pod of pods) {
			const id = serviceKey(pod.namespace, pod.group);
			const group = grouped.get(id);

			if (group) {
				group.pods.push(pod);
				continue;
			}

			grouped.set(id, {
				id,
				namespace: pod.namespace,
				name: pod.group,
				pods: [pod]
			});
		}

		return Array.from(grouped.values()).sort((a, b) => a.id.localeCompare(b.id));
	}

	function buildRouteRows(ingresses: IngressState[], services: Map<string, ServiceState>) {
		const rows: RouteRow[] = [];

		for (const ingress of ingresses) {
			for (const route of ingress.routes) {
				const path = route.path || '/';
				const id = `${ingress.namespace}/${ingress.name}/${route.host}${path}/${route.serviceName}:${route.servicePort}`;

				rows.push({
					id,
					namespace: ingress.namespace,
					ingressName: ingress.name,
					host: route.host,
					path,
					serviceName: route.serviceName,
					servicePort: route.servicePort,
					service: services.get(serviceKey(ingress.namespace, route.serviceName))
				});
			}
		}

		return rows.sort((a, b) => a.id.localeCompare(b.id));
	}

	function comparePods(a: PodState, b: PodState) {
		return `${a.namespace}/${a.group}/${a.name}`.localeCompare(`${b.namespace}/${b.group}/${b.name}`);
	}

	function compareServices(a: ServiceState, b: ServiceState) {
		return serviceKey(a.namespace, a.name).localeCompare(serviceKey(b.namespace, b.name));
	}

	function compareIngresses(a: IngressState, b: IngressState) {
		return serviceKey(a.namespace, a.name).localeCompare(serviceKey(b.namespace, b.name));
	}

	function normalizePhase(phase: string) {
		return phase.toLowerCase();
	}

	function isCritical(phase: string) {
		const normalized = normalizePhase(phase);
		return normalized === 'failed' || normalized === 'crashloopbackoff' || normalized === 'error';
	}

	function isSystemNamespace(namespace: string) {
		return namespace.toLowerCase().startsWith('kube-');
	}

	function blockClass(phase: string) {
		const normalized = normalizePhase(phase);

		if (normalized === 'running') {
			return 'pod-block pod-running';
		}

		if (normalized === 'pending') {
			return 'pod-block pod-pending';
		}

		if (isCritical(phase)) {
			return 'pod-block pod-critical';
		}

		return 'pod-block pod-unknown';
	}

	function namespaceCode(namespace: string) {
		return namespace.toUpperCase().replaceAll('-', '.');
	}

	function serviceKey(namespace: string, name: string) {
		return `${namespace}/${name}`;
	}

	function hostPath(route: RouteRow) {
		return route.path === '/' ? route.host : `${route.host}${route.path}`;
	}

	function percentage(usage: number, capacity: number) {
		if (capacity <= 0) {
			return 0;
		}

		return Math.min(100, Math.round((usage / capacity) * 100));
	}

	function hardwareBar(percent: number) {
		const filled = Math.round((percent / 100) * hardwareBarWidth);
		return `${'|'.repeat(filled)}${'.'.repeat(hardwareBarWidth - filled)}`;
	}

	function formatBytes(bytes: number) {
		if (bytes >= 1024 ** 3) {
			return `${(bytes / 1024 ** 3).toFixed(1)}Gi`;
		}

		if (bytes >= 1024 ** 2) {
			return `${(bytes / 1024 ** 2).toFixed(0)}Mi`;
		}

		return `${bytes}B`;
	}

	function inspectPod(pod: PodState) {
		inspectedPod = `POD::${pod.name} // NS::${pod.namespace} // STATUS::${pod.phase}`;
	}

	function clearInspection() {
		inspectedPod = 'HOVER TARGET :: NONE';
	}

	function handleMouseMove(event: MouseEvent) {
		mouseYPx = event.clientY;
	}

	$: artStyle = [
		`--mouse-y: ${mouseYPx.toFixed(2)}px`
	].join('; ');
</script>

{#snippet workloadGrid(workloads: PodGroup[])}
	{#if workloads.length === 0}
		<section class="grid min-h-28 place-items-center border-b border-red-900/40 text-zinc-500">
			<div class="border border-red-900/40 bg-white/30 px-4 py-3 text-[10px] uppercase tracking-[0.25em] backdrop-blur-md">
				NO WORKLOADS DETECTED
			</div>
		</section>
	{:else}
		<section class="grid grid-cols-1 border-l border-red-900/40 2xl:grid-cols-2">
			{#each workloads as group (group.id)}
				<article class="artifact-panel min-h-24 border-b border-r border-red-900/40">
					<header class="artifact-header flex items-center justify-between border-b border-red-900/40 px-2 py-1 text-[10px] uppercase tracking-[0.12em]">
						<span class="truncate text-zinc-950">[SYS.{namespaceCode(group.namespace)} // {group.name.toUpperCase()}]</span>
						<span class="pl-2 text-red-900/70">{group.pods.length.toString().padStart(2, '0')}</span>
					</header>

					<div class="flex flex-wrap gap-px p-2">
						{#each group.pods as pod (pod.name)}
							<button
								type="button"
								class={`h-3 w-3 border border-black/80 ${blockClass(pod.phase)} focus:outline-none focus:ring-1 focus:ring-amber-200 hover:border-amber-100`}
								aria-label={`${pod.namespace}/${pod.name} ${pod.phase}`}
								onfocus={() => inspectPod(pod)}
								onblur={clearInspection}
								onmouseenter={() => inspectPod(pod)}
								onmouseleave={clearInspection}
							></button>
						{/each}
					</div>
				</article>
			{/each}
		</section>
	{/if}
{/snippet}

{#snippet networkGrid(routes: RouteRow[], services: ServiceState[])}
	<section class="artifact-panel border-b border-red-900/40">
		<header class="artifact-header border-b border-red-900/40 px-2 py-1 text-[10px] uppercase tracking-[0.18em] text-amber-900">
			INGRESS ROUTES :: {routes.length.toString().padStart(2, '0')}
		</header>

		{#if routes.length === 0}
			<div class="border-b border-red-900/40 px-2 py-3 text-[10px] uppercase tracking-[0.22em] text-zinc-500">
				NO EXTERNAL ROUTES
			</div>
		{:else}
			<div class="divide-y divide-red-950/45">
				{#each routes as route (route.id)}
					<div class="grid grid-cols-[minmax(0,1fr)_auto_minmax(0,1fr)] items-center gap-2 px-2 py-2 text-[10px] uppercase tracking-[0.08em]">
						<div class="truncate text-zinc-950">[HOST::{hostPath(route)}]</div>
						<div class="text-red-800">==&gt;</div>
						<div class="truncate text-zinc-950">
							[SVC::{route.serviceName}:{route.servicePort}]
							<span class={route.service ? 'text-red-900/70' : 'text-red-900'}>
								{route.service ? ` // ${route.service.clusterIP}` : ' // UNRESOLVED'}
							</span>
						</div>
					</div>
				{/each}
			</div>
		{/if}
	</section>

	<section class="artifact-panel border-b border-red-900/40">
		<header class="artifact-header border-b border-red-900/40 px-2 py-1 text-[10px] uppercase tracking-[0.18em] text-red-800/80">
			INTERNAL NETWORKING :: {services.length.toString().padStart(2, '0')}
		</header>

		{#if services.length === 0}
			<div class="px-2 py-3 text-[10px] uppercase tracking-[0.22em] text-zinc-500">
				NO INTERNAL SERVICES
			</div>
		{:else}
			<div class="divide-y divide-red-950/45">
				{#each services as service (serviceKey(service.namespace, service.name))}
					<div class="grid grid-cols-[minmax(0,1fr)_auto] gap-3 px-2 py-2 text-[10px] uppercase tracking-[0.08em] text-zinc-800">
						<div class="truncate">[SVC::{service.name} // TYPE::{service.serviceType}]</div>
						<div class="text-red-900">CLUSTERIP::{service.clusterIP}</div>
					</div>
				{/each}
			</div>
		{/if}
	</section>
{/snippet}

{#snippet systemDivider(label: string)}
	<section class="border-y border-red-900/40 bg-white/30 px-3 py-2 text-center text-[10px] uppercase tracking-[0.4em] text-red-900 backdrop-blur-md">
		<span class="text-red-900">////</span>
		<span class="artifact-glow px-3 text-amber-800">[ // {label} ]</span>
		<span class="text-red-900">////</span>
	</section>
{/snippet}

{#snippet infrastructureGrid(nodes: NodeMetricsState[])}
	{#if nodes.length === 0}
		<section class="grid min-h-[40vh] place-items-center border-b border-red-900/40 text-zinc-500">
			<div class="border border-red-900/40 bg-white/40 px-4 py-3 text-[10px] uppercase tracking-[0.2em] text-zinc-800 backdrop-blur-md">
				AWAITING NODE METRICS // SOURCE::METRICS-SERVER // RBAC PERMISSIONS CHECK REQUIRED
			</div>
		</section>
	{:else}
		<section class="divide-y divide-amber-900/35 border-b border-red-900/40">
			{#each nodes as node (node.name)}
				{@const cpuPercent = percentage(node.cpuUsageMilli, node.cpuCapacityMilli)}
				{@const memoryPercent = percentage(node.memoryUsageBytes, node.memoryCapacityBytes)}
				<article class="artifact-panel">
					<header class="artifact-header flex items-center justify-between border-b border-red-900/40 px-2 py-1 text-[10px] uppercase tracking-[0.12em]">
						<span class={node.status === 'Ready' ? 'truncate text-zinc-950' : 'truncate text-red-900'}>
							[NODE::{node.name} // STATUS::{node.status}]
						</span>
						<span class="pl-2 text-red-900">METRICS::10S</span>
					</header>

					<div class="space-y-1 px-2 py-2 text-[10px] uppercase tracking-[0.08em]">
						<div class="grid grid-cols-[auto_minmax(0,1fr)_auto] gap-2 text-zinc-950">
							<span>CPU</span>
							<span>[{hardwareBar(cpuPercent)}]</span>
							<span>{cpuPercent.toString().padStart(3, '0')}% // {node.cpuUsageMilli}m/{node.cpuCapacityMilli}m</span>
						</div>
						<div class="grid grid-cols-[auto_minmax(0,1fr)_auto] gap-2 text-red-900">
							<span>RAM</span>
							<span>[{hardwareBar(memoryPercent)}]</span>
							<span>{memoryPercent.toString().padStart(3, '0')}% // {formatBytes(node.memoryUsageBytes)}/{formatBytes(node.memoryCapacityBytes)}</span>
						</div>
					</div>
				</article>
			{/each}
		</section>
	{/if}
{/snippet}

<svelte:head>
	<title>Ghidorah Tactical Matrix</title>
</svelte:head>

<svelte:window onmousemove={handleMouseMove} />

<main
	class="relative isolate h-screen w-screen overflow-hidden bg-[#f4f4f0]"
	style={artStyle}
>
	<svg class="hidden" aria-hidden="true" focusable="false">
		<filter id="heat-warp">
			<feTurbulence type="fractalNoise" baseFrequency="0.05" numOctaves="1" result="noise">
				<animate attributeName="baseFrequency" values="0.045;0.055;0.05" dur="1.2s" repeatCount="indefinite" />
			</feTurbulence>
			<feDisplacementMap in="SourceGraphic" in2="noise" scale="10" xChannelSelector="R" yChannelSelector="G" />
		</filter>
	</svg>

	<div class="ghidorah-art pointer-events-none absolute inset-0 -z-20 opacity-30 mix-blend-multiply" aria-hidden="true">
		<div class="mural mural-base absolute inset-0 bg-cover bg-center opacity-100"></div>
		<div
			class="mural mural-warp absolute inset-0 bg-cover bg-center opacity-100"
			style="clip-path: inset(calc(var(--mouse-y) - 60px) 0 calc(100vh - var(--mouse-y) - 60px) 0);"
		></div>
	</div>

	<section class="operational-plane absolute inset-0 z-10 grid grid-cols-1 overflow-y-auto font-mono text-[11px] leading-none text-zinc-900 xl:grid-cols-3">
			<section class="relative z-10 border-r border-red-900/40 bg-white/10 backdrop-blur-[2px]">
			<header class="artifact-header artifact-glow border-b border-red-900/40 px-3 py-2 text-[10px] uppercase tracking-[0.22em] text-amber-800">
				LEFT_HEAD // WORKLOAD MATRIX
			</header>

			<section class="artifact-panel border-b border-red-900/40 px-3 py-2 text-[10px] uppercase tracking-[0.16em] text-zinc-800">
				SSE::{status} // PODS::{podList.length} // RUN::{runningCount} // PEND::{warningCount}
			</section>

			<section class="artifact-panel border-b border-red-900/40 px-3 py-2 text-[10px] uppercase tracking-[0.18em] text-amber-900">
				{inspectedPod}
			</section>

			{#if groups.length === 0}
				<section class="grid min-h-[40vh] place-items-center border-b border-red-900/40 text-zinc-500">
					<div class="border border-red-900/40 bg-white/30 px-4 py-3 text-[10px] uppercase tracking-[0.25em] backdrop-blur-md">
						AWAITING POD TELEMETRY
					</div>
				</section>
			{:else}
				{@render workloadGrid(userWorkloads)}
				{@render systemDivider('SYSTEM CORE')}
				{@render workloadGrid(systemWorkloads)}
			{/if}
		</section>

			<section class="relative z-10 border-r border-red-900/40 bg-white/10 backdrop-blur-[2px]">
			<header class="artifact-header artifact-glow border-b border-red-900/40 px-3 py-2 text-[10px] uppercase tracking-[0.22em] text-amber-800">
				CENTER_HEAD // NETWORK ROUTING
			</header>

			<section class="artifact-panel border-b border-red-900/40 px-3 py-2 text-[10px] uppercase tracking-[0.16em] text-zinc-800">
				ROUTES::{routeRows.length} // USER::{userRoutes.length} // SYS::{systemRoutes.length} // SVC::{serviceList.length}
			</section>

			{@render networkGrid(userRoutes, userInternalServices)}
			{@render systemDivider('SYSTEM CORE')}
			{@render networkGrid(systemRoutes, systemInternalServices)}
		</section>

			<section class="relative z-10 bg-white/10 backdrop-blur-[2px]">
			<header class="artifact-header artifact-glow border-b border-red-900/40 px-3 py-2 text-[10px] uppercase tracking-[0.22em] text-amber-800">
				RIGHT_HEAD // INFRASTRUCTURE
			</header>

			<section class="artifact-panel border-b border-red-900/40 px-3 py-2 text-[10px] uppercase tracking-[0.16em] text-zinc-800">
				NODES::{nodeMetricsList.length} // READY::{readyNodeCount} // NOT_READY::{notReadyNodeCount}
			</section>

			<section class="artifact-panel border-b border-red-900/40 px-3 py-2 text-[10px] uppercase tracking-[0.16em] text-zinc-800">
				SRC::{streamURL} // LAST::{lastEvent} // SOURCE::METRICS-SERVER
			</section>

			{@render infrastructureGrid(nodeMetricsList)}
		</section>
	</section>
</main>

<style>
	.mural {
		background-image: url('/assets/mural.jpg');
		background-attachment: fixed;
		transform: scale(1.04);
	}

	.ghidorah-art::after {
		position: absolute;
		inset: 0;
		content: '';
		pointer-events: none;
		opacity: 0.18;
		mix-blend-mode: multiply;
		background-image:
			radial-gradient(circle at 18% 22%, rgba(127, 29, 29, 0.16) 0 1px, transparent 1px),
			radial-gradient(circle at 74% 68%, rgba(24, 24, 27, 0.12) 0 1px, transparent 1px),
			radial-gradient(circle at 46% 42%, rgba(180, 83, 9, 0.12) 0 1px, transparent 1px);
		background-size:
			17px 19px,
			23px 29px,
			11px 13px;
	}

	.mural-warp {
		filter: url('#heat-warp');
		will-change: clip-path, filter;
	}

	.artifact-panel {
		background: linear-gradient(135deg, rgba(255, 255, 255, 0.5), rgba(248, 250, 252, 0.32));
		backdrop-filter: blur(10px) saturate(1.05);
		box-shadow:
			inset 0 0 0 1px rgba(228, 228, 231, 0.66),
			inset 0 0 0 2px rgba(127, 29, 29, 0.1),
			inset 0 0 22px rgba(255, 255, 255, 0.22);
	}

	.artifact-header {
		background: linear-gradient(90deg, rgba(255, 255, 255, 0.62), rgba(254, 243, 199, 0.38), rgba(255, 255, 255, 0.62));
		backdrop-filter: blur(10px) saturate(1.1);
	}

	.artifact-glow {
		text-shadow:
			0 0 8px rgba(245, 158, 11, 0.28),
			0 0 14px rgba(127, 29, 29, 0.22);
	}

	.pod-block {
		position: relative;
		box-shadow: inset 0 0 4px rgba(255, 244, 214, 0.2);
	}

	.pod-running {
		background: #d9a441;
		animation: pod-pulse 1.8s ease-in-out infinite;
		box-shadow:
			0 0 8px rgba(245, 158, 11, 0.72),
			0 0 16px rgba(245, 158, 11, 0.36),
			inset 0 0 6px rgba(255, 251, 235, 0.42),
			inset 0 -1px 2px rgba(0, 0, 0, 0.55);
	}

	.pod-pending {
		background: #8a4f12;
		box-shadow: 0 0 6px rgba(251, 191, 36, 0.45);
	}

	.pod-critical {
		background: #7f1d1d;
		clip-path: polygon(0 0, 68% 0, 100% 30%, 72% 49%, 100% 100%, 36% 82%, 0 100%, 16% 48%);
		box-shadow:
			0 0 9px rgba(239, 68, 68, 0.78),
			inset 0 0 4px rgba(254, 202, 202, 0.3);
	}

	.pod-unknown {
		background: #52525b;
	}

	@keyframes pod-pulse {
		0%,
		100% {
			opacity: 0.78;
		}
		50% {
			opacity: 1;
		}
	}

</style>
