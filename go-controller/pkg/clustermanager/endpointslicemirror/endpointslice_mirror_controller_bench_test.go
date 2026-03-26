package endpointslicemirror

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/allocator/id"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/networkmanager"
	ovntest "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/testing"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
)

// BenchmarkSyncDefaultEndpointSlice_Unchanged measures early exit optimization
// when resourceVersion matches (expected: <1ms per operation)
func BenchmarkSyncDefaultEndpointSlice_Unchanged(b *testing.B) {
	err := config.PrepareTestConfig()
	if err != nil {
		b.Fatal(err)
	}
	config.OVNKubernetesFeature.EnableInterconnect = true
	config.OVNKubernetesFeature.EnableMultiNetwork = true
	config.OVNKubernetesFeature.EnableNetworkSegmentation = true

	namespaceT := *util.NewNamespace("bench-ns")
	namespaceT.Labels[types.RequiredUDNNamespaceLabel] = ""

	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "bench-pod",
			Namespace:   namespaceT.Name,
			Annotations: map[string]string{util.OvnPodAnnotationName: `{"default":{"mac_address":"0a:58:0a:f4:02:03","ip_address":"10.244.2.3/24","role":"infrastructure-locked"},"bench-ns/l3-network":{"mac_address":"0a:58:0a:84:02:04","ip_address":"10.132.2.4/24","role":"primary"}}`},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	defaultEndpointSlice := discovery.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "bench-endpointslice",
			Namespace:       namespaceT.Name,
			ResourceVersion: "100",
			Labels: map[string]string{
				discovery.LabelServiceName: "bench-svc",
				discovery.LabelManagedBy:   types.EndpointSliceDefaultControllerName,
			},
		},
		Endpoints: []discovery.Endpoint{
			{
				Addresses: []string{"10.244.2.3"},
				TargetRef: &corev1.ObjectReference{
					Kind:      "Pod",
					Namespace: namespaceT.Name,
					Name:      pod.Name,
				},
			},
		},
	}

	// Create mirrored slice with MATCHING resourceVersion (triggers early exit)
	mirroredEndpointSlice := ovntest.MirrorEndpointSlice(&defaultEndpointSlice, "l3-network", false)
	mirroredEndpointSlice.Annotations[types.LabelSourceEndpointSliceVersion] = "100"

	objs := []runtime.Object{
		&corev1.PodList{
			Items: []corev1.Pod{pod},
		},
		&corev1.NamespaceList{
			Items: []corev1.Namespace{namespaceT},
		},
		&discovery.EndpointSliceList{
			Items: []discovery.EndpointSlice{
				defaultEndpointSlice,
				*mirroredEndpointSlice,
			},
		},
	}

	fakeClient := util.GetOVNClientset(objs...).GetClusterManagerClientset()
	wf, err := factory.NewClusterManagerWatchFactory(fakeClient)
	if err != nil {
		b.Fatal(err)
	}
	networkManager, err := networkmanager.NewForCluster(&networkmanager.FakeControllerManager{}, wf, fakeClient, nil, id.NewTunnelKeyAllocator("TunnelKeys"))
	if err != nil {
		b.Fatal(err)
	}
	controller, err := NewController(fakeClient, wf, networkManager.Interface())
	if err != nil {
		b.Fatal(err)
	}

	err = wf.Start()
	if err != nil {
		b.Fatal(err)
	}
	defer wf.Shutdown()

	err = networkManager.Start()
	if err != nil {
		b.Fatal(err)
	}
	defer networkManager.Stop()

	// Create NAD
	nad := ovntest.GenerateNAD("l3-network", "l3-network", namespaceT.Name, types.Layer3Topology, "10.132.2.0/16/24", types.NetworkRolePrimary)
	_, err = fakeClient.NetworkAttchDefClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(namespaceT.Name).Create(
		context.TODO(),
		nad,
		metav1.CreateOptions{})
	if err != nil {
		b.Fatal(err)
	}

	key := namespaceT.Name + "/" + defaultEndpointSlice.Name

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := controller.syncDefaultEndpointSlice(context.Background(), key)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSyncDefaultEndpointSlice_Changed measures full sync path
// when resourceVersion differs (expected: ~60ms per operation)
func BenchmarkSyncDefaultEndpointSlice_Changed(b *testing.B) {
	err := config.PrepareTestConfig()
	if err != nil {
		b.Fatal(err)
	}
	config.OVNKubernetesFeature.EnableInterconnect = true
	config.OVNKubernetesFeature.EnableMultiNetwork = true
	config.OVNKubernetesFeature.EnableNetworkSegmentation = true

	namespaceT := *util.NewNamespace("bench-ns")
	namespaceT.Labels[types.RequiredUDNNamespaceLabel] = ""

	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "bench-pod",
			Namespace:   namespaceT.Name,
			Annotations: map[string]string{util.OvnPodAnnotationName: `{"default":{"mac_address":"0a:58:0a:f4:02:03","ip_address":"10.244.2.3/24","role":"infrastructure-locked"},"bench-ns/l3-network":{"mac_address":"0a:58:0a:84:02:04","ip_address":"10.132.2.4/24","role":"primary"}}`},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	objs := []runtime.Object{
		&corev1.PodList{
			Items: []corev1.Pod{pod},
		},
		&corev1.NamespaceList{
			Items: []corev1.Namespace{namespaceT},
		},
		&discovery.EndpointSliceList{
			Items: []discovery.EndpointSlice{},
		},
	}

	fakeClient := util.GetOVNClientset(objs...).GetClusterManagerClientset()
	wf, err := factory.NewClusterManagerWatchFactory(fakeClient)
	if err != nil {
		b.Fatal(err)
	}
	networkManager, err := networkmanager.NewForCluster(&networkmanager.FakeControllerManager{}, wf, fakeClient, nil, id.NewTunnelKeyAllocator("TunnelKeys"))
	if err != nil {
		b.Fatal(err)
	}
	controller, err := NewController(fakeClient, wf, networkManager.Interface())
	if err != nil {
		b.Fatal(err)
	}

	err = wf.Start()
	if err != nil {
		b.Fatal(err)
	}
	defer wf.Shutdown()

	err = networkManager.Start()
	if err != nil {
		b.Fatal(err)
	}
	defer networkManager.Stop()

	// Create NAD
	nad := ovntest.GenerateNAD("l3-network", "l3-network", namespaceT.Name, types.Layer3Topology, "10.132.2.0/16/24", types.NetworkRolePrimary)
	_, err = fakeClient.NetworkAttchDefClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(namespaceT.Name).Create(
		context.TODO(),
		nad,
		metav1.CreateOptions{})
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Create new EndpointSlice with different resourceVersion each iteration
		defaultEndpointSlice := &discovery.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name:            "bench-endpointslice",
				Namespace:       namespaceT.Name,
				ResourceVersion: string(rune(100 + i)), // Different each time
				Labels: map[string]string{
					discovery.LabelServiceName: "bench-svc",
					discovery.LabelManagedBy:   types.EndpointSliceDefaultControllerName,
				},
			},
			Endpoints: []discovery.Endpoint{
				{
					Addresses: []string{"10.244.2.3"},
					TargetRef: &corev1.ObjectReference{
						Kind:      "Pod",
						Namespace: namespaceT.Name,
						Name:      pod.Name,
					},
				},
			},
		}

		_, err := fakeClient.KubeClient.DiscoveryV1().EndpointSlices(namespaceT.Name).Create(
			context.Background(),
			defaultEndpointSlice,
			metav1.CreateOptions{})
		if err != nil {
			// If exists, update it
			_, err = fakeClient.KubeClient.DiscoveryV1().EndpointSlices(namespaceT.Name).Update(
				context.Background(),
				defaultEndpointSlice,
				metav1.UpdateOptions{})
			if err != nil {
				b.Fatal(err)
			}
		}

		key := namespaceT.Name + "/" + defaultEndpointSlice.Name
		b.StartTimer()

		err = controller.syncDefaultEndpointSlice(context.Background(), key)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMirrorEndpointSlice measures the mirrorEndpointSlice function
// focusing on DeepCopy elimination optimization
func BenchmarkMirrorEndpointSlice(b *testing.B) {
	err := config.PrepareTestConfig()
	if err != nil {
		b.Fatal(err)
	}
	config.OVNKubernetesFeature.EnableInterconnect = true
	config.OVNKubernetesFeature.EnableMultiNetwork = true
	config.OVNKubernetesFeature.EnableNetworkSegmentation = true

	namespaceT := *util.NewNamespace("bench-ns")
	namespaceT.Labels[types.RequiredUDNNamespaceLabel] = ""

	// Create EndpointSlice with 100 endpoints (realistic scale)
	endpoints := make([]discovery.Endpoint, 100)
	for i := 0; i < 100; i++ {
		endpoints[i] = discovery.Endpoint{
			Addresses: []string{"10.244.2.3"},
			Conditions: discovery.EndpointConditions{
				Ready:   &[]bool{true}[0],
				Serving: &[]bool{true}[0],
			},
			Hostname: &[]string{"test-hostname"}[0],
			NodeName: &[]string{"test-node"}[0],
			Zone:     &[]string{"test-zone"}[0],
			TargetRef: &corev1.ObjectReference{
				Kind:      "Pod",
				Namespace: namespaceT.Name,
				Name:      "bench-pod",
			},
		}
	}

	defaultEndpointSlice := discovery.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "bench-endpointslice",
			Namespace:       namespaceT.Name,
			ResourceVersion: "100",
			Labels: map[string]string{
				discovery.LabelServiceName: "bench-svc",
				discovery.LabelManagedBy:   types.EndpointSliceDefaultControllerName,
			},
		},
		Endpoints: endpoints,
	}

	mirroredEndpointSlice := ovntest.MirrorEndpointSlice(&defaultEndpointSlice, "l3-network", false)

	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "bench-pod",
			Namespace:   namespaceT.Name,
			Annotations: map[string]string{util.OvnPodAnnotationName: `{"default":{"mac_address":"0a:58:0a:f4:02:03","ip_address":"10.244.2.3/24","role":"infrastructure-locked"},"bench-ns/l3-network":{"mac_address":"0a:58:0a:84:02:04","ip_address":"10.132.2.4/24","role":"primary"}}`},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	objs := []runtime.Object{
		&corev1.PodList{
			Items: []corev1.Pod{pod},
		},
		&corev1.NamespaceList{
			Items: []corev1.Namespace{namespaceT},
		},
		&discovery.EndpointSliceList{
			Items: []discovery.EndpointSlice{defaultEndpointSlice},
		},
	}

	fakeClient := util.GetOVNClientset(objs...).GetClusterManagerClientset()
	wf, err := factory.NewClusterManagerWatchFactory(fakeClient)
	if err != nil {
		b.Fatal(err)
	}
	networkManager, err := networkmanager.NewForCluster(&networkmanager.FakeControllerManager{}, wf, fakeClient, nil, id.NewTunnelKeyAllocator("TunnelKeys"))
	if err != nil {
		b.Fatal(err)
	}
	controller, err := NewController(fakeClient, wf, networkManager.Interface())
	if err != nil {
		b.Fatal(err)
	}

	err = wf.Start()
	if err != nil {
		b.Fatal(err)
	}
	defer wf.Shutdown()

	err = networkManager.Start()
	if err != nil {
		b.Fatal(err)
	}
	defer networkManager.Stop()

	// Create NAD
	nad := ovntest.GenerateNAD("l3-network", "l3-network", namespaceT.Name, types.Layer3Topology, "10.132.2.0/16/24", types.NetworkRolePrimary)
	_, err = fakeClient.NetworkAttchDefClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(namespaceT.Name).Create(
		context.TODO(),
		nad,
		metav1.CreateOptions{})
	if err != nil {
		b.Fatal(err)
	}

	network, err := networkManager.Interface().GetActiveNetworkForNamespace(namespaceT.Name)
	if err != nil {
		b.Fatal(err)
	}

	nadKey, err := networkManager.Interface().GetPrimaryNADForNamespace(namespaceT.Name)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := controller.mirrorEndpointSlice(mirroredEndpointSlice, &defaultEndpointSlice, network, nadKey)
		if err != nil {
			b.Fatal(err)
		}
	}
}
