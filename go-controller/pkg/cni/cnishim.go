package cni

// contains code for cnishim - one that gets called as the cni Plugin
// This does not do the real cni work. This is just the client to the cniserver
// that does the real work.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/containernetworking/cni/pkg/skel"
	"github.com/containernetworking/cni/pkg/types"
	current "github.com/containernetworking/cni/pkg/types/100"
	nadapi "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	kexec "k8s.io/utils/exec"

	ovntypes "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/cni/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
)

// Plugin is the structure to hold the endpoint information and the corresponding
// functions to use it
type Plugin struct {
	socketPath string
	doCNIFunc  func(url string, req interface{}) ([]byte, error)
}

// NewCNIPlugin creates the internal Plugin object
func NewCNIPlugin(socketPath string) *Plugin {
	if len(socketPath) == 0 {
		socketPath = serverSocketPath
	}
	p := &Plugin{socketPath: socketPath}
	p.doCNIFunc = p.doCNI
	return p
}

// Create and fill a Request with this Plugin's environment and stdin which
// contain the CNI variables and configuration
func newCNIRequest(args *skel.CmdArgs, deviceInfo nadapi.DeviceInfo) *Request {
	envMap := make(map[string]string)
	for _, item := range os.Environ() {
		idx := strings.Index(item, "=")
		if idx > 0 {
			envMap[strings.TrimSpace(item[:idx])] = item[idx+1:]
		}
	}
	return &Request{
		Env:        envMap,
		Config:     args.StdinData,
		DeviceInfo: deviceInfo,
	}

}

// Send a CNI request to the CNI server via JSON + HTTP over a root-owned unix socket,
// and return the result
func (p *Plugin) doCNI(url string, req interface{}) ([]byte, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal CNI request %v: %v", req, err)
	}

	client := &http.Client{
		Transport: &http.Transport{
			Dial: func(_, _ string) (net.Conn, error) {
				return net.Dial("unix", p.socketPath)
			},
		},
	}

	resp, err := client.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to send CNI request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read CNI result: %v", err)
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("CNI request failed with status %v: '%s'", resp.StatusCode, string(body))
	}

	return body, nil
}

func setupLogging(conf *ovntypes.NetConf) {
	var err error
	var level klog.Level

	if conf.LogLevel != "" {
		if err = level.Set(conf.LogLevel); err != nil {
			klog.Warningf("Failed to set klog log level to %s: %v", conf.LogLevel, err)
		}
	}
	if conf.LogFile != "" {
		klogFlags := flag.NewFlagSet("klog", flag.ExitOnError)
		klog.InitFlags(klogFlags)
		if err := klogFlags.Set("logtostderr", "false"); err != nil {
			klog.Warningf("Error setting klog logtostderr: %v", err)
		}
		if err := klogFlags.Set("alsologtostderr", "true"); err != nil {
			klog.Warningf("Error setting klog alsologtostderr: %v", err)
		}
		klog.SetOutput(&lumberjack.Logger{
			Filename:   conf.LogFile,
			MaxSize:    conf.LogFileMaxSize, // megabytes
			MaxBackups: conf.LogFileMaxBackups,
			MaxAge:     conf.LogFileMaxAge, // days
			Compress:   true,
		})
	}
}

// report the CNI request processing time to CNI server. This is used for the cni_request_duration_seconds metrics
func (p *Plugin) postMetrics(startTime time.Time, cmd command, err error) {
	elapsedTime := time.Since(startTime).Seconds()
	_, _ = p.doCNI("http://dummy/metrics", &CNIRequestMetrics{
		Command:     cmd,
		ElapsedTime: elapsedTime,
		HasErr:      err != nil,
	})
}

func shimClientsetFromConfig(auth *KubeAPIAuth) (*shimClientset, error) {
	if auth.Kubeconfig == "" && auth.KubeAPIServer == "" {
		return nil, nil
	}

	var caData []byte
	var err error
	if auth.KubeCAData != "" {
		caData, err = base64.StdEncoding.DecodeString(auth.KubeCAData)
		if err != nil {
			return nil, fmt.Errorf("failed to decode Kube API CA data: %v", err)
		}
	}
	kubeconfig := &config.KubernetesConfig{
		Kubeconfig: auth.Kubeconfig,
		APIServer:  auth.KubeAPIServer,
		Token:      auth.KubeAPIToken,
		TokenFile:  auth.KubeAPITokenFile,
		CAData:     caData,
	}

	kclient, err := util.NewKubernetesClientset(kubeconfig)
	if err != nil {
		return nil, err
	}

	return &shimClientset{
		kclient: kclient,
	}, nil
}

type shimClientset struct {
	PodInfoGetter
	kclient kubernetes.Interface
}

func (c *shimClientset) getPod(namespace, name string) (*corev1.Pod, error) {
	return c.kclient.CoreV1().Pods(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// CmdAdd is the callback for 'add' cni calls from skel
func (p *Plugin) CmdAdd(args *skel.CmdArgs) error {
	// TODO(debug): Remove detailed logging after debugging is complete
	processStartTime := time.Now()
	pid := os.Getpid()

	// Log process start with timestamp
	klog.Infof("[CNI-DEBUG] CmdAdd process started: PID=%d, ContainerID=%s, Time=%s",
		pid, args.ContainerID, processStartTime.Format(time.RFC3339Nano))

	// Acquire system-wide lock to prevent excessive concurrent CNI operations
	// This prevents resource exhaustion (threads, goroutines, file descriptors)
	lockAcquireStart := time.Now()
	klog.Infof("[CNI-DEBUG] Attempting to acquire lock: PID=%d, Time=%s",
		pid, lockAcquireStart.Format(time.RFC3339Nano))

	lock, err := AcquireCNILock()
	if err != nil {
		return fmt.Errorf("failed to acquire CNI lock: %v", err)
	}

	lockAcquireEnd := time.Now()
	lockWaitDuration := lockAcquireEnd.Sub(lockAcquireStart)
	klog.Infof("[CNI-DEBUG] Lock acquired: PID=%d, WaitTime=%v, Time=%s",
		pid, lockWaitDuration, lockAcquireEnd.Format(time.RFC3339Nano))

	defer func() {
		lockReleaseStart := time.Now()
		if releaseErr := lock.Release(); releaseErr != nil {
			klog.Warningf("failed to release CNI lock: %v", releaseErr)
		}
		lockReleaseEnd := time.Now()
		klog.Infof("[CNI-DEBUG] Lock released: PID=%d, Time=%s",
			pid, lockReleaseEnd.Format(time.RFC3339Nano))

		// Log total process execution time
		processEndTime := time.Now()
		totalDuration := processEndTime.Sub(processStartTime)
		klog.Infof("[CNI-DEBUG] CmdAdd process completed: PID=%d, ContainerID=%s, TotalTime=%v, StartTime=%s, EndTime=%s",
			pid, args.ContainerID, totalDuration,
			processStartTime.Format(time.RFC3339Nano),
			processEndTime.Format(time.RFC3339Nano))
	}()

	var cmdErr error

	startTime := time.Now()
	defer func() {
		p.postMetrics(startTime, CNIAdd, cmdErr)
	}()

	// read the config stdin args to obtain cniVersion
	conf, errC := config.ReadCNIConfig(args.StdinData)
	if errC != nil {
		cmdErr = fmt.Errorf("invalid stdin args %v", errC)
		return cmdErr
	}
	setupLogging(conf)

	var deviceInfo = nadapi.DeviceInfo{}
	if len(conf.RuntimeConfig.CNIDeviceInfoFile) != 0 {
		bytes, err := os.ReadFile(conf.RuntimeConfig.CNIDeviceInfoFile)
		if err != nil {
			cmdErr = err
			return cmdErr
		}
		if err := json.Unmarshal(bytes, &deviceInfo); err != nil {
			cmdErr = err
			return cmdErr
		}
	}

	req := newCNIRequest(args, deviceInfo)

	// TODO(debug): Log CNI server communication timing
	cniServerStart := time.Now()
	klog.V(4).Infof("[CNI-DEBUG] Calling CNI server: PID=%d", pid)

	body, errB := p.doCNIFunc("http://dummy/", req)

	cniServerEnd := time.Now()
	cniServerDuration := cniServerEnd.Sub(cniServerStart)
	klog.V(4).Infof("[CNI-DEBUG] CNI server response received: PID=%d, Duration=%v", pid, cniServerDuration)
	if errB != nil {
		cmdErr = errB
		klog.Error(cmdErr.Error())
		return cmdErr
	}

	response := &Response{}
	if cmdErr = json.Unmarshal(body, response); cmdErr != nil {
		cmdErr = fmt.Errorf("failed to unmarshal response '%s': %v", string(body), cmdErr)
		klog.Error(cmdErr.Error())
		return cmdErr
	}

	clientset, errK := shimClientsetFromConfig(response.KubeAuth)
	if errK != nil {
		cmdErr = errK
		return cmdErr
	}

	var result *current.Result
	if response.Result != nil {
		// Return the full CNI result from ovnkube-node if it configured the pod interface
		result = response.Result
	} else {
		// The onvkube-node is running in un-privileged mode. The responsibility of
		// plugging an interface into Pod is on the Shim.

		// Use the IPAM details from ovnkube-node to configure the pod interface
		pr, err := cniRequestToPodRequest(req)
		if err != nil {
			cmdErr = fmt.Errorf("failed to create pod request: %v", err)
			klog.Error(cmdErr.Error())
			return cmdErr
		}
		defer pr.cancel()

		if !response.PodIFInfo.IsDPUHostMode {
			// Initialize OVS exec runner; find OVS binaries that the CNI code uses.
			if err := SetExec(kexec.New()); err != nil {
				cmdErr = fmt.Errorf("failed to initialize OVS exec runner: %v", err)
				klog.Error(cmdErr.Error())
				return cmdErr
			}
		}

		// In the case where ovnkube-node is running in Unprivileged mode, all the work
		result, cmdErr = getCNIResult(pr, clientset, response.PodIFInfo)
		if cmdErr != nil {
			cmdErr = fmt.Errorf("failed to get CNI Result from pod interface info %v: %v", response.PodIFInfo, cmdErr)
			klog.Error(cmdErr.Error())
			return cmdErr
		}
		if response.PrimaryUDNPodInfo != nil {
			primaryUDNPodRequest := response.PrimaryUDNPodReq
			primaryUDNPodRequest.ctx, primaryUDNPodRequest.cancel = context.WithCancel(pr.ctx)
			defer primaryUDNPodRequest.cancel()
			cmdErr = primaryUDNCmdAddGetCNIResultFunc(result, getCNIResult, primaryUDNPodRequest, clientset, response.PrimaryUDNPodInfo)
			if cmdErr != nil {
				klog.Error(cmdErr.Error())
				return cmdErr
			}
		}
	}

	return types.PrintResult(result, conf.CNIVersion)
}

// CmdDel is the callback for 'teardown' cni calls from skel
func (p *Plugin) CmdDel(args *skel.CmdArgs) error {
	// TODO(debug): Remove detailed logging after debugging is complete
	processStartTime := time.Now()
	pid := os.Getpid()

	// Log process start with timestamp
	klog.Infof("[CNI-DEBUG] CmdDel process started: PID=%d, ContainerID=%s, Time=%s",
		pid, args.ContainerID, processStartTime.Format(time.RFC3339Nano))

	// Acquire system-wide lock to prevent excessive concurrent CNI operations
	lockAcquireStart := time.Now()
	klog.Infof("[CNI-DEBUG] Attempting to acquire lock: PID=%d, Time=%s",
		pid, lockAcquireStart.Format(time.RFC3339Nano))

	lock, err := AcquireCNILock()
	if err != nil {
		return fmt.Errorf("failed to acquire CNI lock: %v", err)
	}

	lockAcquireEnd := time.Now()
	lockWaitDuration := lockAcquireEnd.Sub(lockAcquireStart)
	klog.Infof("[CNI-DEBUG] Lock acquired: PID=%d, WaitTime=%v, Time=%s",
		pid, lockWaitDuration, lockAcquireEnd.Format(time.RFC3339Nano))

	defer func() {
		lockReleaseStart := time.Now()
		if releaseErr := lock.Release(); releaseErr != nil {
			klog.Warningf("failed to release CNI lock: %v", releaseErr)
		}
		lockReleaseEnd := time.Now()
		klog.Infof("[CNI-DEBUG] Lock released: PID=%d, Time=%s",
			pid, lockReleaseEnd.Format(time.RFC3339Nano))

		// Log total process execution time
		processEndTime := time.Now()
		totalDuration := processEndTime.Sub(processStartTime)
		klog.Infof("[CNI-DEBUG] CmdDel process completed: PID=%d, ContainerID=%s, TotalTime=%v, StartTime=%s, EndTime=%s",
			pid, args.ContainerID, totalDuration,
			processStartTime.Format(time.RFC3339Nano),
			processEndTime.Format(time.RFC3339Nano))
	}()

	var cmdErr error
	var body []byte
	var pr *PodRequest
	var conf *ovntypes.NetConf

	startTime := time.Now()
	defer func() {
		p.postMetrics(startTime, CNIDel, cmdErr)
		if cmdErr != nil {
			klog.Errorf("Error on CmdDel: %v", cmdErr)
		}
	}()

	// read the config stdin args
	conf, cmdErr = config.ReadCNIConfig(args.StdinData)
	if cmdErr != nil {
		return cmdErr
	}
	setupLogging(conf)

	var deviceInfo = nadapi.DeviceInfo{}
	req := newCNIRequest(args, deviceInfo)
	body, cmdErr = p.doCNIFunc("http://dummy/", req)
	if cmdErr != nil {
		return cmdErr
	}

	response := &Response{}
	cmdErr = json.Unmarshal(body, response)
	if cmdErr != nil {
		cmdErr = fmt.Errorf("failed to unmarshal response '%s': %v", string(body), cmdErr)
		return cmdErr
	}

	// if Result is nil, then ovnkube-node is running in unprivileged mode so unconfigure the Interface from here.
	if response.Result == nil {
		pr, cmdErr = cniRequestToPodRequest(req)
		if cmdErr != nil {
			cmdErr = fmt.Errorf("failed to create pod request: %v", cmdErr)
			return cmdErr
		}
		defer pr.cancel()

		if !response.PodIFInfo.IsDPUHostMode {
			// Initialize OVS exec runner; find OVS binaries that the CNI code uses.
			if err := SetExec(kexec.New()); err != nil {
				cmdErr = fmt.Errorf("failed to initialize OVS exec runner: %v", err)
				klog.Error(cmdErr.Error())
				return cmdErr
			}
		}

		cmdErr = podRequestInterfaceOps.UnconfigureInterface(pr, response.PodIFInfo)
	}
	return cmdErr
}

// CmdCheck is the callback for 'checking' container's networking is as expected.
func (p *Plugin) CmdCheck(_ *skel.CmdArgs) error {
	// noop...CMD check is not considered useful, and has a considerable performance impact
	// to pod bring up times with CRIO. This is due to the fact that CRIO currently calls check
	// after CNI ADD before it finishes bringing the container up
	return nil
}
