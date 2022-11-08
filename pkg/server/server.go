/*
 * Tencent is pleased to support the open source community by making TKEStack available.
 *
 * Copyright (C) 2012-2019 Tencent. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package server

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	displayapi "tkestack.io/gpu-manager/pkg/api/runtime/display"
	"tkestack.io/gpu-manager/pkg/config"
	deviceFactory "tkestack.io/gpu-manager/pkg/device"
	containerRuntime "tkestack.io/gpu-manager/pkg/runtime"
	allocFactory "tkestack.io/gpu-manager/pkg/services/allocator"
	"tkestack.io/gpu-manager/pkg/services/response"

	// Register allocator controller
	_ "tkestack.io/gpu-manager/pkg/services/allocator/register"
	"tkestack.io/gpu-manager/pkg/services/display"
	vitrual_manager "tkestack.io/gpu-manager/pkg/services/virtual-manager"
	"tkestack.io/gpu-manager/pkg/services/volume"
	"tkestack.io/gpu-manager/pkg/services/watchdog"
	"tkestack.io/gpu-manager/pkg/types"
	"tkestack.io/gpu-manager/pkg/utils"

	systemd "github.com/coreos/go-systemd/daemon"
	google_protobuf1 "github.com/golang/protobuf/ptypes/empty"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

type managerImpl struct {
	config *config.Config

	allocator      allocFactory.GPUTopoService
	displayer      *display.Display
	virtualManager *vitrual_manager.VirtualManager

	bundleServer map[string]ResourceServer
	srv          *grpc.Server
}

// NewManager creates and returns a new managerImpl struct
func NewManager(cfg *config.Config) Manager {
	manager := &managerImpl{
		config:       cfg,
		bundleServer: make(map[string]ResourceServer),
		srv:          grpc.NewServer(),
	}

	return manager
}

// Ready tells the manager whether all bundle servers are truely running
func (m *managerImpl) Ready() bool {
	readyServers := 0
	for _, ins := range m.bundleServer {

		if err := utils.WaitForServer(ins.SocketName()); err == nil {
			readyServers++
			klog.V(2).Infof("Server %s is ready, readyServers: %d", ins.SocketName(), readyServers)
			continue
		}

		return false
	}

	return readyServers > 0 && readyServers == len(m.bundleServer)
}

// manager实体运行的入口
// 1. 解析输入参数
// extra config
// volume config file
// sdNotify
// 2. kubeconfig
// 3. containerRuntimeManager
// 4.Pod Cache
// 5. Node Label
// 6. container response data?
// 7. virtualManager
// 8.tree topo
// 9. grpc service/grpcGW
// 10. display/pporf
// #lizard forgives
func (m *managerImpl) Run() error {
	//run之前执行的初始化...

	//什么extra config?
	if err := m.validExtraConfig(m.config.ExtraConfigPath); err != nil {
		klog.Errorf("Can not load extra config, err %s", err)

		return err
	}

	//The driver name for manager 默认是iluvatar
	if m.config.Driver == "" {
		return fmt.Errorf("you should define a driver")
	}

	//The volume config file location  这个是干啥的？
	if len(m.config.VolumeConfigPath) > 0 {
		volumeManager, err := volume.NewVolumeManager(m.config.VolumeConfigPath, m.config.EnableShare)
		if err != nil {
			klog.Errorf("Can not create volume managerImpl, err %s", err)
			return err
		}

		if err := volumeManager.Run(); err != nil {
			klog.Errorf("Can not start volume managerImpl, err %s", err)
			return err
		}
	}

	// SdNotify应该是用于监听unix sock的重新注册，下面的注释没太明白 init daemon是什么意思

	// SdNotify sends a message to the init daemon. It is common to ignore the error.
	// If `unsetEnvironment` is true, the environment variable `NOTIFY_SOCKET`
	// will be unconditionally unset.
	//
	// It returns one of the following:
	// (false, nil) - notification not supported (i.e. NOTIFY_SOCKET is unset)
	// (false, err) - notification supported, but failure happened (e.g. error connecting to NOTIFY_SOCKET or while sending data)
	// (true, nil) - notification supported, data has been sent
	sent, err := systemd.SdNotify(true, "READY=1\n")
	if err != nil {
		klog.Errorf("Unable to send systemd daemon successful start message: %v\n", err)
	}
	//失败打log
	if !sent {
		klog.Errorf("Unable to set Type=notify in systemd service file?")
	}

	var (
		client    *kubernetes.Clientset
		clientCfg *rest.Config
	)

	///////////
	//与k8s连通的处理
	//这里是创建kubeconfig相关的
	clientCfg, err = clientcmd.BuildConfigFromFlags("", m.config.KubeConfig)
	if err != nil {
		return fmt.Errorf("invalid client config: err(%v)", err)
	}

	client, err = kubernetes.NewForConfig(clientCfg)
	if err != nil {
		return fmt.Errorf("can not generate client from config: error(%v)", err)
	}

	//容器运行时管理，这个时有啥用？
	containerRuntimeManager, err := containerRuntime.NewContainerRuntimeManager(
		m.config.CgroupDriver, m.config.ContainerRuntimeEndpoint, m.config.RequestTimeout)
	if err != nil {
		klog.Errorf("can't create container runtime manager: %v", err)
		return err
	}
	klog.V(2).Infof("Container runtime manager is running")

	//watchdog是干啥的？
	//用于listPod，
	watchdog.NewPodCache(client, m.config.Hostname)
	klog.V(2).Infof("Watchdog is running")

	//初始化label，
	labeler := watchdog.NewNodeLabeler(client.CoreV1(), m.config.Hostname, m.config.NodeLabels)
	if err := labeler.Run(); err != nil {
		return err
	}

	//container resp管理是负责干啥？
	klog.V(2).Infof("Load container response data")
	responseManager := response.NewResponseManager()
	if err := responseManager.LoadFromFile(m.config.DevicePluginPath); err != nil {
		klog.Errorf("can't load container response data, %+#v", err)
		return err
	}

	//虚拟化管理，这个是干啥？
	m.virtualManager = vitrual_manager.NewVirtualManager(m.config, containerRuntimeManager, responseManager)
	m.virtualManager.Run()

	//这块就是设备管理相关的？
	//tree topo
	treeInitFn := deviceFactory.NewFuncForName(m.config.Driver)
	tree := treeInitFn(m.config)

	tree.Init("")
	tree.Update()

	initAllocator := allocFactory.NewFuncForName(m.config.Driver)
	if initAllocator == nil {
		return fmt.Errorf("can not find allocator for %s", m.config.Driver)
	}

	m.allocator = initAllocator(m.config, tree, client, responseManager)
	m.displayer = display.NewDisplay(m.config, tree, containerRuntimeManager)

	//创建GRPC service，将vcore和vmem初始化好，每一个device plugin 资源vcore，vmem都是一个grpc service
	klog.V(2).Infof("Starting the GRPC server, driver %s, queryPort %d", m.config.Driver, m.config.QueryPort)
	m.setupGRPCService()
	//创建一个GRPC网关，包含pprof和display，display是干啥的？
	mux, err := m.setupGRPCGatewayService()
	if err != nil {
		return err
	}
	//创建一个指标监控，
	m.setupMetricsService(mux)

	//这是处理display相关的http listen，display很可能是请求相关统计信息
	go func() {
		//QueryAddr: address for query statistics information"
		displayListenHandler := net.JoinHostPort(m.config.QueryAddr, strconv.Itoa(m.config.QueryPort))
		if err := http.ListenAndServe(displayListenHandler, mux); err != nil {
			klog.Fatalf("failed to serve connections: %v", err)
		}
	}()

	return m.runServer()
}

// 初始化grpc service，包括创建了两个device plugin的资源
func (m *managerImpl) setupGRPCService() {
	//创建两个grpc service，即HTTP/TCP server，这个server绑定了 vcore unix sock，这个用于向kubelet注册资源？
	vcoreServer := newVcoreServer(m)
	vmemoryServer := newVmemoryServer(m)

	//将device plugin的2个资源作为server，管理起来
	m.bundleServer[types.VCoreAnnotation] = vcoreServer
	m.bundleServer[types.VMemoryAnnotation] = vmemoryServer

	//这个是啥？display什么？
	displayapi.RegisterGPUDisplayServer(m.srv, m)
}

// 初始化GRPC gateway，作为一个http service
func (m *managerImpl) setupGRPCGatewayService() (*http.ServeMux, error) {
	//创建http Mutilpexer多路复用 的服务
	mux := http.NewServeMux()
	//返回一个空的ServeMux..?why
	displayMux := runtime.NewServeMux()

	//做URL映射
	mux.Handle("/", displayMux)
	mux.HandleFunc("/debug/pprof/", pprof.Index)

	go func() {
		//创建一个协程，用于展现？？
		if err := displayapi.RegisterGPUDisplayHandlerFromEndpoint(context.Background(), displayMux, types.ManagerSocket, utils.DefaultDialOptions); err != nil {
			klog.Fatalf("Register display service failed, error %s", err)
		}
	}()

	return mux, nil
}

// 查看指标，启动一个http service，这个具体指标处理的部分，在哪?
func (m *managerImpl) setupMetricsService(mux *http.ServeMux) {
	r := prometheus.NewRegistry()

	r.MustRegister(m.displayer)

	mux.Handle("/metric", promhttp.HandlerFor(r, promhttp.HandlerOpts{ErrorHandling: promhttp.ContinueOnError}))
}

func (m *managerImpl) runServer() error {
	for name, srv := range m.bundleServer {
		klog.V(2).Infof("Server %s is running", name)
		go srv.Run()
	}

	err := syscall.Unlink(types.ManagerSocket)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	l, err := net.Listen("unix", types.ManagerSocket)
	if err != nil {
		return err
	}

	klog.V(2).Infof("Server is ready at %s", types.ManagerSocket)

	return m.srv.Serve(l)
}

func (m *managerImpl) Stop() {
	for name, srv := range m.bundleServer {
		klog.V(2).Infof("Server %s is stopping", name)
		srv.Stop()
	}
	m.srv.Stop()
	klog.Fatal("Stop server")
}

func (m *managerImpl) validExtraConfig(path string) error {
	if path != "" {
		if _, err := os.Stat(path); err != nil {
			return err
		}

		file, err := os.Open(path)
		if err != nil {
			return err
		}

		cfg := make(map[string]*config.ExtraConfig)
		if err := json.NewDecoder(file).Decode(&cfg); err != nil {
			return err
		}
	}

	return nil
}

/** device plugin interface */
func (m *managerImpl) Allocate(ctx context.Context, reqs *pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error) {
	return m.allocator.Allocate(ctx, reqs)
}

func (m *managerImpl) ListAndWatchWithResourceName(resourceName string, e *pluginapi.Empty, s pluginapi.DevicePlugin_ListAndWatchServer) error {
	return m.allocator.ListAndWatchWithResourceName(resourceName, e, s)
}

func (m *managerImpl) GetDevicePluginOptions(ctx context.Context, e *pluginapi.Empty) (*pluginapi.DevicePluginOptions, error) {
	return m.allocator.GetDevicePluginOptions(ctx, e)
}

func (m *managerImpl) PreStartContainer(ctx context.Context, req *pluginapi.PreStartContainerRequest) (*pluginapi.PreStartContainerResponse, error) {
	return m.allocator.PreStartContainer(ctx, req)
}

/** statistics interface */
func (m *managerImpl) PrintGraph(ctx context.Context, req *google_protobuf1.Empty) (*displayapi.GraphResponse, error) {
	return m.displayer.PrintGraph(ctx, req)
}

func (m *managerImpl) PrintUsages(ctx context.Context, req *google_protobuf1.Empty) (*displayapi.UsageResponse, error) {
	return m.displayer.PrintUsages(ctx, req)
}

func (m *managerImpl) Version(ctx context.Context, req *google_protobuf1.Empty) (*displayapi.VersionResponse, error) {
	return m.displayer.Version(ctx, req)
}

// gpu-manager实现了device plugin，通过与kubelet进行交互，通过unix sock
// 问题； 重新注册怎么办？
// 这里面实现了k8s device plugin??
func (m *managerImpl) RegisterToKubelet() error {
	//获取kubelet unix sock file
	socketFile := filepath.Join(m.config.DevicePluginPath, types.KubeletSocket)
	//unix sock的grpc属性
	dialOptions := []grpc.DialOption{grpc.WithInsecure(), grpc.WithDialer(utils.UnixDial), grpc.WithBlock(), grpc.WithTimeout(time.Second * 5)}

	//gpu-manager连通kubelet，注册到kubelet上
	conn, err := grpc.Dial(socketFile, dialOptions...)
	if err != nil {
		return err
	}
	defer conn.Close()

	//k8s的kubelet api托管这个 unix sock grpc,生成的对象是这个kubelet client
	client := pluginapi.NewRegistrationClient(conn)

	//一捆server??这个server是什么server？
	//A:这里这个server是本device plugin创建的资源，例如vcore vmemory，并且封装为两个grpc service，
	//下面将这两个grpc service 地址unix sock作为参数，填写在与kubelet 的unix sock grpc通信中
	for _, srv := range m.bundleServer {
		//创建一个注册请求
		req := &pluginapi.RegisterRequest{
			//device plugin kubelet版本号
			Version: pluginapi.Version,
			//这是什么server？
			Endpoint: path.Base(srv.SocketName()),
			//资源名称
			ResourceName: srv.ResourceName(),
			//本device plugin的属性？
			Options: &pluginapi.DevicePluginOptions{PreStartRequired: true},
		}

		klog.V(2).Infof("Register to kubelet with endpoint %s", req.Endpoint)

		//向kubelet注册
		_, err = client.Register(context.Background(), req)
		if err != nil {
			return err
		}
	}

	return nil
}
