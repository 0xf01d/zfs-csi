package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/0xf01d/zfs-csi/pkg/backend"
	"github.com/0xf01d/zfs-csi/pkg/driver"
	"github.com/0xf01d/zfs-csi/pkg/mount"
	"github.com/0xf01d/zfs-csi/pkg/peer"
	"github.com/0xf01d/zfs-csi/pkg/replication"
	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var version = "dev"

type peerFlags []replication.Peer

func (p *peerFlags) String() string {
	var parts []string
	for _, e := range *p {
		parts = append(parts, e.Node+"="+e.Client.BaseURL)
	}
	return strings.Join(parts, ",")
}

func (p *peerFlags) Set(v string) error {
	node, addr := "", v
	if i := strings.Index(v, "="); i > 0 {
		node, addr = v[:i], v[i+1:]
	}
	if node == "" {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			return fmt.Errorf("peer %q must be node=host:port: %w", v, err)
		}
		node = host
	}
	*p = append(*p, replication.Peer{Node: node, Client: peer.NewClient(addr, node, nil)})
	return nil
}

func main() {
	var (
		endpoint      = flag.String("endpoint", "unix:///csi/csi.sock", "CSI endpoint")
		mode          = flag.String("mode", "", "controller or node")
		driverName    = flag.String("driver-name", "zfs-csi.0xf01d.github.io", "CSI driver name")
		nodeID        = flag.String("node-id", os.Getenv("NODE_NAME"), "node identity, usually spec.nodeName")
		pool          = flag.String("pool", "tank/k8s", "parent ZFS dataset for volumes; must already exist")
		topologyKey   = flag.String("topology-key", "topology.zfs-csi.io/node", "CSI topology key")
		zfsPath       = flag.String("zfs-path", "zfs", "path to zfs(8)")
		peerListen    = flag.String("peer-listen", ":9809", "address for the peer replication API (node mode)")
		peerPort      = flag.String("peer-port", "9809", "TCP port peers listen on, used with --peer-discovery")
		peerDiscovery = flag.Bool("peer-discovery", true, "discover peers from Kubernetes Nodes when no --peer is given")
		tlsCert       = flag.String("tls-cert", "", "PEM certificate for peer mutual TLS")
		tlsKey        = flag.String("tls-key", "", "PEM key for peer mutual TLS")
		tlsCA         = flag.String("tls-ca", "", "PEM CA for peer mutual TLS")
		replInterval  = flag.Duration("replicate-interval", 0, "periodic replication interval (0 = event driven)")
		keepSnapshots = flag.Int("keep-snapshots", 10, "number of @rep-* snapshots to keep (0 = keep all)")
		maxVolumes    = flag.Int64("max-volumes-per-node", 0, "CSI advertised MaxVolumesPerNode")
		kubeconfig    = flag.String("kubeconfig", "", "kubeconfig for node discovery (in-cluster by default)")
		showVersion   = flag.Bool("version", false, "print version and exit")
	)
	var peers peerFlags
	flag.Var(&peers, "peer", "replication peer, repeatable: node=host:port")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}
	if *mode != "controller" && *mode != "node" {
		log.Fatal("--mode must be controller or node")
	}
	if *pool == "" {
		log.Fatal("--pool is required")
	}

	tlsFiles := peer.TLSFiles{CertFile: *tlsCert, KeyFile: *tlsKey, CAFile: *tlsCA}
	clientTLS, err := tlsFiles.ClientConfig()
	if err != nil {
		log.Fatalf("peer client TLS: %v", err)
	}
	serverTLS, err := tlsFiles.ServerConfig()
	if err != nil {
		log.Fatalf("peer server TLS: %v", err)
	}
	for i := range peers {
		base := peers[i].Client.BaseURL
		if clientTLS != nil && strings.HasPrefix(base, "http://") {
			base = "https://" + strings.TrimPrefix(base, "http://")
		}
		peers[i].Client = peer.NewClient(base, peers[i].Node, clientTLS)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	kubeNodes := discoverKubeNodes(ctx, *kubeconfig)
	if len(peers) == 0 && *peerDiscovery {
		for _, kn := range kubeNodes {
			if *mode == "node" && kn.Name == *nodeID {
				continue
			}
			if kn.Address == "" {
				log.Printf("peer discovery: node %s has no address, skipped", kn.Name)
				continue
			}
			addr := net.JoinHostPort(kn.Address, *peerPort)
			peers = append(peers, replication.Peer{Node: kn.Name, Client: peer.NewClient(addr, kn.Name, clientTLS)})
		}
	}
	var nodeNames []string
	for _, kn := range kubeNodes {
		nodeNames = append(nodeNames, kn.Name)
	}

	identity := &driver.Identity{DriverName: *driverName, Version: version}
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(16 << 20),
		grpc.MaxSendMsgSize(16 << 20),
	}
	server := grpc.NewServer(opts...)
	csi.RegisterIdentityServer(server, identity)

	switch *mode {
	case "controller":
		ctl := driver.NewController(*driverName, *pool, *topologyKey, peers, nodeNames)
		csi.RegisterControllerServer(server, ctl)
		log.Printf("controller: driver=%s pool=%s peers=%d nodes=%v", *driverName, *pool, len(peers), nodeNames)
	case "node":
		if *nodeID == "" {
			log.Fatal("--node-id (or NODE_NAME) is required in node mode")
		}
		b := backend.NewZFS(*zfsPath)
		rep := replication.New(b, *pool, *nodeID, peers, *keepSnapshots, 30*time.Minute)
		peerServer := peer.NewServer(peer.ServerConfig{
			NodeName:  *nodeID,
			Pool:      *pool,
			Backend:   b,
			ClientTLS: clientTLS,
			OnSnapshot: func(volume, snapshot string) {
				rep.NotifyAsync(volume, snapshot)
			},
		})
		if *peerListen != "" {
			go func() {
				if err := peerServer.Run(ctx, *peerListen, serverTLS); err != nil {
					log.Fatalf("peer server: %v", err)
				}
			}()
			log.Printf("node %s: peer API on %s (tls=%v)", *nodeID, *peerListen, serverTLS != nil)
		}
		go rep.Run(ctx, *replInterval)
		node := driver.NewNode(*driverName, *nodeID, *pool, *topologyKey, *maxVolumes, b, mount.NewLinux(), rep)
		csi.RegisterNodeServer(server, node)
		log.Printf("node %s: driver=%s pool=%s peers=%d", *nodeID, *driverName, *pool, len(peers))
	}

	lis, err := listen(*endpoint)
	if err != nil {
		log.Fatalf("listen %s: %v", *endpoint, err)
	}
	go func() {
		<-ctx.Done()
		server.GracefulStop()
	}()
	log.Printf("serving CSI on %s", *endpoint)
	if err := server.Serve(lis); err != nil {
		log.Fatalf("grpc: %v", err)
	}
}

func listen(endpoint string) (net.Listener, error) {
	switch {
	case strings.HasPrefix(endpoint, "unix://"):
		path := strings.TrimPrefix(endpoint, "unix://")
		if path == "" {
			return nil, fmt.Errorf("empty unix socket path")
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			return nil, err
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		return net.Listen("unix", path)
	case strings.HasPrefix(endpoint, "tcp://"):
		return net.Listen("tcp", strings.TrimPrefix(endpoint, "tcp://"))
	default:
		return nil, fmt.Errorf("unsupported endpoint %q", endpoint)
	}
}

type kubeNode struct {
	Name    string
	Address string
}

func discoverKubeNodes(ctx context.Context, kubeconfig string) []kubeNode {
	var (
		cfg *rest.Config
		err error
	)
	if kubeconfig != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		cfg, err = rest.InClusterConfig()
	}
	if err != nil {
		log.Printf("node discovery disabled: %v", err)
		return nil
	}
	cfg.Timeout = 10 * time.Second
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Printf("node discovery disabled: %v", err)
		return nil
	}
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	list, err := client.CoreV1().Nodes().List(cctx, metav1.ListOptions{})
	if err != nil {
		log.Printf("node discovery failed: %v", err)
		return nil
	}
	var nodes []kubeNode
	for _, n := range list.Items {
		addr := ""
		for _, a := range n.Status.Addresses {
			if a.Type == "InternalIP" {
				addr = a.Address
				break
			}
		}
		if addr == "" {
			for _, a := range n.Status.Addresses {
				if a.Type == "ExternalIP" || a.Type == "Hostname" {
					addr = a.Address
					break
				}
			}
		}
		nodes = append(nodes, kubeNode{Name: n.Name, Address: addr})
	}
	return nodes
}
