// Package peer moves ZFS datasets between nodes.
//
// The transport is HTTP over optional mutual TLS. A snapshot stream is just
// an HTTP body: the target asks the source for /v1/send and pipes the response
// into zfs receive. There is no database and no controller in the data path.
package peer

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/0xf01d/zfs-csi/pkg/backend"
	"github.com/0xf01d/zfs-csi/pkg/naming"
)

// DatasetStatus is the answer to "does this node have the volume, and which
// snapshots does it know about?".
type DatasetStatus struct {
	Volume    string                 `json:"volume"`
	Exists    bool                   `json:"exists"`
	UsedBytes int64                  `json:"usedBytes"`
	Snapshots []backend.SnapshotInfo `json:"snapshots,omitempty"`
}

// CreateRequest creates a dataset, optionally cloning a locally available
// snapshot.
type CreateRequest struct {
	Volume         string `json:"volume"`
	SourceSnapshot string `json:"sourceSnapshot,omitempty"`
}

// SnapshotRequest creates one snapshot on the target node.
type SnapshotRequest struct {
	Volume   string `json:"volume"`
	Snapshot string `json:"snapshot"`
}

// ImportRequest asks a node to pull a snapshot from another node.
type ImportRequest struct {
	Volume       string `json:"volume"`
	SourceVolume string `json:"sourceVolume,omitempty"`
	Snapshot     string `json:"snapshot"`
	From         string `json:"from,omitempty"`
	Source       string `json:"source"`
}

// ExpandRequest changes the quota of a dataset.
type ExpandRequest struct {
	Volume string `json:"volume"`
	Bytes  int64  `json:"bytes"`
}

// ServerConfig configures a node's peer endpoint.
type ServerConfig struct {
	NodeName   string
	Pool       string
	Backend    backend.Backend
	ClientTLS  *tls.Config
	OnSnapshot func(volume, snapshot string)
}

// Server is the node-local peer HTTP API.
type Server struct {
	cfg ServerConfig
}

func NewServer(cfg ServerConfig) *Server { return &Server{cfg: cfg} }

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/health", s.health)
	mux.HandleFunc("/v1/dataset", s.dataset)
	mux.HandleFunc("/v1/datasets", s.datasets)
	mux.HandleFunc("/v1/volumes", s.volumes)
	mux.HandleFunc("/v1/snapshots", s.snapshots)
	mux.HandleFunc("/v1/send", s.send)
	mux.HandleFunc("/v1/receive", s.receive)
	mux.HandleFunc("/v1/import", s.importSnapshot)
	mux.HandleFunc("/v1/clone", s.clone)
	mux.HandleFunc("/v1/expand", s.expand)
	return mux
}

// Run serves until ctx is cancelled.
func (s *Server) Run(ctx context.Context, addr string, serverTLS *tls.Config) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	if serverTLS != nil {
		ln = tls.NewListener(ln, serverTLS)
	}
	srv := &http.Server{
		Handler:           s.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	err = srv.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "node": s.cfg.NodeName})
}

func (s *Server) dataset(w http.ResponseWriter, r *http.Request) {
	volume := r.URL.Query().Get("volume")
	if volume == "" {
		http.Error(w, "volume is required", http.StatusBadRequest)
		return
	}
	ds := naming.Dataset(s.cfg.Pool, volume)
	switch r.Method {
	case http.MethodGet:
		status, err := s.status(volume, ds)
		if err != nil {
			writeBackendError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, status)
	case http.MethodDelete:
		if err := s.cfg.Backend.DestroyDataset(ds, true); err != nil {
			writeBackendError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"deleted": true})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) status(volume, ds string) (DatasetStatus, error) {
	exists, err := s.cfg.Backend.DatasetExists(ds)
	if err != nil {
		return DatasetStatus{}, err
	}
	st := DatasetStatus{Volume: volume, Exists: exists}
	if !exists {
		return st, nil
	}
	st.UsedBytes, _ = s.cfg.Backend.UsedBytes(ds)
	st.Snapshots, _ = s.cfg.Backend.ListSnapshots(ds)
	return st, nil
}

func (s *Server) datasets(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req CreateRequest
	if err := decode(r, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ds := naming.Dataset(s.cfg.Pool, req.Volume)
	var err error
	if req.SourceSnapshot != "" {
		err = s.cfg.Backend.Clone(req.SourceSnapshot, ds)
	} else {
		err = s.cfg.Backend.CreateDataset(ds)
	}
	if err != nil {
		writeBackendError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"created": true})
}

func (s *Server) volumes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	datasets, err := s.cfg.Backend.ListDatasets(s.cfg.Pool)
	if err != nil && !errors.Is(err, backend.ErrNotFound) {
		writeBackendError(w, err)
		return
	}
	prefix := strings.TrimSuffix(s.cfg.Pool, "/") + "/"
	volumes := make([]string, 0, len(datasets))
	for _, ds := range datasets {
		if strings.HasPrefix(ds, prefix) {
			volumes = append(volumes, strings.TrimPrefix(ds, prefix))
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"volumes": volumes})
}

func (s *Server) snapshots(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	volume := q.Get("volume")
	if volume == "" {
		http.Error(w, "volume is required", http.StatusBadRequest)
		return
	}
	ds := naming.Dataset(s.cfg.Pool, volume)
	switch r.Method {
	case http.MethodGet:
		status, err := s.status(volume, ds)
		if err != nil {
			writeBackendError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, status)
	case http.MethodPost:
		var req SnapshotRequest
		if err := decode(r, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.Volume != volume {
			http.Error(w, "volume mismatch", http.StatusBadRequest)
			return
		}
		if err := s.cfg.Backend.Snapshot(ds, req.Snapshot); err != nil {
			writeBackendError(w, err)
			return
		}
		if s.cfg.OnSnapshot != nil {
			s.cfg.OnSnapshot(volume, req.Snapshot)
		}
		writeJSON(w, http.StatusOK, map[string]bool{"created": true})
	case http.MethodDelete:
		snap := q.Get("snapshot")
		if snap == "" {
			http.Error(w, "snapshot is required", http.StatusBadRequest)
			return
		}
		if err := s.cfg.Backend.DestroySnapshot(ds, snap); err != nil {
			writeBackendError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"deleted": true})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) send(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	q := r.URL.Query()
	volume := q.Get("volume")
	to := q.Get("to")
	from := q.Get("from")
	if volume == "" || to == "" {
		http.Error(w, "volume and to are required", http.StatusBadRequest)
		return
	}
	// The stream names are full dataset@snapshot names. If only a snapshot
	// component was provided, resolve it against the volume's dataset.
	if !strings.Contains(to, "@") {
		to = naming.Snapshot(naming.Dataset(s.cfg.Pool, volume), to)
	}
	if from != "" && !strings.Contains(from, "@") {
		from = naming.Snapshot(naming.Dataset(s.cfg.Pool, volume), from)
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	if err := s.cfg.Backend.SendIncremental(from, to, w); err != nil {
		// Headers are already sent; the client sees a truncated stream and
		// zfs receive rejects it. Log through the response trailer is not
		// reliable, so this is intentionally best effort.
		return
	}
}

func (s *Server) receive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	q := r.URL.Query()
	volume := q.Get("volume")
	from := q.Get("from")
	if volume == "" {
		http.Error(w, "volume is required", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()
	ds := naming.Dataset(s.cfg.Pool, volume)
	exists, err := s.cfg.Backend.DatasetExists(ds)
	if err != nil {
		writeBackendError(w, err)
		return
	}
	if exists && from == "" {
		if err := s.cfg.Backend.DestroyDataset(ds, true); err != nil {
			writeBackendError(w, err)
			return
		}
	}
	if err := s.cfg.Backend.Receive(r.Body, ds); err != nil {
		writeBackendError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"received": true})
}

func (s *Server) importSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req ImportRequest
	if err := decode(r, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Source == "" || req.Snapshot == "" || req.Volume == "" {
		http.Error(w, "volume, snapshot and source are required", http.StatusBadRequest)
		return
	}
	u, err := url.Parse(req.Source)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		http.Error(w, "source must be an http(s) URL", http.StatusBadRequest)
		return
	}
	sourceVolume := req.SourceVolume
	if sourceVolume == "" {
		sourceVolume = req.Volume
	}
	q := u.Query()
	q.Set("volume", sourceVolume)
	q.Set("to", req.Snapshot)
	if req.From != "" {
		q.Set("from", req.From)
	}
	u.Path = strings.TrimSuffix(u.Path, "/") + "/v1/send"
	u.RawQuery = q.Encode()

	client := &http.Client{Transport: &http.Transport{TLSClientConfig: s.cfg.ClientTLS}}
	resp, err := client.Get(u.String())
	if err != nil {
		writeBackendError(w, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		http.Error(w, fmt.Sprintf("source send failed: %s: %s", resp.Status, strings.TrimSpace(string(b))), http.StatusBadGateway)
		return
	}

	ds := naming.Dataset(s.cfg.Pool, req.Volume)
	exists, err := s.cfg.Backend.DatasetExists(ds)
	if err != nil {
		writeBackendError(w, err)
		return
	}
	if exists && req.From == "" {
		if err := s.cfg.Backend.DestroyDataset(ds, true); err != nil {
			writeBackendError(w, err)
			return
		}
	}
	if err := s.cfg.Backend.Receive(resp.Body, ds); err != nil {
		writeBackendError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"received": true})
}

func (s *Server) clone(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req CreateRequest
	if err := decode(r, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.SourceSnapshot == "" {
		http.Error(w, "sourceSnapshot is required", http.StatusBadRequest)
		return
	}
	target := naming.Dataset(s.cfg.Pool, req.Volume)
	if err := s.cfg.Backend.Clone(req.SourceSnapshot, target); err != nil {
		writeBackendError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"cloned": true})
}

func (s *Server) expand(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req ExpandRequest
	if err := decode(r, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.cfg.Backend.SetQuota(naming.Dataset(s.cfg.Pool, req.Volume), req.Bytes); err != nil {
		writeBackendError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"expanded": true})
}

func decode(r *http.Request, v any) error {
	defer r.Body.Close()
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("invalid json: %w", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeBackendError(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	if errors.Is(err, backend.ErrNotFound) {
		code = http.StatusNotFound
	}
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

// Client talks to one peer. BaseURL must not have a trailing slash.
type Client struct {
	BaseURL string
	Node    string
	HTTP    *http.Client
}

func NewClient(baseURL, node string, tlsCfg *tls.Config) *Client {
	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		scheme := "http"
		if tlsCfg != nil {
			scheme = "https"
		}
		baseURL = scheme + "://" + baseURL
	}
	return &Client{
		BaseURL: strings.TrimSuffix(baseURL, "/"),
		Node:    node,
		HTTP:    &http.Client{Transport: &http.Transport{TLSClientConfig: tlsCfg}},
	}
}

func (c *Client) Health(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/v1/health", nil)
	if err != nil {
		return err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health: %s", resp.Status)
	}
	return nil
}

func (c *Client) Volumes(ctx context.Context) ([]string, error) {
	var out struct {
		Volumes []string `json:"volumes"`
	}
	err := c.getJSON(ctx, "/v1/volumes", &out)
	return out.Volumes, err
}

func (c *Client) Status(ctx context.Context, volume string) (DatasetStatus, error) {
	var st DatasetStatus
	err := c.getJSON(ctx, "/v1/dataset?volume="+url.QueryEscape(volume), &st)
	return st, err
}

func (c *Client) CreateDataset(ctx context.Context, volume, sourceSnapshot string) error {
	return c.postJSON(ctx, "/v1/datasets", CreateRequest{Volume: volume, SourceSnapshot: sourceSnapshot}, nil)
}

func (c *Client) DeleteDataset(ctx context.Context, volume string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.BaseURL+"/v1/dataset?volume="+url.QueryEscape(volume), nil)
	if err != nil {
		return err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return responseError(resp)
	}
	return nil
}

func (c *Client) CreateSnapshot(ctx context.Context, volume, snapshot string) error {
	return c.postJSON(ctx, "/v1/snapshots?volume="+url.QueryEscape(volume), SnapshotRequest{Volume: volume, Snapshot: snapshot}, nil)
}

func (c *Client) DeleteSnapshot(ctx context.Context, volume, snapshot string) error {
	u := c.BaseURL + "/v1/snapshots?volume=" + url.QueryEscape(volume) + "&snapshot=" + url.QueryEscape(snapshot)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, nil)
	if err != nil {
		return err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return responseError(resp)
	}
	return nil
}

func (c *Client) Send(ctx context.Context, volume, from, to string) (io.ReadCloser, error) {
	q := url.Values{}
	q.Set("volume", volume)
	q.Set("to", to)
	if from != "" {
		q.Set("from", from)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/v1/send?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		return nil, responseError(resp)
	}
	return resp.Body, nil
}

func (c *Client) Receive(ctx context.Context, volume, from string, body io.Reader) error {
	q := url.Values{}
	q.Set("volume", volume)
	if from != "" {
		q.Set("from", from)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/v1/receive?"+q.Encode(), body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return responseError(resp)
	}
	return nil
}

func (c *Client) Import(ctx context.Context, volume, sourceVolume, snapshot, from, source string) error {
	return c.postJSON(ctx, "/v1/import", ImportRequest{Volume: volume, SourceVolume: sourceVolume, Snapshot: snapshot, From: from, Source: source}, nil)
}

func (c *Client) Clone(ctx context.Context, volume, snapshot string) error {
	return c.postJSON(ctx, "/v1/clone", CreateRequest{Volume: volume, SourceSnapshot: snapshot}, nil)
}

func (c *Client) Expand(ctx context.Context, volume string, bytes int64) error {
	return c.postJSON(ctx, "/v1/expand", ExpandRequest{Volume: volume, Bytes: bytes}, nil)
}

func (c *Client) getJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return responseError(resp)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *Client) postJSON(ctx context.Context, path string, in, out any) error {
	b, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+path, strings.NewReader(string(b)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return responseError(resp)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func responseError(resp *http.Response) error {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	msg := strings.TrimSpace(string(b))
	var e struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(b, &e) == nil && e.Error != "" {
		msg = e.Error
	}
	if resp.StatusCode == http.StatusNotFound {
		return backend.ErrNotFound
	}
	return fmt.Errorf("peer %s: %s: %s", resp.Request.URL.Host, resp.Status, msg)
}

// LatestSnapshot returns the snapshot with the highest creation time.
func LatestSnapshot(st DatasetStatus) (backend.SnapshotInfo, bool) {
	var latest backend.SnapshotInfo
	found := false
	for _, s := range st.Snapshots {
		if !found || s.Creation.After(latest.Creation) {
			latest = s
			found = true
		}
	}
	return latest, found
}

// SnapshotExists reports whether name is in st.
func SnapshotExists(st DatasetStatus, name string) bool {
	for _, s := range st.Snapshots {
		if s.Name == name {
			return true
		}
	}
	return false
}

// CommonSnapshot returns the newest local snapshot that also exists remotely.
func CommonSnapshot(local, remote []backend.SnapshotInfo) (backend.SnapshotInfo, bool) {
	set := make(map[string]bool, len(remote))
	for _, s := range remote {
		set[s.Name] = true
	}
	var best backend.SnapshotInfo
	found := false
	for _, s := range local {
		if !set[s.Name] {
			continue
		}
		if !found || s.Creation.After(best.Creation) {
			best = s
			found = true
		}
	}
	return best, found
}

// ParsePort is a helper for --peer flags of the form node=host:port.
func ParsePort(s string) (host, port string, err error) {
	h, p, err := net.SplitHostPort(s)
	if err != nil {
		return "", "", err
	}
	if _, err := strconv.Atoi(p); err != nil {
		return "", "", fmt.Errorf("invalid port %q", p)
	}
	return h, p, nil
}
