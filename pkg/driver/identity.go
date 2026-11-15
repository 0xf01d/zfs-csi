package driver

import (
	"context"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Identity implements the CSI Identity service.
type Identity struct {
	csi.UnimplementedIdentityServer
	DriverName string
	Version    string
}

func (i *Identity) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	if i.DriverName == "" {
		return nil, status.Error(codes.Unavailable, "driver name not configured")
	}
	return &csi.GetPluginInfoResponse{
		Name:          i.DriverName,
		VendorVersion: i.Version,
	}, nil
}

func (i *Identity) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{
		Capabilities: []*csi.PluginCapability{
			{Type: &csi.PluginCapability_Service_{Service: &csi.PluginCapability_Service{
				Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
			}}},
			{Type: &csi.PluginCapability_Service_{Service: &csi.PluginCapability_Service{
				Type: csi.PluginCapability_Service_VOLUME_ACCESSIBILITY_CONSTRAINTS,
			}}},
		},
	}, nil
}

func (i *Identity) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{}, nil
}
