package tools

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	internalmcp "github.com/Tencent/WeKnora/internal/mcp"
	"github.com/Tencent/WeKnora/internal/modelcontext"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/utils"
	sdkmcp "github.com/mark3labs/mcp-go/mcp"
	sdkserver "github.com/mark3labs/mcp-go/server"
	"github.com/stretchr/testify/require"
)

func TestLoadMCPDirectoryUsesFreshSnapshotWithoutConnecting(t *testing.T) {
	ctx := catalogTestContext()
	service := &types.MCPService{ID: "svc", Name: "Orders", Enabled: true}
	want := []*types.MCPTool{{Name: "get_order", Description: "lookup"}}
	var gets atomic.Int32
	directory, err := loadMCPDirectory(ctx, service, nil, nil, nil, &MCPMetadataIO{
		Get: func(_ context.Context, tenant uint64, id string) (*types.MCPMetadata, error) {
			gets.Add(1)
			require.Equal(t, uint64(7), tenant)
			require.Equal(t, "svc", id)
			return &types.MCPMetadata{Tools: want, Instructions: "from-db"}, nil
		},
		Put: func(context.Context, uint64, string, []*types.MCPTool, string, types.MCPServerInfo, time.Time) error {
			t.Fatal("fresh snapshot must not be rewritten")
			return nil
		},
	}, false)
	require.NoError(t, err)
	require.Equal(t, int32(1), gets.Load())
	require.Equal(t, want, directory.tools)
	require.Equal(t, "from-db", directory.instructions)
	require.True(t, directory.saved)
}

func TestLoadMCPDirectoryDoesNotLiveFillStaleSnapshot(t *testing.T) {
	ctx := catalogTestContext()
	service := &types.MCPService{ID: "svc", Name: "Orders", Enabled: true}
	_, err := loadMCPDirectory(ctx, service, nil, nil, nil, &MCPMetadataIO{
		Get: func(context.Context, uint64, string) (*types.MCPMetadata, error) {
			return &types.MCPMetadata{
				Stale: true,
				Tools: []*types.MCPTool{{Name: "old"}},
			}, nil
		},
		Put: func(context.Context, uint64, string, []*types.MCPTool, string, types.MCPServerInfo, time.Time) error {
			t.Fatal("stale snapshot must not live-fill")
			return nil
		},
	}, false)
	require.ErrorContains(t, err, "stale")
}

func TestMCPDiscoveryRecoversStaleDirectory(t *testing.T) {
	ctx := catalogTestContext()
	server := newRedeployableMCPServer(t)
	server.deploy("2", "current instructions", "current_tool")
	saved := savedDirectory("1", "old instructions", time.Now(), "old_tool")
	saved.Stale = true
	store := &memoryDirectoryStore{snapshot: saved}
	other := &types.MCPService{ID: "other", Name: "Other", Enabled: true}
	metadata := store.io(nil)
	getSaved := metadata.Get
	metadata.Get = func(ctx context.Context, tenant uint64, id string) (*types.MCPMetadata, error) {
		if id == other.ID {
			return &types.MCPMetadata{}, nil
		}
		return getSaved(ctx, tenant, id)
	}
	manager := internalmcp.NewMCPManager(nil)
	t.Cleanup(manager.Shutdown)
	registry := NewToolRegistry()
	_, err := RegisterMCPTools(
		ctx, registry, []*types.MCPService{server.service(), other}, manager, nil, 0, nil, metadata,
	)
	require.NoError(t, err)

	registry.PrepareMCPTools(ctx)
	summaries := func() map[string]mcpServerSummary {
		defs := registry.GetModelFunctionDefinitions()
		require.Len(t, defs, 1)
		require.Equal(t, ToolDiscoverMCPTools, defs[0].Name)
		listed := discoverPage(ctx, t, registry, map[string]any{"mode": "list_servers"})
		require.Len(t, listed.Servers, 2)
		byID := make(map[string]mcpServerSummary)
		for _, row := range listed.Servers {
			encoded, err := json.Marshal(row)
			require.NoError(t, err)
			require.Contains(t, defs[0].Description, string(encoded))
			byID[row.ServerID] = row
		}
		return byID
	}
	before := summaries()
	require.Equal(t, "stale", before["svc"].Status)
	require.Contains(t, before["svc"].NextStep, "need this server")
	require.Contains(t, before["svc"].NextStep, "list_tools")
	require.Contains(t, before["svc"].NextStep, "refresh=true")
	require.Equal(t, "ready", before[other.ID].Status)
	require.Empty(t, before[other.ID].NextStep)

	failed, err := registry.ExecuteTool(
		ctx, ToolDiscoverMCPTools, json.RawMessage(`{"mode":"list_tools","server_id":"svc"}`),
	)
	require.NoError(t, err)
	require.False(t, failed.Success)
	require.Equal(t, "stale", failed.Data["status"])
	visible := modelcontext.NewRegistry(true).ModelToolResultForTool(ToolDiscoverMCPTools, failed)
	require.Contains(t, visible, "list_tools")
	require.Contains(t, visible, "refresh=true")
	require.Zero(t, server.initializes.Load(), "stale metadata alone must not initiate a connection")

	page := discoverPage(ctx, t, registry, map[string]any{"mode": "list_tools", "server_id": "svc", "refresh": true})
	require.Equal(t, "ready", page.Status)
	require.Len(t, page.Tools, 1)
	require.Equal(t, "current_tool", page.Tools[0].Name)
	snapshot, writes := store.current()
	require.Equal(t, 1, writes)
	require.False(t, snapshot.Stale)
	after := summaries()
	require.Equal(t, "ready", after["svc"].Status)
	require.Empty(t, after["svc"].NextStep)
	require.Equal(t, before[other.ID], after[other.ID])
}

func requireDirectoryFailure(
	ctx context.Context, t *testing.T, registry *ToolRegistry, args map[string]any, status string,
) string {
	t.Helper()
	raw, err := json.Marshal(args)
	require.NoError(t, err)
	result, err := registry.ExecuteTool(ctx, ToolDiscoverMCPTools, raw)
	require.NoError(t, err)
	require.False(t, result.Success)
	require.Equal(t, status, result.Data["status"])
	return result.Error
}

func TestMCPDiscoveryRechecksFailedStaleRefreshWhenSettingsChange(t *testing.T) {
	utils.SetSSRFWhitelistFromRaw("127.0.0.1")
	t.Cleanup(utils.ResetSSRFWhitelistForTest)
	var requests, changedRequests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path == "/changed" {
			changedRequests.Add(1)
		}
		http.Error(w, "private-upstream-detail", http.StatusServiceUnavailable)
	}))
	t.Cleanup(upstream.Close)
	service := types.MCPService{
		ID: "svc", Name: "Orders", Enabled: true, URL: &upstream.URL, TransportType: types.MCPTransportHTTPStreamable,
	}
	current := service
	lookup := func(context.Context, uint64, string) (*types.MCPService, error) {
		snapshot := current
		return &snapshot, nil
	}
	ctx := catalogTestContext()
	store := &memoryDirectoryStore{snapshot: &types.MCPMetadata{Stale: true}}
	manager := internalmcp.NewMCPManager(nil)
	t.Cleanup(manager.Shutdown)
	registry := NewToolRegistry()
	_, err := RegisterMCPTools(ctx, registry, []*types.MCPService{&service}, manager, nil, 0, lookup, store.io(nil))
	require.NoError(t, err)
	list := map[string]any{"mode": "list_tools", "server_id": service.ID}
	refresh := map[string]any{"mode": "list_tools", "server_id": service.ID, "refresh": true}
	describe := map[string]any{"mode": "describe", "server_id": service.ID, "tool_name": "external_tool"}
	require.Contains(t, requireDirectoryFailure(ctx, t, registry, list, "stale"), "refresh=true")
	failure := requireDirectoryFailure(ctx, t, registry, refresh, "error")

	current.UsageInstructions = "Find customer orders"
	current.UpdatedAt = current.UpdatedAt.Add(time.Second)
	require.Equal(t, failure, requireDirectoryFailure(ctx, t, registry, list, "error"))
	current.AdvancedConfig = &types.MCPAdvancedConfig{Timeout: 5}
	current.UpdatedAt = current.UpdatedAt.Add(time.Second)
	require.Equal(t, failure, requireDirectoryFailure(ctx, t, registry, list, "error"), "the 503 was not a timeout")

	newURL := upstream.URL + "/new"
	current.URL = &newURL
	current.UpdatedAt = current.UpdatedAt.Add(time.Second)
	require.Contains(t, requireDirectoryFailure(ctx, t, registry, list, "stale"), "refresh=true")
	requireDirectoryFailure(ctx, t, registry, refresh, "error")

	// The first read of the next configuration is an explicit refresh.
	changedURL := upstream.URL + "/changed"
	current.URL = &changedURL
	current.UpdatedAt = current.UpdatedAt.Add(time.Second)
	message := requireDirectoryFailure(ctx, t, registry, refresh, "error")
	require.NotContains(t, message, "private-upstream-detail")
	require.NotContains(t, message, "refresh=true")
	require.Positive(t, changedRequests.Load())
	after := requests.Load()
	row := discoverPage(ctx, t, registry, map[string]any{"mode": "list_servers"}).Servers[0]
	require.Equal(t, "error", row.Status)
	require.Empty(t, row.NextStep)
	require.NotEmpty(t, row.LastRefreshError)
	require.NotContains(t, row.LastRefreshError, "private-upstream-detail")
	for _, args := range []map[string]any{list, describe} {
		message := requireDirectoryFailure(ctx, t, registry, args, "error")
		require.Contains(t, message, "refresh failed")
		require.NotContains(t, message, "private-upstream-detail")
		require.NotContains(t, message, "refresh=true")
	}

	store.save(savedDirectory("2", "updated in Settings", time.Now(), "external_tool"))
	page := discoverPage(ctx, t, registry, list)
	require.Equal(t, "ready", page.Status)
	require.Len(t, page.Tools, 1)
	require.Equal(t, "external_tool", page.Tools[0].Name)
	describeTool(ctx, t, registry, service.ID, "external_tool")
	row = discoverPage(ctx, t, registry, map[string]any{"mode": "list_servers"}).Servers[0]
	require.Equal(t, "ready", row.Status)
	require.Empty(t, row.LastRefreshError)
	require.Equal(t, after, requests.Load())

	// A failed refresh of a fresh directory must still allow ordinary discovery
	// to live-fill it if the saved metadata is subsequently removed.
	message = requireDirectoryFailure(ctx, t, registry, refresh, "error")
	require.NotContains(t, message, "still needs a refresh")
	store.save(nil)
	beforeFill := requests.Load()
	requireDirectoryFailure(ctx, t, registry, list, "error")
	require.Greater(t, requests.Load(), beforeFill)
}

func TestMCPDiscoveryReconsidersTimeoutAfterBudgetChange(t *testing.T) {
	ctx, registry, catalog, _, _ := catalogFixture(t, 1)
	service := catalog.servers["server-1"].service
	service.AdvancedConfig = &types.MCPAdvancedConfig{Timeout: 1}
	current := *service
	catalog.lookup = func(context.Context, uint64, string) (*types.MCPService, error) {
		snapshot := current
		return &snapshot, nil
	}
	liveCalls := 0
	load := catalog.load
	catalog.load = func(ctx context.Context, s *types.MCPService, live bool) ([]*MCPTool, error) {
		if !live {
			return nil, errMCPDirectoryStale
		}
		liveCalls++
		if s.AdvancedConfig.Timeout == 1 {
			return nil, context.DeadlineExceeded
		}
		return load(ctx, s, live)
	}
	list := map[string]any{"mode": "list_tools", "server_id": service.ID}
	refresh := map[string]any{"mode": "list_tools", "server_id": service.ID, "refresh": true}
	describe := map[string]any{"mode": "describe", "server_id": service.ID, "tool_name": "tool_000"}
	require.Contains(t, requireDirectoryFailure(ctx, t, registry, list, "stale"), "refresh=true")
	require.Contains(t, requireDirectoryFailure(ctx, t, registry, refresh, "error"), "timed out")
	current.AdvancedConfig = &types.MCPAdvancedConfig{Timeout: 5}
	current.UpdatedAt = current.UpdatedAt.Add(time.Second)
	require.Equal(t, types.MCPConfigFingerprint(service), types.MCPConfigFingerprint(&current))
	for _, args := range []map[string]any{list, describe} {
		message := requireDirectoryFailure(ctx, t, registry, args, "stale")
		require.Contains(t, message, "refresh=true")
		require.NotContains(t, message, "timed out")
	}
	row := discoverPage(ctx, t, registry, map[string]any{"mode": "list_servers"}).Servers[0]
	require.Equal(t, "stale", row.Status)
	require.Contains(t, row.NextStep, "refresh=true")
	require.Empty(t, row.LastRefreshError)
	require.Equal(t, 1, liveCalls)
	page := discoverPage(ctx, t, registry, refresh)
	require.Equal(t, "ready", page.Status)
	describeTool(ctx, t, registry, service.ID, "tool_000")
	require.Equal(t, 2, liveCalls)
}

func TestMCPMissingStaticDirectoryIsLoadedOnFirstUse(t *testing.T) {
	for _, mode := range []string{"deferred", "direct"} {
		t.Run(mode, func(t *testing.T) {
			ctx := catalogTestContext()
			server := newRedeployableMCPServer(t)
			server.deploy("2", "Orders", "get_order")
			service := server.service()
			manager := internalmcp.NewMCPManager(nil)
			t.Cleanup(manager.Shutdown)
			store := &memoryDirectoryStore{}
			registry := NewToolRegistry()
			_, err := RegisterMCPTools(ctx, registry, []*types.MCPService{service}, manager, nil, 0, nil, store.io(nil))
			require.NoError(t, err)

			if mode == "direct" {
				registry.PrepareMCPToolsDirect(ctx)
				<-registry.mcpCatalog().preloadDone
				registry.RefreshMCPTools(ctx)
				require.Len(t, MCPToolNamesByServiceID(registry)[service.ID], 1)
			} else {
				registry.PrepareMCPTools(ctx)
				<-registry.mcpCatalog().preloadDone
				page := discoverPage(ctx, t, registry, map[string]any{"mode": "list_servers"})
				require.Len(t, page.Servers, 1)
				require.Equal(t, "not_loaded", page.Servers[0].Status)
				require.Empty(t, page.Servers[0].LastRefreshError)
				defs := registry.GetModelFunctionDefinitions()
				require.Len(t, defs, 1)
				require.Equal(t, ToolDiscoverMCPTools, defs[0].Name)
				require.Zero(t, server.initializes.Load())
				_, writes := store.current()
				require.Zero(t, writes)
			}

			page := discoverPage(ctx, t, registry, map[string]any{"mode": "list_tools", "server_id": service.ID})
			require.Equal(t, "ready", page.Status)
			require.Len(t, page.Tools, 1)
			described := describeTool(ctx, t, registry, service.ID, page.Tools[0].Name)
			registry.RefreshMCPTools(ctx)
			raw, err := json.Marshal(map[string]any{"tool_ref": described.ToolRef, "arguments": map[string]any{}})
			require.NoError(t, err)
			called, err := registry.ExecuteTool(ctx, ToolCallMCPTool, raw)
			require.NoError(t, err)
			require.True(t, called.Success, called.Error)
			require.Contains(t, called.Output, "ok 2")
			_, writes := store.current()
			require.Equal(t, 1, writes)
			require.EqualValues(t, 1, server.initializes.Load())
		})
	}
}

func TestLoadMCPDirectoryLiveFillsMissingNonOAuthDirectory(t *testing.T) {
	utils.SetSSRFWhitelistFromRaw("127.0.0.1")
	t.Cleanup(utils.ResetSSRFWhitelistForTest)
	server := sdkserver.NewMCPServer(
		"Orders",
		"1",
		sdkserver.WithToolCapabilities(false),
		sdkserver.WithInstructions("live"),
	)
	server.AddTool(
		sdkmcp.Tool{Name: "get_order", Description: "lookup", RawInputSchema: json.RawMessage(`{"type":"object"}`)},
		func(context.Context, sdkmcp.CallToolRequest) (*sdkmcp.CallToolResult, error) {
			return sdkmcp.NewToolResultText("ok"), nil
		},
	)
	upstream := httptest.NewServer(sdkserver.NewStreamableHTTPServer(server, sdkserver.WithStateLess(true)))
	t.Cleanup(upstream.Close)
	manager := internalmcp.NewMCPManager(nil)
	t.Cleanup(manager.Shutdown)
	service := &types.MCPService{
		ID:            "svc",
		Name:          "Orders",
		Enabled:       true,
		URL:           &upstream.URL,
		TransportType: types.MCPTransportHTTPStreamable,
	}
	var put atomic.Int32
	ctx := catalogTestContext()
	directory, err := loadMCPDirectory(ctx, service, manager, nil, nil, &MCPMetadataIO{
		Get: func(context.Context, uint64, string) (*types.MCPMetadata, error) { return nil, nil },
		Put: func(
			_ context.Context, tenant uint64, id string,
			listed []*types.MCPTool, text string, server types.MCPServerInfo, listedAt time.Time,
		) error {
			put.Add(1)
			require.Equal(t, types.MCPServerInfo{Name: "Orders", Version: "1"}, server)
			require.False(t, listedAt.IsZero(), "a live listing is dated by when it started")
			require.Equal(t, uint64(7), tenant)
			require.Equal(t, "svc", id)
			require.Equal(t, "live", text)
			require.Len(t, listed, 1)
			require.Equal(t, "get_order", listed[0].Name)
			return nil
		},
	}, false)
	require.NoError(t, err)
	require.Equal(t, int32(1), put.Load())
	require.Len(t, directory.tools, 1)
	require.Equal(t, "get_order", directory.tools[0].Name)
	require.Equal(t, "live", directory.instructions)
}

func TestLoadMCPDirectoryOAuthMissingRequiresToolExecContext(t *testing.T) {
	service := &types.MCPService{
		ID:         "svc",
		Name:       "Orders",
		Enabled:    true,
		AuthConfig: &types.MCPAuthConfig{AuthType: types.MCPAuthOAuth},
	}
	io := &MCPMetadataIO{
		Get: func(context.Context, uint64, string) (*types.MCPMetadata, error) { return nil, nil },
		Put: func(context.Context, uint64, string, []*types.MCPTool, string, types.MCPServerInfo, time.Time) error {
			t.Fatal("OAuth preload must not persist a directory")
			return nil
		},
	}
	_, err := loadMCPDirectory(catalogTestContext(), service, nil, nil, nil, io, false)
	require.ErrorContains(t, err, "MCP directory is missing")

	manager := internalmcp.NewMCPManager(nil)
	t.Cleanup(manager.Shutdown)
	execCtx := WithToolExecContext(catalogTestContext(), &ToolExecContext{ToolCallID: "describe-1"})
	_, err = loadMCPDirectory(execCtx, service, manager, nil, nil, io, false)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "MCP directory is missing")
}

func TestLoadMCPDirectoryExplicitRefreshRelistsExistingAndStaleSnapshots(t *testing.T) {
	utils.SetSSRFWhitelistFromRaw("127.0.0.1")
	t.Cleanup(utils.ResetSSRFWhitelistForTest)
	server := sdkserver.NewMCPServer(
		"Orders",
		"1",
		sdkserver.WithToolCapabilities(false),
		sdkserver.WithInstructions("live"),
	)
	server.AddTool(
		sdkmcp.Tool{Name: "get_order", Description: "lookup", RawInputSchema: json.RawMessage(`{"type":"object"}`)},
		func(context.Context, sdkmcp.CallToolRequest) (*sdkmcp.CallToolResult, error) {
			return sdkmcp.NewToolResultText("ok"), nil
		},
	)
	var upstream atomic.Int32
	inner := sdkserver.NewStreamableHTTPServer(server, sdkserver.WithStateLess(true))
	upstreamServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstream.Add(1)
		inner.ServeHTTP(w, r)
	}))
	t.Cleanup(upstreamServer.Close)
	manager := internalmcp.NewMCPManager(nil)
	t.Cleanup(manager.Shutdown)
	service := &types.MCPService{
		ID:            "svc",
		Name:          "Orders",
		Enabled:       true,
		URL:           &upstreamServer.URL,
		TransportType: types.MCPTransportHTTPStreamable,
	}
	var put atomic.Int32
	io := &MCPMetadataIO{
		Get: func(context.Context, uint64, string) (*types.MCPMetadata, error) {
			t.Fatal("explicit refresh must not return the saved snapshot")
			return nil, nil
		},
		Put: func(
			_ context.Context, _ uint64, _ string, listed []*types.MCPTool, text string, _ types.MCPServerInfo,
			_ time.Time,
		) error {
			put.Add(1)
			require.Equal(t, "live", text)
			require.Equal(t, "get_order", listed[0].Name)
			return nil
		},
	}
	ctx := catalogTestContext()
	directory, err := loadMCPDirectory(ctx, service, manager, nil, nil, io, true)
	require.NoError(t, err)
	require.Greater(t, upstream.Load(), int32(0))
	require.Equal(t, int32(1), put.Load())
	require.Equal(t, "get_order", directory.tools[0].Name)
	require.Equal(t, "live", directory.instructions)
}
