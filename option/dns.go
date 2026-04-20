package option

import (
	"context"
	"net/netip"

	C "github.com/sagernet/sing-box/constant"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/json"
	"github.com/sagernet/sing/common/json/badjson"
	"github.com/sagernet/sing/common/json/badoption"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/service"
)

type RawDNSOptions struct {
	Servers        []DNSServerOptions `json:"servers,omitempty"`
	Rules          []DNSRule          `json:"rules,omitempty"`
	Final          string             `json:"final,omitempty"`
	ReverseMapping bool               `json:"reverse_mapping,omitempty"`
	DNSClientOptions
}

type DNSOptions struct {
	RawDNSOptions
}

const (
	legacyDNSFakeIPRemovedMessage = "legacy DNS fakeip options are deprecated in sing-box 1.12.0 and removed in sing-box 1.14.0, checkout migration: https://sing-box.sagernet.org/migration/#migrate-to-new-dns-server-formats"
	legacyDNSServerRemovedMessage = "legacy DNS server formats are deprecated in sing-box 1.12.0 and removed in sing-box 1.14.0, checkout migration: https://sing-box.sagernet.org/migration/#migrate-to-new-dns-server-formats"
)

type removedLegacyDNSOptions struct {
	FakeIP json.RawMessage `json:"fakeip,omitempty"`
}

func (o *DNSOptions) UnmarshalJSONContext(ctx context.Context, content []byte) error {
	var legacyOptions removedLegacyDNSOptions
	err := json.UnmarshalContext(ctx, content, &legacyOptions)
	if err != nil {
		return err
	}
	if len(legacyOptions.FakeIP) != 0 {
		return E.New(legacyDNSFakeIPRemovedMessage)
	}
	return badjson.UnmarshallExcludedContext(ctx, content, legacyOptions, &o.RawDNSOptions)
}

type DNSClientOptions struct {
	Strategy         DomainStrategy        `json:"strategy,omitempty"`
	DisableCache     bool                  `json:"disable_cache,omitempty"`
	DisableExpire    bool                  `json:"disable_expire,omitempty"`
	IndependentCache bool                  `json:"independent_cache,omitempty"`
	CacheCapacity    uint32                `json:"cache_capacity,omitempty"`
	Optimistic       *OptimisticDNSOptions `json:"optimistic,omitempty"`
	ClientSubnet     *badoption.Prefixable `json:"client_subnet,omitempty"`
}

type _OptimisticDNSOptions struct {
	Enabled bool               `json:"enabled,omitempty"`
	Timeout badoption.Duration `json:"timeout,omitempty"`
}

type OptimisticDNSOptions _OptimisticDNSOptions

func (o OptimisticDNSOptions) MarshalJSON() ([]byte, error) {
	if o.Timeout == 0 {
		return json.Marshal(o.Enabled)
	}
	return json.Marshal((_OptimisticDNSOptions)(o))
}

func (o *OptimisticDNSOptions) UnmarshalJSON(bytes []byte) error {
	err := json.Unmarshal(bytes, &o.Enabled)
	if err == nil {
		return nil
	}
	return json.UnmarshalDisallowUnknownFields(bytes, (*_OptimisticDNSOptions)(o))
}

type DNSTransportOptionsRegistry interface {
	CreateOptions(transportType string) (any, bool)
}
type _DNSServerOptions struct {
	Type    string `json:"type,omitempty"`
	Tag     string `json:"tag,omitempty"`
	Options any    `json:"-"`
}

type DNSServerOptions _DNSServerOptions

func (o *DNSServerOptions) MarshalJSONContext(ctx context.Context) ([]byte, error) {
	return badjson.MarshallObjectsContext(ctx, (*_DNSServerOptions)(o), o.Options)
}

func (o *DNSServerOptions) UnmarshalJSONContext(ctx context.Context, content []byte) error {
	err := json.UnmarshalContext(ctx, content, (*_DNSServerOptions)(o))
	if err != nil {
		return err
	}
	registry := service.FromContext[DNSTransportOptionsRegistry](ctx)
	if registry == nil {
		return E.New("missing DNS transport options registry in context")
	}
	var options any
	switch o.Type {
	case "", C.DNSTypeLegacy:
		return E.New(legacyDNSServerRemovedMessage)
	default:
		var loaded bool
		options, loaded = registry.CreateOptions(o.Type)
		if !loaded {
			return E.New("unknown transport type: ", o.Type)
		}
	}
	err = badjson.UnmarshallExcludedContext(ctx, content, (*_DNSServerOptions)(o), options)
	if err != nil {
		return err
	}
	o.Options = options
	if o.Type == C.DNSTypeLegacy && !dontUpgradeFromContext(ctx) {
		err = o.Upgrade(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *DNSServerOptions) Upgrade(ctx context.Context) error {
	if o.Type != C.DNSTypeLegacy {
		return nil
	}
	options := o.Options.(*LegacyDNSServerOptions)
	serverURL, _ := url.Parse(options.Address)
	var serverType string
	if serverURL != nil && serverURL.Scheme != "" {
		serverType = serverURL.Scheme
	} else {
		switch options.Address {
		case "local", "fakeip":
			serverType = options.Address
		default:
			serverType = C.DNSTypeUDP
		}
	}
	remoteOptions := RemoteDNSServerOptions{
		RawLocalDNSServerOptions: RawLocalDNSServerOptions{
			DialerOptions: DialerOptions{
				Detour: options.Detour,
				DomainResolver: &DomainResolveOptions{
					Server:   options.AddressResolver,
					Strategy: options.AddressStrategy,
				},
				FallbackDelay: options.AddressFallbackDelay,
			},
			Legacy:              true,
			LegacyStrategy:      options.Strategy,
			LegacyDefaultDialer: options.Detour == "",
			LegacyClientSubnet:  options.ClientSubnet.Build(netip.Prefix{}),
		},
		LegacyAddressResolver:      options.AddressResolver,
		LegacyAddressStrategy:      options.AddressStrategy,
		LegacyAddressFallbackDelay: options.AddressFallbackDelay,
	}
	switch serverType {
	case C.DNSTypeLocal:
		o.Type = C.DNSTypeLocal
		o.Options = &LocalDNSServerOptions{
			RawLocalDNSServerOptions: remoteOptions.RawLocalDNSServerOptions,
		}
	case C.DNSTypeUDP:
		o.Type = C.DNSTypeUDP
		o.Options = &remoteOptions
		var serverAddr M.Socksaddr
		if serverURL == nil || serverURL.Scheme == "" {
			serverAddr = M.ParseSocksaddr(options.Address)
		} else {
			serverAddr = M.ParseSocksaddr(serverURL.Host)
		}
		if !serverAddr.IsValid() {
			return E.New("invalid server address")
		}
		remoteOptions.Server = serverAddr.AddrString()
		if serverAddr.Port != 0 && serverAddr.Port != 53 {
			remoteOptions.ServerPort = serverAddr.Port
		}
	case C.DNSTypeTCP:
		o.Type = C.DNSTypeTCP
		o.Options = &remoteOptions
		if serverURL == nil {
			return E.New("invalid server address")
		}
		serverAddr := M.ParseSocksaddr(serverURL.Host)
		if !serverAddr.IsValid() {
			return E.New("invalid server address")
		}
		remoteOptions.Server = serverAddr.AddrString()
		if serverAddr.Port != 0 && serverAddr.Port != 53 {
			remoteOptions.ServerPort = serverAddr.Port
		}
	case C.DNSTypeTLS, C.DNSTypeQUIC:
		o.Type = serverType
		if serverURL == nil {
			return E.New("invalid server address")
		}
		serverAddr := M.ParseSocksaddr(serverURL.Host)
		if !serverAddr.IsValid() {
			return E.New("invalid server address")
		}
		remoteOptions.Server = serverAddr.AddrString()
		if serverAddr.Port != 0 && serverAddr.Port != 853 {
			remoteOptions.ServerPort = serverAddr.Port
		}
		o.Options = &RemoteTLSDNSServerOptions{
			RemoteDNSServerOptions: remoteOptions,
		}
	case C.DNSTypeHTTPS, C.DNSTypeHTTP3:
		o.Type = serverType
		httpsOptions := RemoteHTTPSDNSServerOptions{
			RemoteTLSDNSServerOptions: RemoteTLSDNSServerOptions{
				RemoteDNSServerOptions: remoteOptions,
			},
		}
		o.Options = &httpsOptions
		if serverURL == nil {
			return E.New("invalid server address")
		}
		serverAddr := M.ParseSocksaddr(serverURL.Host)
		if !serverAddr.IsValid() {
			return E.New("invalid server address")
		}
		httpsOptions.Server = serverAddr.AddrString()
		if serverAddr.Port != 0 && serverAddr.Port != 443 {
			httpsOptions.ServerPort = serverAddr.Port
		}
		if serverURL.Path != "/dns-query" {
			httpsOptions.Path = serverURL.Path
		}
	case "rcode":
		var rcode int
		if serverURL == nil {
			return E.New("invalid server address")
		}
		switch serverURL.Host {
		case "success":
			rcode = dns.RcodeSuccess
		case "format_error":
			rcode = dns.RcodeFormatError
		case "server_failure":
			rcode = dns.RcodeServerFailure
		case "name_error":
			rcode = dns.RcodeNameError
		case "not_implemented":
			rcode = dns.RcodeNotImplemented
		case "refused":
			rcode = dns.RcodeRefused
		default:
			return E.New("unknown rcode: ", serverURL.Host)
		}
		o.Type = C.DNSTypeLegacyRcode
		o.Options = rcode
	case C.DNSTypeDHCP:
		o.Type = C.DNSTypeDHCP
		dhcpOptions := DHCPDNSServerOptions{}
		if serverURL == nil {
			return E.New("invalid server address")
		}
		if serverURL.Host != "" && serverURL.Host != "auto" {
			dhcpOptions.Interface = serverURL.Host
		}
		o.Options = &dhcpOptions
	case C.DNSTypeFakeIP:
		o.Type = C.DNSTypeFakeIP
		fakeipOptions := FakeIPDNSServerOptions{}
		if legacyOptions, loaded := ctx.Value((*LegacyDNSFakeIPOptions)(nil)).(*LegacyDNSFakeIPOptions); loaded {
			fakeipOptions.Inet4Range = legacyOptions.Inet4Range
			fakeipOptions.Inet6Range = legacyOptions.Inet6Range
		}
		o.Options = &fakeipOptions
	case C.DNSTypeSDNS:
		o.Type = C.DNSTypeSDNS
		if serverURL == nil {
			return E.New("invalid server address")
		}
		serverAddr := M.ParseSocksaddr(serverURL.Host)
		if !serverAddr.IsValid() {
			return E.New("invalid server address")
		}
		o.Options = &SDNSDNSServerOptions{
			RemoteDNSServerOptions: remoteOptions,
			Stamp:                  serverAddr.AddrString(),
		}
	default:
		return E.New("unsupported DNS server scheme: ", serverType)
	}
	return nil
}

type DNSServerAddressOptions struct {
	Server     string `json:"server"`
	ServerPort uint16 `json:"server_port,omitempty"`
}

func (o DNSServerAddressOptions) Build() M.Socksaddr {
	return M.ParseSocksaddrHostPort(o.Server, o.ServerPort)
}

func (o DNSServerAddressOptions) ServerIsDomain() bool {
	return o.Build().IsDomain()
}

func (o *DNSServerAddressOptions) TakeServerOptions() ServerOptions {
	return ServerOptions(*o)
}

func (o *DNSServerAddressOptions) ReplaceServerOptions(options ServerOptions) {
	*o = DNSServerAddressOptions(options)
}

type HostsDNSServerOptions struct {
	Path       badoption.Listable[string]                                `json:"path,omitempty"`
	Predefined *badjson.TypedMap[string, badoption.Listable[netip.Addr]] `json:"predefined,omitempty"`
}

type RawLocalDNSServerOptions struct {
	DialerOptions
}

type LocalDNSServerOptions struct {
	RawLocalDNSServerOptions
	PreferGo bool `json:"prefer_go,omitempty"`
}

type RemoteDNSServerOptions struct {
	RawLocalDNSServerOptions
	DNSServerAddressOptions
}

type RemoteTLSDNSServerOptions struct {
	RemoteDNSServerOptions
	OutboundTLSOptionsContainer
}

type RemoteHTTPSDNSServerOptions struct {
	RemoteTLSDNSServerOptions
	Path    string               `json:"path,omitempty"`
	Method  string               `json:"method,omitempty"`
	Headers badoption.HTTPHeader `json:"headers,omitempty"`
}

type FakeIPDNSServerOptions struct {
	Inet4Range *badoption.Prefix `json:"inet4_range,omitempty"`
	Inet6Range *badoption.Prefix `json:"inet6_range,omitempty"`
}

type DHCPDNSServerOptions struct {
	LocalDNSServerOptions
	Interface string `json:"interface,omitempty"`
}

type SDNSDNSServerOptions struct {
	RemoteDNSServerOptions
	Stamp string `json:"stamp"`
}

type MultiDNSServerOptions struct {
	RawLocalDNSServerOptions
	Servers      []string           `json:"servers,omitempty"`
	Parallel     bool               `json:"parallel,omitempty"`
	IgnoreRanges []badoption.Prefix `json:"ignore_ranges,omitempty"`
}
