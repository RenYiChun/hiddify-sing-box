package box

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"time"

	"github.com/sagernet/sing-box/adapter"
	boxCertificate "github.com/sagernet/sing-box/adapter/certificate"
	"github.com/sagernet/sing-box/adapter/endpoint"
	"github.com/sagernet/sing-box/adapter/inbound"
	"github.com/sagernet/sing-box/adapter/outbound"
	boxService "github.com/sagernet/sing-box/adapter/service"
	"github.com/sagernet/sing-box/common/certificate"
	"github.com/sagernet/sing-box/common/dialer"
	"github.com/sagernet/sing-box/common/httpclient"
	"github.com/sagernet/sing-box/common/monitoring"
	"github.com/sagernet/sing-box/common/taskmonitor"
	"github.com/sagernet/sing-box/common/tls"
	"github.com/sagernet/sing-box/common/urltest"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/dns"
	"github.com/sagernet/sing-box/experimental"
	"github.com/sagernet/sing-box/experimental/cachefile"
	"github.com/sagernet/sing-box/experimental/deprecated"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/direct"
	"github.com/sagernet/sing-box/protocol/hiddify/hinvalid"
	"github.com/sagernet/sing-box/route"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	F "github.com/sagernet/sing/common/format"
	"github.com/sagernet/sing/common/ntp"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/sing/service/pause"
)

var _ adapter.SimpleLifecycle = (*Box)(nil)

type Box struct {
	createdAt           time.Time
	logFactory          log.Factory
	logger              log.ContextLogger
	network             *route.NetworkManager
	endpoint            *endpoint.Manager
	inbound             *inbound.Manager
	outbound            *outbound.Manager
	service             *boxService.Manager
	certificateProvider *boxCertificate.Manager
	dnsTransport        *dns.TransportManager
	dnsRouter           *dns.Router
	connection          *route.ConnectionManager
	router              *route.Router
	httpClientService   adapter.LifecycleService
	internalService     []adapter.LifecycleService
	done                chan struct{}
}

type Options struct {
	option.Options
	Context           context.Context
	PlatformLogWriter log.PlatformWriter
}

func Context(
	ctx context.Context,
	inboundRegistry adapter.InboundRegistry,
	outboundRegistry adapter.OutboundRegistry,
	endpointRegistry adapter.EndpointRegistry,
	dnsTransportRegistry adapter.DNSTransportRegistry,
	serviceRegistry adapter.ServiceRegistry,
	certificateProviderRegistry adapter.CertificateProviderRegistry,
) context.Context {
	if service.FromContext[option.InboundOptionsRegistry](ctx) == nil ||
		service.FromContext[adapter.InboundRegistry](ctx) == nil {
		ctx = service.ContextWith[option.InboundOptionsRegistry](ctx, inboundRegistry)
		ctx = service.ContextWith[adapter.InboundRegistry](ctx, inboundRegistry)
	}
	if service.FromContext[option.OutboundOptionsRegistry](ctx) == nil ||
		service.FromContext[adapter.OutboundRegistry](ctx) == nil {
		ctx = service.ContextWith[option.OutboundOptionsRegistry](ctx, outboundRegistry)
		ctx = service.ContextWith[adapter.OutboundRegistry](ctx, outboundRegistry)
	}
	if service.FromContext[option.EndpointOptionsRegistry](ctx) == nil ||
		service.FromContext[adapter.EndpointRegistry](ctx) == nil {
		ctx = service.ContextWith[option.EndpointOptionsRegistry](ctx, endpointRegistry)
		ctx = service.ContextWith[adapter.EndpointRegistry](ctx, endpointRegistry)
	}
	if service.FromContext[adapter.DNSTransportRegistry](ctx) == nil {
		ctx = service.ContextWith[option.DNSTransportOptionsRegistry](ctx, dnsTransportRegistry)
		ctx = service.ContextWith[adapter.DNSTransportRegistry](ctx, dnsTransportRegistry)
	}
	if service.FromContext[adapter.ServiceRegistry](ctx) == nil {
		ctx = service.ContextWith[option.ServiceOptionsRegistry](ctx, serviceRegistry)
		ctx = service.ContextWith[adapter.ServiceRegistry](ctx, serviceRegistry)
	}
	if service.FromContext[adapter.CertificateProviderRegistry](ctx) == nil {
		ctx = service.ContextWith[option.CertificateProviderOptionsRegistry](ctx, certificateProviderRegistry)
		ctx = service.ContextWith[adapter.CertificateProviderRegistry](ctx, certificateProviderRegistry)
	}
	return ctx
}

func New(options Options) (*Box, error) {
	createdAt := time.Now()
	ctx := options.Context
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = service.ContextWithDefaultRegistry(ctx)

	endpointRegistry := service.FromContext[adapter.EndpointRegistry](ctx)
	inboundRegistry := service.FromContext[adapter.InboundRegistry](ctx)
	outboundRegistry := service.FromContext[adapter.OutboundRegistry](ctx)
	dnsTransportRegistry := service.FromContext[adapter.DNSTransportRegistry](ctx)
	serviceRegistry := service.FromContext[adapter.ServiceRegistry](ctx)
	certificateProviderRegistry := service.FromContext[adapter.CertificateProviderRegistry](ctx)

	if endpointRegistry == nil {
		return nil, E.New("missing endpoint registry in context")
	}
	if inboundRegistry == nil {
		return nil, E.New("missing inbound registry in context")
	}
	if outboundRegistry == nil {
		return nil, E.New("missing outbound registry in context")
	}
	if dnsTransportRegistry == nil {
		return nil, E.New("missing DNS transport registry in context")
	}
	if serviceRegistry == nil {
		return nil, E.New("missing service registry in context")
	}
	if certificateProviderRegistry == nil {
		return nil, E.New("missing certificate provider registry in context")
	}

	ctx = pause.WithDefaultManager(ctx)
	experimentalOptions := common.PtrValueOrDefault(options.Experimental)
	err := applyDebugOptions(common.PtrValueOrDefault(experimentalOptions.Debug))
	if err != nil {
		return nil, err
	}
	var needCacheFile bool
	var needClashAPI bool
	var needV2RayAPI bool
	if experimentalOptions.CacheFile != nil && experimentalOptions.CacheFile.Enabled || options.PlatformLogWriter != nil {
		needCacheFile = true
	}
	if experimentalOptions.ClashAPI != nil || options.PlatformLogWriter != nil {
		needClashAPI = true
	}
	if experimentalOptions.V2RayAPI != nil && experimentalOptions.V2RayAPI.Listen != "" {
		needV2RayAPI = true
	}
	if experimentalOptions.UnifiedDelay != nil && experimentalOptions.UnifiedDelay.Enabled {
		ctx = urltest.ContextWithIsUnifiedDelay(ctx)
	}
	platformInterface := service.FromContext[adapter.PlatformInterface](ctx)
	var defaultLogWriter io.Writer
	if platformInterface != nil {
		defaultLogWriter = io.Discard
	}
	logFactory, err := log.New(log.Options{
		Context:        ctx,
		Options:        common.PtrValueOrDefault(options.Log),
		Observable:     needClashAPI,
		DefaultWriter:  defaultLogWriter,
		BaseTime:       createdAt,
		PlatformWriter: options.PlatformLogWriter,
	})
	if err != nil {
		return nil, E.Cause(err, "create log factory")
	}

	var internalServices []adapter.LifecycleService
	routeOptions := common.PtrValueOrDefault(options.Route)
	certificateOptions := common.PtrValueOrDefault(options.Certificate)
	if C.IsAndroid || certificateOptions.Store != "" && certificateOptions.Store != C.CertificateStoreSystem ||
		len(certificateOptions.Certificate) > 0 ||
		len(certificateOptions.CertificatePath) > 0 ||
		len(certificateOptions.CertificateDirectoryPath) > 0 {
		certificateStore, err := certificate.NewStore(ctx, logFactory.NewLogger("certificate"), certificateOptions)
		if err != nil {
			return nil, err
		}
		service.MustRegister[adapter.CertificateStore](ctx, certificateStore)
		internalServices = append(internalServices, certificateStore)
	}
	dnsOptions := common.PtrValueOrDefault(options.DNS)
	endpointManager := endpoint.NewManager(logFactory.NewLogger("endpoint"), endpointRegistry)
	inboundManager := inbound.NewManager(logFactory.NewLogger("inbound"), inboundRegistry, endpointManager)
	outboundManager := outbound.NewManager(logFactory.NewLogger("outbound"), outboundRegistry, endpointManager, routeOptions.Final)
	dnsTransportManager := dns.NewTransportManager(logFactory.NewLogger("dns/transport"), dnsTransportRegistry, outboundManager, dnsOptions.Final)
	serviceManager := boxService.NewManager(logFactory.NewLogger("service"), serviceRegistry)
	certificateProviderManager := boxCertificate.NewManager(logFactory.NewLogger("certificate-provider"), certificateProviderRegistry)
	service.MustRegister[adapter.EndpointManager](ctx, endpointManager)
	service.MustRegister[adapter.InboundManager](ctx, inboundManager)
	service.MustRegister[adapter.OutboundManager](ctx, outboundManager)
	service.MustRegister[adapter.DNSTransportManager](ctx, dnsTransportManager)
	service.MustRegister[adapter.ServiceManager](ctx, serviceManager)
	service.MustRegister[adapter.CertificateProviderManager](ctx, certificateProviderManager)
	dnsRouter, err := dns.NewRouter(ctx, logFactory, dnsOptions)
	if err != nil {
		return nil, E.Cause(err, "initialize DNS router")
	}
	service.MustRegister[adapter.DNSRouter](ctx, dnsRouter)
	service.MustRegister[adapter.DNSRuleSetUpdateValidator](ctx, dnsRouter)
	networkManager, err := route.NewNetworkManager(ctx, logFactory.NewLogger("network"), routeOptions, dnsOptions)
	if err != nil {
		return nil, E.Cause(err, "initialize network manager")
	}
	service.MustRegister[adapter.NetworkManager](ctx, networkManager)
	connectionManager := route.NewConnectionManager(logFactory.NewLogger("connection"))
	service.MustRegister[adapter.ConnectionManager](ctx, connectionManager)
	// Must register after ConnectionManager: the Apple HTTP engine's proxy bridge reads it from the context when Manager.Start resolves the default client.
	httpClientManager := httpclient.NewManager(ctx, logFactory.NewLogger("httpclient"), options.HTTPClients, routeOptions.DefaultHTTPClient)
	service.MustRegister[adapter.HTTPClientManager](ctx, httpClientManager)
	httpClientService := adapter.LifecycleService(httpClientManager)
	router := route.NewRouter(ctx, logFactory, routeOptions, dnsOptions)
	service.MustRegister[adapter.Router](ctx, router)
	err = router.Initialize(routeOptions.Rules, routeOptions.RuleSet)
	if err != nil {
		return nil, E.Cause(err, "initialize router")
	}
	ntpOptions := common.PtrValueOrDefault(options.NTP)
	var timeService *tls.TimeServiceWrapper
	if ntpOptions.Enabled {
		timeService = new(tls.TimeServiceWrapper)
		service.MustRegister[ntp.TimeService](ctx, timeService)
	}
	for i, transportOptions := range dnsOptions.Servers {
		var tag string
		if transportOptions.Tag != "" {
			tag = transportOptions.Tag
		} else {
			tag = F.ToString(i)
		}
		err = dnsTransportManager.Create(
			ctx,
			logFactory.NewLogger(F.ToString("dns/", transportOptions.Type, "[", tag, "]")),
			tag,
			transportOptions.Type,
			transportOptions.Options,
		)
		if err != nil {
			return nil, E.Cause(err, "initialize DNS server[", i, "]")
		}
	}
	err = dnsRouter.Initialize(dnsOptions.Rules)
	if err != nil {
		return nil, E.Cause(err, "initialize dns router")
	}
	for i, endpointOptions := range options.Endpoints {
		var tag string
		if endpointOptions.Tag != "" {
			tag = endpointOptions.Tag
		} else {
			tag = F.ToString(i)
		}
		endpointCtx := ctx
		if tag != "" {
			// TODO: remove this
			endpointCtx = adapter.WithContext(endpointCtx, &adapter.InboundContext{
				Outbound: tag,
			})
		}
		err = endpointManager.Create(
			endpointCtx,
			router,
			logFactory.NewLogger(F.ToString("endpoint/", endpointOptions.Type, "[", tag, "]")),
			tag,
			endpointOptions.Type,
			endpointOptions.Options,
		)
		if err != nil {
			return nil, E.Cause(err, "initialize endpoint[", i, "]")
		}
	}
	for i, inboundOptions := range options.Inbounds {
		var tag string
		if inboundOptions.Tag != "" {
			tag = inboundOptions.Tag
		} else {
			tag = F.ToString(i)
		}
		err = inboundManager.Create(
			ctx,
			router,
			logFactory.NewLogger(F.ToString("inbound/", inboundOptions.Type, "[", tag, "]")),
			tag,
			inboundOptions.Type,
			inboundOptions.Options,
		)
		if err != nil {
			return nil, E.Cause(err, "initialize inbound[", i, "]")
		}
	}
	for i, serviceOptions := range options.Services {
		var tag string
		if serviceOptions.Tag != "" {
			tag = serviceOptions.Tag
		} else {
			tag = F.ToString(i)
		}
		err = serviceManager.Create(
			ctx,
			logFactory.NewLogger(F.ToString("service/", serviceOptions.Type, "[", tag, "]")),
			tag,
			serviceOptions.Type,
			serviceOptions.Options,
		)
		if err != nil {
			return nil, E.Cause(err, "initialize service[", i, "]")
		}
	}
	for i, outboundOptions := range options.Outbounds {
		var tag string
		if outboundOptions.Tag != "" {
			tag = outboundOptions.Tag
		} else {
			tag = F.ToString(i)
		}
		outboundCtx := ctx
		if tag != "" {
			// TODO: remove this
			outboundCtx = adapter.WithContext(outboundCtx, &adapter.InboundContext{
				Outbound: tag,
			})
		}
		err = outboundManager.Create(
			outboundCtx,
			router,
			logFactory.NewLogger(F.ToString("outbound/", outboundOptions.Type, "[", tag, "]")),
			tag,
			outboundOptions.Type,
			outboundOptions.Options,
		)
		if err != nil {
			return nil, E.Cause(err, "initialize outbound[", i, "]")
		}
	}
	var invalidOutbound *hinvalid.Outbound
	for _, outbound := range outboundManager.Outbounds() {
		if outbound.Type() == C.TypeURLTest || outbound.Type() == C.TypeSelector || outbound.Type() == C.TypeDirect {
			continue
		}
		if outbound.Type() == C.TypeHInvalidConfig {
			invalidOutbound = outbound.(*hinvalid.Outbound)
			continue
		}
		invalidOutbound = nil
		break
	}
	if invalidOutbound != nil && invalidOutbound.InvalidOptions.Err != nil {
		return nil, E.Cause(invalidOutbound.InvalidOptions.Err)
	}

	for i, certificateProviderOptions := range options.CertificateProviders {
		var tag string
		if certificateProviderOptions.Tag != "" {
			tag = certificateProviderOptions.Tag
		} else {
			tag = F.ToString(i)
		}
		err = certificateProviderManager.Create(
			ctx,
			logFactory.NewLogger(F.ToString("certificate-provider/", certificateProviderOptions.Type, "[", tag, "]")),
			tag,
			certificateProviderOptions.Type,
			certificateProviderOptions.Options,
		)
		if err != nil {
			return nil, E.Cause(err, "initialize certificate provider[", i, "]")
		}
	}
	outboundManager.Initialize(func() (adapter.Outbound, error) {
		return direct.NewOutbound(
			ctx,
			router,
			logFactory.NewLogger("outbound/direct"),
			"direct",
			option.DirectOutboundOptions{},
		)
	})
	dnsTransportManager.Initialize(func() (adapter.DNSTransport, error) {
		return dnsTransportRegistry.CreateDNSTransport(
			ctx,
			logFactory.NewLogger("dns/local"),
			"local",
			C.DNSTypeLocal,
			&option.LocalDNSServerOptions{},
		)
	})
	httpClientManager.Initialize(func() (*httpclient.ManagedTransport, error) {
		deprecated.Report(ctx, deprecated.OptionImplicitDefaultHTTPClient)
		var httpClientOptions option.HTTPClientOptions
		httpClientOptions.DefaultOutbound = true
		return httpclient.NewTransport(ctx, logFactory.NewLogger("httpclient"), "", httpClientOptions)
	})
	if platformInterface != nil {
		err = platformInterface.Initialize(networkManager)
		if err != nil {
			return nil, E.Cause(err, "initialize platform interface")
		}
	}
	if needCacheFile {
		cacheFile := cachefile.New(ctx, logFactory.NewLogger("cache-file"), common.PtrValueOrDefault(experimentalOptions.CacheFile))
		service.MustRegister[adapter.CacheFile](ctx, cacheFile)
		internalServices = append(internalServices, cacheFile)
	}
	if needClashAPI {
		clashAPIOptions := common.PtrValueOrDefault(experimentalOptions.ClashAPI)
		clashAPIOptions.ModeList = experimental.CalculateClashModeList(options.Options)
		clashServer, err := experimental.NewClashServer(ctx, logFactory.(log.ObservableFactory), clashAPIOptions)
		if err != nil {
			return nil, E.Cause(err, "create clash-server")
		}
		router.AppendTracker(clashServer)
		service.MustRegister[adapter.ClashServer](ctx, clashServer)
		internalServices = append(internalServices, clashServer)
	}
	if needV2RayAPI {
		v2rayServer, err := experimental.NewV2RayServer(logFactory.NewLogger("v2ray-api"), common.PtrValueOrDefault(experimentalOptions.V2RayAPI))
		if err != nil {
			return nil, E.Cause(err, "create v2ray-server")
		}
		if v2rayServer.StatsService() != nil {
			router.AppendTracker(v2rayServer.StatsService())
			internalServices = append(internalServices, v2rayServer)
			service.MustRegister[adapter.V2RayServer](ctx, v2rayServer)
		}
	}
	monitor, err := monitoring.NewOutboundMonitoring(ctx, logFactory.NewLogger("monitoring"), common.PtrValueOrDefault(experimentalOptions.Monitoring))
	if err != nil {
		return nil, E.Cause(err, "create outbound monitoring")
	}
	internalServices = append(internalServices, monitor)
	service.MustRegisterPtr[monitoring.OutboundMonitoring](ctx, monitor)

	router.AppendTracker(monitor)

	if ntpOptions.Enabled {
		ntpDialer, err := dialer.New(ctx, ntpOptions.DialerOptions, ntpOptions.ServerIsDomain())
		if err != nil {
			return nil, E.Cause(err, "create NTP service")
		}
		ntpService := ntp.NewService(ntp.Options{
			Context:       ctx,
			Dialer:        ntpDialer,
			Logger:        logFactory.NewLogger("ntp"),
			Server:        ntpOptions.ServerOptions.Build(),
			Interval:      time.Duration(ntpOptions.Interval),
			WriteToSystem: ntpOptions.WriteToSystem,
		})
		timeService.TimeService = ntpService
		internalServices = append(internalServices, adapter.NewLifecycleService(ntpService, "ntp service"))
	}
	return &Box{
		network:             networkManager,
		endpoint:            endpointManager,
		inbound:             inboundManager,
		outbound:            outboundManager,
		dnsTransport:        dnsTransportManager,
		service:             serviceManager,
		certificateProvider: certificateProviderManager,
		dnsRouter:           dnsRouter,
		connection:          connectionManager,
		router:              router,
		httpClientService:   httpClientService,
		createdAt:           createdAt,
		logFactory:          logFactory,
		logger:              logFactory.Logger(),
		internalService:     internalServices,
		done:                make(chan struct{}),
	}, nil
}

func (s *Box) PreStart() error {
	err := s.preStart()
	if err != nil {
		// TODO: remove catch error
		defer func() {
			v := recover()
			if v != nil {
				println(err.Error())
				debug.PrintStack()
				panic("panic on early close: " + fmt.Sprint(v))
			}
		}()
		s.Close()
		return err
	}
	s.logger.Info("sing-box pre-started (", F.Seconds(time.Since(s.createdAt).Seconds()), "s)")
	return nil
}

func (s *Box) Start() error {
	err := s.start()
	if err != nil {
		// TODO: remove catch error
		defer func() {
			v := recover()
			if v != nil {
				println(err.Error())
				debug.PrintStack()
				println("panic on early start: " + fmt.Sprint(v))
			}
		}()
		s.Close()
		return err
	}
	s.logger.Info("sing-box started (", F.Seconds(time.Since(s.createdAt).Seconds()), "s)")
	return nil
}

func (s *Box) preStart() error {
	monitor := taskmonitor.New(s.logger, C.StartTimeout)
	if err := s.runStartStep("preStart logger", func() error {
		monitor.Start("start logger")
		err := s.logFactory.Start()
		monitor.Finish()
		return err
	}); err != nil {
		return E.Cause(err, "start logger")
	}
	if err := s.runStartStep("initialize internal services", func() error {
		return adapter.StartNamed(s.logger, adapter.StartStateInitialize, s.internalService)
	}); err != nil {
		return err
	}
	for _, step := range []struct {
		name    string
		service adapter.Lifecycle
	}{
		{"initialize network", s.network},
		{"initialize dns transport", s.dnsTransport},
		{"initialize dns router", s.dnsRouter},
		{"initialize connection", s.connection},
		{"initialize router", s.router},
		{"initialize outbound", s.outbound},
		{"initialize inbound", s.inbound},
		{"initialize endpoint", s.endpoint},
		{"initialize service", s.service},
		{"initialize certificate-provider", s.certificateProvider},
	} {
		if err := s.runStartStep(step.name, func() error {
			return adapter.Start(s.logger, adapter.StartStateInitialize, step.service)
		}); err != nil {
			return err
		}
	}
	for _, step := range []struct {
		name    string
		service adapter.Lifecycle
	}{
		{"start outbound", s.outbound},
		{"start dns transport", s.dnsTransport},
		{"start dns router", s.dnsRouter},
		{"start network", s.network},
		{"start connection", s.connection},
		{"start router", s.router},
	} {
		if err := s.runStartStep(step.name, func() error {
			return adapter.Start(s.logger, adapter.StartStateStart, step.service)
		}); err != nil {
			return err
		}
	}
	if s.httpClientService != nil {
		if err := s.runStartStep("start http client", func() error {
			return adapter.StartNamed(s.logger, adapter.StartStateStart, []adapter.LifecycleService{s.httpClientService})
		}); err != nil {
			return err
		}
	}
	for _, step := range []struct {
		name    string
		service adapter.Lifecycle
	}{
		{"start router", s.router},
		{"start dns router", s.dnsRouter},
	} {
		if err := s.runStartStep(step.name, func() error {
			return adapter.Start(s.logger, adapter.StartStateStart, step.service)
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Box) start() error {
	if err := s.runStartStep("preStart", s.preStart); err != nil {
		return err
	}
	if err := s.runStartStep("start internal services", func() error {
		return adapter.StartNamed(s.logger, adapter.StartStateStart, s.internalService)
	}); err != nil {
		return err
	}
	for _, step := range []struct {
		name    string
		service adapter.Lifecycle
	}{
		{"start endpoint", s.endpoint},
		{"start certificate-provider", s.certificateProvider},
		{"start inbound", s.inbound},
		{"start service", s.service},
	} {
		if err := s.runStartStep(step.name, func() error {
			return adapter.Start(s.logger, adapter.StartStateStart, step.service)
		}); err != nil {
			return err
		}
	}
	for _, step := range []struct {
		name    string
		service adapter.Lifecycle
	}{
		{"post-start outbound", s.outbound},
		{"post-start network", s.network},
		{"post-start dns transport", s.dnsTransport},
		{"post-start dns router", s.dnsRouter},
		{"post-start connection", s.connection},
		{"post-start router", s.router},
		{"post-start endpoint", s.endpoint},
		{"post-start certificate-provider", s.certificateProvider},
		{"post-start inbound", s.inbound},
		{"post-start service", s.service},
	} {
		if err := s.runStartStep(step.name, func() error {
			return adapter.Start(s.logger, adapter.StartStatePostStart, step.service)
		}); err != nil {
			return err
		}
	}
	if err := s.runStartStep("post-start internal services", func() error {
		return adapter.StartNamed(s.logger, adapter.StartStatePostStart, s.internalService)
	}); err != nil {
		return err
	}
	for _, step := range []struct {
		name    string
		service adapter.Lifecycle
	}{
		{"finish-start network", s.network},
		{"finish-start dns transport", s.dnsTransport},
		{"finish-start dns router", s.dnsRouter},
		{"finish-start connection", s.connection},
		{"finish-start router", s.router},
		{"finish-start outbound", s.outbound},
		{"finish-start endpoint", s.endpoint},
		{"finish-start certificate-provider", s.certificateProvider},
		{"finish-start inbound", s.inbound},
		{"finish-start service", s.service},
	} {
		if err := s.runStartStep(step.name, func() error {
			return adapter.Start(s.logger, adapter.StartStateStarted, step.service)
		}); err != nil {
			return err
		}
	}
	if err := s.runStartStep("finish-start internal services", func() error {
		return adapter.StartNamed(s.logger, adapter.StartStateStarted, s.internalService)
	}); err != nil {
		return err
	}
	return nil
}

func (s *Box) runStartStep(name string, fn func() error) error {
	startedAt := time.Now()
	println("H CORE TIMING Box", name, "begin")
	err := fn()
	if err != nil {
		println("H CORE TIMING Box", name, "failed after", time.Since(startedAt).String(), err.Error())
		return err
	}
	println("H CORE TIMING Box", name, "took", time.Since(startedAt).String())
	return nil
}

func (s *Box) Close() error {
	select {
	case <-s.done:
		return os.ErrClosed
	default:
		close(s.done)
	}
	closeTimeout := time.Second * 10
	var err error
	closedInternalServices := make(map[int]bool)
	for i, lifecycleService := range s.internalService {
		if lifecycleService.Name() != "outbound-monitoring" {
			continue
		}
		cerr := s.closeWithTimeout(lifecycleService.Name(), closeTimeout, lifecycleService.Close)
		err = E.Append(err, cerr, func(err error) error {
			return E.Cause(err, "close ", lifecycleService.Name())
		})
		closedInternalServices[i] = true
	}
	for _, closeItem := range []struct {
		name    string
		service adapter.Lifecycle
	}{
		{"service", s.service},
		{"inbound", s.inbound},
		{"certificate-provider", s.certificateProvider},
		{"endpoint", s.endpoint},
		{"outbound", s.outbound},
		{"router", s.router},
		{"connection", s.connection},
		{"dns-router", s.dnsRouter},
		{"dns-transport", s.dnsTransport},
		{"network", s.network},
	} {
		cerr := s.closeWithTimeout(closeItem.name, closeTimeout, closeItem.service.Close)
		err = E.Append(err, cerr, func(err error) error {
			return E.Cause(err, "close ", closeItem.name)
		})
	}
	if s.httpClientService != nil {
		cerr := s.closeWithTimeout(s.httpClientService.Name(), closeTimeout, s.httpClientService.Close)
		err = E.Append(err, cerr, func(err error) error {
			return E.Cause(err, "close ", s.httpClientService.Name())
		})
	}
	for i, lifecycleService := range s.internalService {
		if closedInternalServices[i] {
			continue
		}
		cerr := s.closeWithTimeout(lifecycleService.Name(), closeTimeout, lifecycleService.Close)
		err = E.Append(err, cerr, func(err error) error {
			return E.Cause(err, "close ", lifecycleService.Name())
		})
	}
	cerr := s.closeWithTimeout("logger", closeTimeout, s.logFactory.Close)
	err = E.Append(err, cerr, func(err error) error {
		return E.Cause(err, "close logger")
	})
	return err
}
func (s *Box) closeWithTimeout(name string, timeout time.Duration, closeFn func() error) (err error) {
	s.logger.Trace("closeing ", name)
	startTime := time.Now()
	defer func() {
		if err != nil {
			s.logger.Error("close ", name, " error (", F.Seconds(time.Since(startTime).Seconds()), "s)"+": "+err.Error())
		} else {
			s.logger.Trace("close ", name, " completed (", F.Seconds(time.Since(startTime).Seconds()), "s)")
		}
	}()
	done := make(chan error, 1)

	go func() {
		done <- closeFn()
	}()

	select {
	case err = <-done:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("close %s timed out after %s", name, timeout)
	}

}
func (s *Box) Network() adapter.NetworkManager {
	return s.network
}

func (s *Box) Router() adapter.Router {
	return s.router
}

func (s *Box) Inbound() adapter.InboundManager {
	return s.inbound
}

func (s *Box) Outbound() adapter.OutboundManager {
	return s.outbound
}
func (s *Box) Endpoint() adapter.EndpointManager {
	return s.endpoint
}

func (s *Box) LogFactory() log.Factory {
	return s.logFactory
}

func (s *Box) AddService(service adapter.LifecycleService) {
	s.internalService = append(s.internalService, service)
}

func (s *Box) Logger() log.ContextLogger {
	return s.logger
}
