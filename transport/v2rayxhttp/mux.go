package xhttp

import (
	"context"
	"crypto/rand"
	"math"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/option"
	E "github.com/sagernet/sing/common/exceptions"
)

type XmuxConn interface {
	IsClosed() bool
}

type closeableXmuxConn interface {
	Close() error
}

type XmuxClient struct {
	XmuxConn     XmuxConn
	OpenUsage    atomic.Int32
	leftUsage    int32
	LeftRequests atomic.Int32
	UnreusableAt time.Time
}

type XmuxManager struct {
	options     option.V2RayXHTTPXmuxOptions
	concurrency int32
	connections int32
	newConnFunc func() XmuxConn
	xmuxClients []*XmuxClient
	mtx         sync.Mutex
}

func NewXmuxManager(options option.V2RayXHTTPXmuxOptions, newConnFunc func() XmuxConn) *XmuxManager {
	return &XmuxManager{
		options:     options,
		concurrency: options.GetNormalizedMaxConcurrency().Rand(),
		connections: options.GetNormalizedMaxConnections().Rand(),
		newConnFunc: newConnFunc,
		xmuxClients: make([]*XmuxClient, 0),
	}
}

func (m *XmuxManager) newXmuxClient() *XmuxClient {
	xmuxClient := &XmuxClient{
		XmuxConn:  m.newConnFunc(),
		leftUsage: -1,
	}
	if x := m.options.GetNormalizedCMaxReuseTimes().Rand(); x > 0 {
		xmuxClient.leftUsage = x - 1
	}
	xmuxClient.LeftRequests.Store(math.MaxInt32)
	if x := m.options.GetNormalizedHMaxRequestTimes().Rand(); x > 0 {
		xmuxClient.LeftRequests.Store(x)
	}
	if x := m.options.GetNormalizedHMaxReusableSecs().Rand(); x > 0 {
		xmuxClient.UnreusableAt = time.Now().Add(time.Duration(x) * time.Second)
	}
	m.xmuxClients = append(m.xmuxClients, xmuxClient)
	return xmuxClient
}

func (m *XmuxManager) GetXmuxClient(ctx context.Context) *XmuxClient {
	m.mtx.Lock()
	now := time.Now()
	retiredConnections := m.removeRetiredClientsLocked(now)
	reusableClients := make([]*XmuxClient, 0, len(m.xmuxClients))
	for _, xmuxClient := range m.xmuxClients {
		if xmuxClient.isReusable(now) {
			reusableClients = append(reusableClients, xmuxClient)
		}
	}

	var selectedClient *XmuxClient
	if len(reusableClients) == 0 {
		selectedClient = m.newXmuxClient()
	} else if m.connections > 0 && len(reusableClients) < int(m.connections) {
		selectedClient = m.newXmuxClient()
	} else {
		xmuxClients := reusableClients
		if m.concurrency > 0 {
			xmuxClients = make([]*XmuxClient, 0, len(reusableClients))
			for _, xmuxClient := range reusableClients {
				if xmuxClient.OpenUsage.Load() < m.concurrency {
					xmuxClients = append(xmuxClients, xmuxClient)
				}
			}
		}
		if len(xmuxClients) == 0 {
			selectedClient = m.newXmuxClient()
		} else {
			i, _ := rand.Int(rand.Reader, big.NewInt(int64(len(xmuxClients))))
			selectedClient = xmuxClients[i.Int64()]
			if selectedClient.leftUsage > 0 {
				selectedClient.leftUsage -= 1
			}
		}
	}
	m.mtx.Unlock()

	closeXmuxConnections(retiredConnections)
	return selectedClient
}

func (c *XmuxClient) isReusable(now time.Time) bool {
	return !c.XmuxConn.IsClosed() &&
		c.leftUsage != 0 &&
		c.LeftRequests.Load() > 0 &&
		(c.UnreusableAt.IsZero() || !now.After(c.UnreusableAt))
}

func (m *XmuxManager) removeRetiredClientsLocked(now time.Time) []closeableXmuxConn {
	retiredConnections := make([]closeableXmuxConn, 0)
	for i := 0; i < len(m.xmuxClients); {
		xmuxClient := m.xmuxClients[i]
		if !xmuxClient.isReusable(now) && xmuxClient.OpenUsage.Load() == 0 {
			m.xmuxClients = append(m.xmuxClients[:i], m.xmuxClients[i+1:]...)
			if closer, ok := xmuxClient.XmuxConn.(closeableXmuxConn); ok {
				retiredConnections = append(retiredConnections, closer)
			}
		} else {
			i++
		}
	}
	return retiredConnections
}

func closeXmuxConnections(connections []closeableXmuxConn) {
	for _, connection := range connections {
		_ = connection.Close()
	}
}

func (m *XmuxManager) Close() error {
	m.mtx.Lock()
	clients := m.xmuxClients
	m.xmuxClients = nil
	m.mtx.Unlock()

	var err error
	for _, client := range clients {
		if closer, ok := client.XmuxConn.(closeableXmuxConn); ok {
			err = E.Append(err, closer.Close(), nil)
		}
	}
	return err
}
