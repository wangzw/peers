package link

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"errors"
	"fmt"
	"net"

	"github.com/wangzw/peers/internal/pairing"
	"google.golang.org/grpc/credentials"
)

// authType is the peer-info auth type, distinct from the TLS security protocol
// Info reports.
const authType = "peers-tls"

// Only TLS 1.3 and an Ed25519 certificate are accepted; identity comes from
// SubjectPublicKeyInfo, never from the chain.
func identityFromState(st tls.ConnectionState, remote string) (peerIdentity, error) {
	if st.Version != tls.VersionTLS13 {
		return peerIdentity{}, fmt.Errorf("the peer negotiated TLS version %#x, want TLS 1.3", st.Version)
	}
	if len(st.PeerCertificates) == 0 {
		return peerIdentity{}, errors.New("the peer presented no certificate")
	}
	cert := st.PeerCertificates[0]
	pub, ok := cert.PublicKey.(ed25519.PublicKey)
	if !ok {
		return peerIdentity{}, fmt.Errorf("the peer certificate key type %T is not Ed25519", cert.PublicKey)
	}
	if len(pub) != ed25519.PublicKeySize {
		return peerIdentity{}, fmt.Errorf("the peer Ed25519 key has %d bytes, want %d", len(pub), ed25519.PublicKeySize)
	}
	return peerIdentity{
		fp:     pairing.FingerprintOf(cert.RawSubjectPublicKeyInfo),
		pub:    append([]byte(nil), pub...),
		remote: remote,
	}, nil
}

type serverAuthInfo struct {
	credentials.CommonAuthInfo
	id      peerIdentity
	tracked *trackedConn
}

func (serverAuthInfo) AuthType() string { return authType }

type clientAuthInfo struct {
	credentials.CommonAuthInfo
	id peerIdentity
}

func (clientAuthInfo) AuthType() string { return authType }

type serverCreds struct {
	cfg func() *tls.Config
	// known is called after the TLS handshake, before the registry lock is taken.
	known func(fp pairing.Fingerprint) bool
}

func (c serverCreds) ServerHandshake(raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	cfg := c.cfg()
	if cfg == nil {
		return nil, nil, errors.New("no TLS configuration is available")
	}
	tc := tls.Server(raw, cfg)
	if err := tc.HandshakeContext(context.Background()); err != nil {
		_ = tc.Close()
		return nil, nil, err
	}
	tracked, _ := raw.(*trackedConn)
	id, err := identityFromState(tc.ConnectionState(), connectionRemote(raw))
	if err != nil {
		_ = tc.Close()
		return nil, nil, err
	}
	// Promote before reading the first frame so a stranger flood cannot evict
	// the connection, and query the role outside the registry lock.
	if tracked != nil && c.known != nil && c.known(id.fp) {
		tracked.promoteKnown(id.fp)
	}
	return tc, serverAuthInfo{
		CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.PrivacyAndIntegrity},
		id:             id,
		tracked:        tracked,
	}, nil
}

func (serverCreds) ClientHandshake(context.Context, string, net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("server credentials cannot dial")
}

func (serverCreds) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "tls"}
}

func (c serverCreds) Clone() credentials.TransportCredentials { return c }

func (serverCreds) OverrideServerName(string) error { return nil }

type clientCreds struct {
	id peerIdentity
}

func (c clientCreds) ClientHandshake(_ context.Context, _ string, raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return raw, clientAuthInfo{
		CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.PrivacyAndIntegrity},
		id:             clonePeerIdentity(c.id),
	}, nil
}

func (clientCreds) ServerHandshake(net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("client credentials cannot accept")
}

func (clientCreds) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "tls"}
}

func (c clientCreds) Clone() credentials.TransportCredentials {
	return clientCreds{id: clonePeerIdentity(c.id)}
}

func (clientCreds) OverrideServerName(string) error { return nil }

func clonePeerIdentity(id peerIdentity) peerIdentity {
	id.pub = append([]byte(nil), id.pub...)
	return id
}

func connectionRemote(c net.Conn) string {
	if c == nil || c.RemoteAddr() == nil {
		return ""
	}
	return c.RemoteAddr().String()
}
