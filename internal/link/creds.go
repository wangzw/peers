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

// authType names the credentials in gRPC peer information. It is not the TLS
// security protocol, which Info reports separately.
const authType = "peers-tls"

// identityFromState extracts the authenticated peer identity from a finished
// TLS handshake. Only TLS 1.3 is accepted and the peer must have presented an
// Ed25519 certificate. Identity comes from the certificate's
// SubjectPublicKeyInfo, never from its chain.
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

// serverAuthInfo carries the identity of the dialing peer and, when the inbound
// connection was tracked, the wrapper that enforces the first-frame rules.
type serverAuthInfo struct {
	credentials.CommonAuthInfo
	id      peerIdentity
	tracked *trackedConn
}

func (serverAuthInfo) AuthType() string { return authType }

// clientAuthInfo carries the identity of the accepting peer on a dialed
// connection.
type clientAuthInfo struct {
	credentials.CommonAuthInfo
	id peerIdentity
}

func (clientAuthInfo) AuthType() string { return authType }

// serverCreds performs the server side of TLS. The dialing side is
// untrusted until the certificate is checked, so the handshake runs here
// rather than in a gRPC interceptor.
type serverCreds struct {
	cfg func() *tls.Config
	// known reports whether a fingerprint is in the peer table. It is called
	// after the TLS handshake, before the registry lock is taken, so a paired
	// peer's connection can be moved out of the stranger-source pool.
	known func(fp pairing.Fingerprint) bool
}

// ServerHandshake wraps raw in a TLS server, completes the handshake and reads
// the client identity from it. The raw connection is a *trackedConn when it
// came through a trackingListener; that wrapper is passed along so the stream
// can claim it once.
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
	// TLS 1.3 and CertificateVerify proved the peer holds this fingerprint's
	// private key. A fingerprint already in the peer table leaves the
	// stranger-source pool now, so a flood from other sources cannot evict it
	// before its Hello is checked. The role query runs outside the registry
	// lock; promoteKnown takes the lock without calling Peers.
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

// clientCreds hands an already authenticated TLS connection to gRPC. The
// dialer performs both the TCP and the TLS handshake before gRPC sees the
// connection, so nothing is sent to a peer whose fingerprint failed the check.
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

// clonePeerIdentity copies the public key so callers cannot mutate the key
// slices shared by credential copies.
func clonePeerIdentity(id peerIdentity) peerIdentity {
	id.pub = append([]byte(nil), id.pub...)
	return id
}

// connectionRemote returns the peer address of c, or the empty string when the
// connection or its address is unavailable.
func connectionRemote(c net.Conn) string {
	if c == nil || c.RemoteAddr() == nil {
		return ""
	}
	return c.RemoteAddr().String()
}
