package termcmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// UnaryTimeout bounds each unary call to the Local service. The Audit stream
// has no overall deadline; it ends when the command is interrupted.
const UnaryTimeout = 10 * time.Second

// updateStatusTimeout bounds one UpdateStatus call. It is shorter than
// UnaryTimeout because the update command polls it while a Broker restarts.
const updateStatusTimeout = 2 * time.Second

// ErrDiagnosticsOnly reports that the Broker runs degraded with diagnostics
// only. Its status has already been written when RunStatus returns it; the
// status command exits with 1 without repeating the reason on standard error.
var ErrDiagnosticsOnly = errors.New("the Broker runs degraded with diagnostics only")

// Dialer opens a client connection to the Broker on the selected socket. The
// production value checks the peer before gRPC uses the connection. It is an
// alias so that a function returning this signature directly satisfies
// Selector.
type Dialer = func() (*grpc.ClientConn, localpb.LocalClient, error)

// Selector returns the socket path of a running Broker together with a function
// that opens a connection to it. It never starts a Broker.
// mcpserver.FindBrokerWithDial satisfies it.
type Selector func(ctx context.Context) (socketPath string, dial Dialer, err error)

// RunStatus connects to the selected Broker, checks that both ends speak the
// same local protocol version and writes the Broker status to w.
func RunStatus(ctx context.Context, sel Selector, w io.Writer) error {
	client, closeConn, err := dial(ctx, sel)
	if err != nil {
		return err
	}
	defer closeConn()

	callCtx, cancel := context.WithTimeout(ctx, UnaryTimeout)
	defer cancel()

	resp, err := client.Status(callCtx, &localpb.StatusRequest{})
	if err != nil {
		return describedError(err)
	}
	if err := WriteStatus(w, resp); err != nil {
		return err
	}
	if resp.GetDegraded().GetLevel() == localpb.DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY {
		return ErrDiagnosticsOnly
	}
	return nil
}

// RunAudit streams the audit log from the selected Broker to w, one line per
// event as it arrives. limit is passed to the server, where zero means no
// limit. Events written before a stream error stay in w.
func RunAudit(ctx context.Context, sel Selector, limit uint32, w io.Writer) error {
	client, closeConn, err := dial(ctx, sel)
	if err != nil {
		return err
	}
	defer closeConn()

	stream, err := client.Audit(ctx, &localpb.AuditRequest{Limit: limit})
	if err != nil {
		return describedError(err)
	}
	for {
		ev, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return describedError(err)
		}
		if err := WriteAuditEvent(w, ev); err != nil {
			return err
		}
	}
}

// RunConfigGet connects to the selected Broker and writes the requested
// configuration entries to w. An empty key asks for every entry.
func RunConfigGet(ctx context.Context, sel Selector, key string, w io.Writer) error {
	client, closeConn, err := dial(ctx, sel)
	if err != nil {
		return err
	}
	defer closeConn()

	callCtx, cancel := context.WithTimeout(ctx, UnaryTimeout)
	defer cancel()

	resp, err := client.GetConfig(callCtx, &localpb.GetConfigRequest{Key: key})
	if err != nil {
		return describedError(err)
	}
	return WriteConfigEntries(w, resp.GetEntries())
}

// RunConfigSet connects to the selected Broker, sets one configuration value
// and writes the old and new values to w.
func RunConfigSet(ctx context.Context, sel Selector, key, value string, w io.Writer) error {
	client, closeConn, err := dial(ctx, sel)
	if err != nil {
		return err
	}
	defer closeConn()

	callCtx, cancel := context.WithTimeout(ctx, UnaryTimeout)
	defer cancel()

	resp, err := client.SetConfig(callCtx, &localpb.SetConfigRequest{Key: key, Value: value})
	if err != nil {
		return describedError(err)
	}
	return WriteConfigChange(w, key, resp.GetOldValue(), resp.GetNewValue())
}

// RunSetOnline connects to the selected Broker, sets broker.offline and
// reports the new state in one line. Broker selection and error reporting are
// the same as for config set.
func RunSetOnline(ctx context.Context, sel Selector, online bool, w io.Writer) error {
	client, closeConn, err := dial(ctx, sel)
	if err != nil {
		return err
	}
	defer closeConn()

	callCtx, cancel := context.WithTimeout(ctx, UnaryTimeout)
	defer cancel()

	value, message := "true", "The Broker is now offline."
	if online {
		value, message = "false", "The Broker is now online."
	}
	if _, err := client.SetConfig(callCtx, &localpb.SetConfigRequest{
		Key:   "broker.offline",
		Value: value,
	}); err != nil {
		return describedError(err)
	}
	_, err = fmt.Fprintln(w, message)
	return err
}

// RunPairingKeyCreate connects to the selected Broker, creates a one-time
// pairing key and writes the key, its expiry and the addresses to dial.
func RunPairingKeyCreate(ctx context.Context, sel Selector, w io.Writer) error {
	client, closeConn, err := dial(ctx, sel)
	if err != nil {
		return err
	}
	defer closeConn()

	callCtx, cancel := context.WithTimeout(ctx, UnaryTimeout)
	defer cancel()

	resp, err := client.CreatePairingKey(callCtx, &localpb.CreatePairingKeyRequest{})
	if err != nil {
		return errors.New(pairingKeyErrorText(err))
	}
	return WritePairingKey(w, resp)
}

// pairingKeyErrorText describes a failed key creation and, when the Broker
// refuses because it does not listen, names the terminal command that pairs
// from this side. The Broker states the refusal without MCP tool names, which
// only the model would understand.
func pairingKeyErrorText(err error) string {
	text := Describe(err)
	if errorCode(err) == localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT {
		text += "; ask the other side to create a pairing key and pair from here with `peers broker connect`"
	}
	return text
}

// RunPeerList connects to the selected Broker and writes every Broker it knows
// as a table sorted by alias.
func RunPeerList(ctx context.Context, sel Selector, w io.Writer) error {
	client, closeConn, err := dial(ctx, sel)
	if err != nil {
		return err
	}
	defer closeConn()

	resp, err := listBrokers(ctx, client)
	if err != nil {
		return describedError(err)
	}
	return WriteBrokerList(w, resp.GetBrokers(), resp.GetLocal())
}

// RunPeerConnect connects to the selected Broker and pairs with the Broker at
// address. The result line goes to w and every warning goes to warn, each on
// its own line.
//
// The call is not bounded by UnaryTimeout: the Broker performs a pairing
// handshake that has its own ten-second timeout, and a deadline of the same
// length here would cancel the call before the Broker could report that
// timeout as a pairing failure.
func RunPeerConnect(ctx context.Context, sel Selector, address, key, alias string, w, warn io.Writer) error {
	client, closeConn, err := dial(ctx, sel)
	if err != nil {
		return err
	}
	defer closeConn()

	resp, err := client.Connect(ctx, &localpb.ConnectRequest{
		Address:    address,
		Key:        key,
		LocalAlias: alias,
	})
	if err != nil {
		return errors.New(connectErrorText(err))
	}
	if err := WritePairingResult(w, resp); err != nil {
		return err
	}
	return WriteWarnings(warn, resp.GetWarnings())
}

// connectErrorText describes a failed pairing and, when the other Broker's
// alias is taken, names --alias, the terminal command's form of the
// local_alias parameter.
func connectErrorText(err error) string {
	text := Describe(err)
	if errorCode(err) == localpb.ErrorCode_ERROR_CODE_NEEDS_LOCAL_ALIAS {
		text += "; rerun with --alias set to a new name for it"
	}
	return text
}

// RunPeerDisconnect connects to the selected Broker, removes one paired Broker
// and writes the line naming it.
func RunPeerDisconnect(ctx context.Context, sel Selector, broker string, w io.Writer) error {
	client, closeConn, err := dial(ctx, sel)
	if err != nil {
		return err
	}
	defer closeConn()

	callCtx, cancel := context.WithTimeout(ctx, UnaryTimeout)
	defer cancel()

	if _, err := client.Disconnect(callCtx, &localpb.DisconnectRequest{Broker: broker}); err != nil {
		return errors.New(disconnectErrorText(err))
	}
	_, err = fmt.Fprintf(w, "Disconnected %s\n", broker)
	return err
}

// disconnectErrorText describes a failed disconnect and, when no peer matches
// the reference, names the terminal command that lists the paired Brokers.
func disconnectErrorText(err error) string {
	text := Describe(err)
	if errorCode(err) == localpb.ErrorCode_ERROR_CODE_NOT_FOUND {
		text += "; `peers broker list` shows the paired Brokers"
	}
	return text
}

// PeerNames returns the aliases and fingerprints of the paired Brokers. It
// feeds the shell completion of a broker argument: a Broker known through the
// directory but not paired contributes no candidate. The caller decides what a
// failure means; this function writes nothing.
func PeerNames(ctx context.Context, sel Selector) ([]string, error) {
	client, closeConn, err := dial(ctx, sel)
	if err != nil {
		return nil, err
	}
	defer closeConn()

	resp, err := listBrokers(ctx, client)
	if err != nil {
		return nil, describedError(err)
	}
	var names []string
	for _, b := range resp.GetBrokers() {
		if !b.GetIsPeer() {
			continue
		}
		if alias := b.GetAlias(); alias != "" {
			names = append(names, alias)
		}
		if fingerprint := fingerprintHex(b.GetFingerprint()); fingerprint != "" {
			names = append(names, fingerprint)
		}
	}
	return names, nil
}

// ApplyUpdateResult is the outcome of the Broker's ApplyUpdate RPC.
type ApplyUpdateResult struct {
	FromVersion string
	ToVersion   string
	Restarting  bool
}

// ApplyUpdateError is a failed ApplyUpdate with the gRPC code kept, so a caller
// can tell the Broker's refusal from a transport or permission failure while
// the text stays the described one.
type ApplyUpdateError struct {
	Code codes.Code
	Text string
}

func (e *ApplyUpdateError) Error() string { return e.Text }

// IsFailedPrecondition reports whether err is an ApplyUpdate refusal. The
// update command falls back to replacing a user binary only on this
// answer, which is how a Broker that cannot apply updates itself declines.
func IsFailedPrecondition(err error) bool {
	var e *ApplyUpdateError
	return errors.As(err, &e) && e.Code == codes.FailedPrecondition
}

// RunApplyUpdate asks the selected Broker to install the newest release and
// restart itself. The request carries no version: the Broker decides what is
// newer and refuses a downgrade. The call has no deadline of its own because
// the Broker downloads and verifies the release before answering; cancelling
// ctx cancels that work.
func RunApplyUpdate(ctx context.Context, sel Selector) (ApplyUpdateResult, error) {
	client, closeConn, err := dial(ctx, sel)
	if err != nil {
		return ApplyUpdateResult{}, err
	}
	defer closeConn()

	resp, err := client.ApplyUpdate(ctx, &localpb.ApplyUpdateRequest{})
	if err != nil {
		return ApplyUpdateResult{}, &ApplyUpdateError{Code: status.Code(err), Text: Describe(err)}
	}
	return ApplyUpdateResult{
		FromVersion: resp.GetFromVersion(),
		ToVersion:   resp.GetToVersion(),
		Restarting:  resp.GetRestarting(),
	}, nil
}

// RunUpdateStatus returns the version the selected Broker runs now and the
// newer release it knows of. The call is bounded by updateStatusTimeout, which
// is short because it is polled while a Broker restarts; a Broker that is
// still starting refuses the connection instead of answering slowly.
func RunUpdateStatus(ctx context.Context, sel Selector) (current, available string, err error) {
	client, closeConn, err := dial(ctx, sel)
	if err != nil {
		return "", "", err
	}
	defer closeConn()

	callCtx, cancel := context.WithTimeout(ctx, updateStatusTimeout)
	defer cancel()
	resp, err := client.UpdateStatus(callCtx, &localpb.UpdateStatusRequest{})
	if err != nil {
		return "", "", describedError(err)
	}
	return resp.GetCurrentVersion(), resp.GetAvailableVersion(), nil
}

// listBrokers calls ListBrokers with the unary timeout. Both the listing
// commands and shell completion read the Broker list this way.
func listBrokers(ctx context.Context, client localpb.LocalClient) (*localpb.ListBrokersResponse, error) {
	callCtx, cancel := context.WithTimeout(ctx, UnaryTimeout)
	defer cancel()
	return client.ListBrokers(callCtx, &localpb.ListBrokersRequest{})
}

// dial selects the Broker, opens a connection through the selector's dialer and
// performs the version handshake. The returned close function closes the
// connection.
func dial(ctx context.Context, sel Selector) (localpb.LocalClient, func(), error) {
	path, open, err := sel(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot reach the Broker: %w", err)
	}

	conn, client, err := open()
	if err != nil {
		return nil, nil, describedError(err)
	}
	closeConn := func() { _ = conn.Close() }

	callCtx, cancel := context.WithTimeout(ctx, UnaryTimeout)
	defer cancel()

	resp, err := client.Hello(callCtx, &localpb.HelloRequest{ProtocolVersion: localpb.ProtocolVersion})
	if err != nil {
		closeConn()
		if st, ok := status.FromError(err); ok && st.Code() == codes.Unavailable {
			return nil, nil, fmt.Errorf("cannot reach the Broker at %s: %s", path, sanitizeText(st.Message()))
		}
		return nil, nil, describedError(err)
	}
	if v := resp.GetProtocolVersion(); v != localpb.ProtocolVersion {
		closeConn()
		return nil, nil, &VersionMismatchError{Broker: v, Client: localpb.ProtocolVersion}
	}
	return client, closeConn, nil
}
