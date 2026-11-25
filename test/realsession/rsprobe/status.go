package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// statusTimeout bounds the Hello and Status calls; it only decides how long a
// Broker that never answers takes to fail.
const statusTimeout = eventTimeout

func init() {
	register("status", cmdStatus)
}

func cmdStatus(args []string) error {
	return runStatus(args, os.Stdout)
}

func runStatus(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	socket := fs.String("socket", defaultSocketPath(), "path to the Broker's local socket")
	asJSON := fs.Bool("json", false, "print the raw Status response as JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}

	resp, err := fetchStatus(*socket)
	if err != nil {
		return err
	}
	if *asJSON {
		raw, err := protojson.Marshal(resp)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(stdout, "%s\n", raw)
		return err
	}
	return writeStatus(stdout, resp)
}

func defaultSocketPath() string {
	return fmt.Sprintf("/tmp/peers-%d/broker.sock", os.Getuid())
}

func fetchStatus(path string) (*localpb.StatusResponse, error) {
	conn, err := grpc.NewClient("unix://"+path, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("broker not reachable at %s: %v", path, err)
	}
	defer func() { _ = conn.Close() }()
	client := localpb.NewLocalClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), statusTimeout)
	defer cancel()

	hello, err := client.Hello(ctx, &localpb.HelloRequest{ProtocolVersion: localpb.ProtocolVersion})
	if err != nil {
		return nil, fmt.Errorf("broker not reachable at %s: %v", path, err)
	}
	if v := hello.GetProtocolVersion(); v != localpb.ProtocolVersion {
		return nil, fmt.Errorf("broker at %s speaks protocol version %d, want %d", path, v, localpb.ProtocolVersion)
	}

	resp, err := client.Status(ctx, &localpb.StatusRequest{})
	if err != nil {
		return nil, fmt.Errorf("status: %v", err)
	}
	return resp, nil
}

func writeStatus(w io.Writer, resp *localpb.StatusResponse) error {
	ids := make([]string, 0, len(resp.GetAgents()))
	for _, agent := range resp.GetAgents() {
		ids = append(ids, agent.GetAgentId())
	}
	refs := shortRefs(ids)
	for _, agent := range resp.GetAgents() {
		if _, err := fmt.Fprintf(w, "%s %s status=%s topics=%s prev=%s\n",
			refs[agent.GetAgentId()],
			agent.GetAddress(),
			agentStatusLabel(agent.GetStatus()),
			strings.Join(agent.GetTopics(), ","),
			agent.GetPreviousName()); err != nil {
			return err
		}
	}
	for _, peer := range resp.GetPeers() {
		if _, err := fmt.Fprintf(w, "peer %s %s\n", peer.GetAlias(), brokerStatusLabel(peer.GetStatus())); err != nil {
			return err
		}
	}
	return nil
}

func agentStatusLabel(v localpb.AgentStatus) string {
	return enumLabel(localpb.AgentStatus_name, "AGENT_STATUS_", int32(v))
}

func brokerStatusLabel(v localpb.BrokerStatus) string {
	return enumLabel(localpb.BrokerStatus_name, "BROKER_STATUS_", int32(v))
}

// enumLabel renders a proto enum value without its shared prefix, in lower
// case, as the terminal status command does.
func enumLabel(names map[int32]string, prefix string, v int32) string {
	if name, ok := names[v]; ok {
		return strings.ToLower(strings.TrimPrefix(name, prefix))
	}
	return strconv.Itoa(int(v))
}
