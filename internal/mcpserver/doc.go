// Package mcpserver is the per-session MCP server process. It declares the
// tools to the model, selects a local Broker and registers the session as an
// agent in the background, and forwards each tool call to that Broker.
package mcpserver
