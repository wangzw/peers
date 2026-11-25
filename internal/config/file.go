package config

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v3"
)

type configDocument struct {
	document *yaml.Node
	root     *yaml.Node
	values   map[string]Value
	present  map[string]bool
	invalid  map[string]error
	exists   bool
	// empty reports that the bytes held no YAML document at all: an empty
	// file, one containing only whitespace and comments, or a document with no
	// content. Load treats such a file as an empty configuration, but a reload
	// must treat it as unparseable so a truncation window cannot reset every
	// value to its default.
	empty       bool
	headComment string
	// raw holds the exact bytes this document was parsed from, or the bytes a
	// placeholder replacement produced. bytes returns it verbatim, so a Set
	// that only uncomments an inactive example line changes no other byte. A
	// node edit clears it and bytes re-encodes the document instead.
	raw []byte
	// contentHash is the hash of exactly the bytes that were parsed into this
	// document. Load copies it into core.lastFileHash, so the first reload
	// comparison starts from the hash of the bytes these values came from.
	// Reloads themselves hash the file on disk and compare core.lastFileHash;
	// a document built by parseConfigFile during a reload leaves this field
	// zero.
	contentHash [32]byte
}

// asEmpty records that data held no YAML document and returns the document
// carrying its leading comment and the exact parsed bytes.
func (d configDocument) asEmpty(data []byte) configDocument {
	d.empty = true
	d.headComment = leadingComment(data)
	d.raw = append([]byte(nil), data...)
	return d
}

func emptyDocument() configDocument {
	root := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	doc := &yaml.Node{Kind: yaml.DocumentNode, Content: []*yaml.Node{root}}
	return configDocument{
		document: doc,
		root:     root,
		values:   make(map[string]Value),
		present:  make(map[string]bool),
		invalid:  make(map[string]error),
	}
}

// maxConfigFileBytes is the largest configuration file, in bytes, that this
// package reads. A configuration file can be written by a member of the admin
// group for a system Broker, so the limit keeps an oversized file from
// consuming arbitrary memory. The generated templates and the documented
// examples are far smaller.
const maxConfigFileBytes = 1 << 20

// readConfigFileLimited reads path but never more than maxConfigFileBytes+1
// bytes. A file one byte above the limit is refused, so its remaining bytes are
// neither read nor parsed.
func readConfigFileLimited(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	data, err := io.ReadAll(io.LimitReader(f, maxConfigFileBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxConfigFileBytes {
		return nil, errors.New("the file is larger than 1 MiB")
	}
	return data, nil
}

func readConfigFile(path string) (configDocument, error) {
	return readConfigFileFrom(path, readConfigFileLimited)
}

// readConfigFileFrom reads the file through readFile exactly once, and records
// the hash of the same bytes it parsed. A second read to hash the file could
// see different content, leaving a stored hash that describes bytes the parsed
// values never came from.
func readConfigFileFrom(path string, readFile func(string) ([]byte, error)) (configDocument, error) {
	data, err := readFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			doc := emptyDocument()
			doc.empty = true
			return doc, nil
		}
		return configDocument{}, fmt.Errorf("cannot read %s: %w", path, err)
	}
	// The default reader applies this limit itself; the check also covers a
	// reader supplied through the seam, so no path can hand an oversized file
	// to the parser.
	if len(data) > maxConfigFileBytes {
		return configDocument{}, fmt.Errorf("cannot read %s: the file is larger than 1 MiB", path)
	}
	hash := sha256.Sum256(data)
	doc, err := parseConfigFile(data)
	if err != nil {
		return configDocument{}, err
	}
	doc.exists = true
	doc.contentHash = hash
	return doc, nil
}

func parseConfigFile(data []byte) (configDocument, error) {
	doc := emptyDocument()
	dec := yaml.NewDecoder(bytes.NewReader(data))
	var parsed yaml.Node
	err := dec.Decode(&parsed)
	if errors.Is(err, io.EOF) {
		return doc.asEmpty(data), nil
	}
	if err != nil {
		return configDocument{}, fmt.Errorf("config file parse error: %w", err)
	}
	var extra yaml.Node
	if err := dec.Decode(&extra); err == nil && extra.Kind != 0 {
		return configDocument{}, errors.New("config file parse error: multiple YAML documents")
	} else if err != nil && !errors.Is(err, io.EOF) {
		return configDocument{}, fmt.Errorf("config file parse error: %w", err)
	}
	root := &parsed
	if root.Kind == yaml.DocumentNode {
		if len(root.Content) == 0 {
			return doc.asEmpty(data), nil
		}
		root = root.Content[0]
	}
	if root == nil || root.Kind == 0 {
		return doc.asEmpty(data), nil
	}
	// checkStructure runs before the top-level kind check so a document whose
	// first node is an alias is refused as an anchor or alias, not as a
	// non-mapping. It walks the node tree only, so it never follows an alias
	// pointer and cannot cycle or expand an alias exponentially.
	if err := checkStructure(root); err != nil {
		return configDocument{}, err
	}
	if root.Kind != yaml.MappingNode {
		return configDocument{}, fmt.Errorf("config file parse error: top level must be a mapping at line %d, column %d", root.Line, root.Column)
	}
	for _, section := range knownSections() {
		n, ok := findSection(root, section)
		if !ok {
			continue
		}
		if n == nil || n.Kind != yaml.MappingNode {
			line, column := sectionPosition(root, section)
			return configDocument{}, fmt.Errorf("config file parse error: section %q must be a mapping at line %d, column %d", section, line, column)
		}
	}
	doc.document = &parsed
	doc.root = root
	for _, name := range allKeyNames() {
		keyNode, ok := findKeyNode(root, name)
		if !ok {
			continue
		}
		doc.present[name] = true
		spec, _ := lookupKey(name)
		v, err := parseNode(spec, keyNode, false)
		if err != nil {
			doc.invalid[name] = err
			continue
		}
		doc.values[name] = v
	}
	doc.raw = append([]byte(nil), data...)
	return doc, nil
}

// leadingComment returns the whole file when it contains only blank lines and
// comments. Such a document has no YAML nodes to carry the comments.
func leadingComment(data []byte) string {
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" && !strings.HasPrefix(trimmed, "#") {
			return ""
		}
	}
	return string(data)
}

// maxConfigNesting is the deepest mapping and sequence nesting a configuration
// file may use. A file one level deeper is refused, so an extremely deep
// document cannot exhaust the stack. The templates and normal configurations
// are far shallower.
const maxConfigNesting = 32

func checkStructure(n *yaml.Node) error {
	return checkNodeStructure(n, 1)
}

// checkNodeStructure walks the node tree, rejecting anchors, aliases and
// excessive nesting. It never follows an Alias pointer, so a self-referential
// or exponentially expanding alias cannot run away: the first alias or anchored
// node ends the walk with a parse error.
func checkNodeStructure(n *yaml.Node, depth int) error {
	if n == nil {
		return errors.New("config file parse error: empty node")
	}
	if n.Kind == yaml.AliasNode || n.Anchor != "" {
		return fmt.Errorf("config file parse error: anchors and aliases are not supported at line %d, column %d", n.Line, n.Column)
	}
	switch n.Kind {
	case yaml.MappingNode, yaml.SequenceNode:
		if depth > maxConfigNesting {
			return fmt.Errorf("config file parse error: nesting is deeper than %d levels at line %d, column %d", maxConfigNesting, n.Line, n.Column)
		}
	}
	switch n.Kind {
	case yaml.MappingNode:
		seen := make(map[string]bool, len(n.Content)/2)
		for i := 0; i+1 < len(n.Content); i += 2 {
			k := n.Content[i]
			if k == nil {
				return errors.New("config file parse error: empty node")
			}
			if k.Kind == yaml.AliasNode || k.Anchor != "" {
				return fmt.Errorf("config file parse error: anchors and aliases are not supported at line %d, column %d", k.Line, k.Column)
			}
			if k.Kind != yaml.ScalarNode {
				return fmt.Errorf("config file parse error: mapping key at line %d, column %d is not a scalar", n.Line, n.Column)
			}
			if k.Value == "<<" {
				return fmt.Errorf("config file parse error: merge keys are not supported at line %d, column %d", k.Line, k.Column)
			}
			if seen[k.Value] {
				return fmt.Errorf("config file parse error: duplicate key %q at line %d, column %d", k.Value, k.Line, k.Column)
			}
			seen[k.Value] = true
			if err := checkNodeStructure(n.Content[i+1], depth+1); err != nil {
				return err
			}
		}
	case yaml.SequenceNode:
		for _, item := range n.Content {
			if err := checkNodeStructure(item, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func sectionAndLeaf(key string) (section, leaf string) {
	if i := strings.IndexByte(key, '.'); i >= 0 {
		return key[:i], key[i+1:]
	}
	return key, ""
}

// replacePlaceholderLine replaces the first line matching
// "^\s*#\s*<leaf>:" inside section with an active "<leaf>: <value>" line. It
// returns false when the section or such a line is absent. The replacement
// keeps the line's indentation and every other byte of the file.
func replacePlaceholderLine(data []byte, section, leaf string, spec *keySpec, v Value) ([]byte, bool) {
	text, err := encodeValueText(spec, v)
	if err != nil {
		return nil, false
	}
	lines := strings.Split(string(data), "\n")
	for i := 0; i < len(lines); i++ {
		indent, ok := sectionHeaderIndent(lines[i], section)
		if !ok {
			continue
		}
		for j := i + 1; j < len(lines); j++ {
			if strings.TrimSpace(lines[j]) == "" {
				continue
			}
			if lineIndent(lines[j]) <= indent {
				break // the section ended
			}
			at, ok := placeholderIndent(lines[j], leaf)
			if !ok {
				continue
			}
			cr := ""
			if strings.HasSuffix(lines[j], "\r") {
				cr = "\r"
			}
			lines[j] = lines[j][:at] + leaf + ": " + text + cr
			return []byte(strings.Join(lines, "\n")), true
		}
	}
	return nil, false
}

// sectionHeaderIndent reports the indentation of a "section:" header line and
// whether line is one.
func sectionHeaderIndent(line, section string) (int, bool) {
	indent := lineIndent(line)
	rest := line[indent:]
	if !strings.HasPrefix(rest, section) {
		return 0, false
	}
	after := rest[len(section):]
	if !strings.HasPrefix(after, ":") {
		return 0, false
	}
	trimmed := strings.TrimSpace(after[1:])
	if trimmed != "" && !strings.HasPrefix(trimmed, "#") {
		return 0, false
	}
	return indent, true
}

// placeholderIndent reports the indentation of a line matching
// "^\s*#\s*<leaf>:<end or whitespace>" and whether line is one.
func placeholderIndent(line, leaf string) (int, bool) {
	indent := lineIndent(line)
	rest := line[indent:]
	if !strings.HasPrefix(rest, "#") {
		return 0, false
	}
	rest = strings.TrimLeft(rest[1:], " \t")
	if !strings.HasPrefix(rest, leaf) {
		return 0, false
	}
	after := rest[len(leaf):]
	if !strings.HasPrefix(after, ":") {
		return 0, false
	}
	after = after[1:]
	if after != "" && after[0] != ' ' && after[0] != '\t' && after[0] != '\r' {
		return 0, false
	}
	return indent, true
}

// lineIndent returns the number of leading spaces and tabs in line.
func lineIndent(line string) int {
	i := 0
	for i < len(line) && (line[i] == ' ' || line[i] == '\t') {
		i++
	}
	return i
}

func knownSections() []string {
	seen := make(map[string]bool)
	var out []string
	for name := range keyTable {
		section, _ := sectionAndLeaf(name)
		if !seen[section] {
			seen[section] = true
			out = append(out, section)
		}
	}
	sort.Strings(out)
	return out
}

func sectionPosition(root *yaml.Node, section string) (line, column int) {
	for i := 0; i+1 < len(root.Content); i += 2 {
		k := resolveAliasNode(root.Content[i])
		if k != nil && k.Kind == yaml.ScalarNode && k.Value == section {
			return k.Line, k.Column
		}
	}
	return root.Line, root.Column
}

func findSection(root *yaml.Node, section string) (*yaml.Node, bool) {
	for i := 0; i+1 < len(root.Content); i += 2 {
		k := resolveAliasNode(root.Content[i])
		if k != nil && k.Kind == yaml.ScalarNode && k.Value == section {
			return resolveAliasNode(root.Content[i+1]), true
		}
	}
	return nil, false
}

func findKeyNode(root *yaml.Node, key string) (*yaml.Node, bool) {
	section, leaf := sectionAndLeaf(key)
	m, ok := findSection(root, section)
	if !ok {
		return nil, false
	}
	if m == nil || m.Kind != yaml.MappingNode {
		return nil, false
	}
	for i := 0; i+1 < len(m.Content); i += 2 {
		k := resolveAliasNode(m.Content[i])
		if k != nil && k.Kind == yaml.ScalarNode && k.Value == leaf {
			return resolveAliasNode(m.Content[i+1]), true
		}
	}
	return nil, false
}

func (d *configDocument) nodeFor(key string) *yaml.Node {
	if d == nil || d.root == nil {
		return nil
	}
	n, _ := findKeyNode(d.root, key)
	return n
}

func (d *configDocument) setKey(key string, v Value) error {
	spec, ok := lookupKey(key)
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownKey, key)
	}
	if d.root == nil {
		d.document = nil
		*d = emptyDocument()
	}
	if d.root.Kind != yaml.MappingNode {
		return fmt.Errorf("%w: %s: top level is not a mapping", ErrInvalidValue, key)
	}
	section, leaf := sectionAndLeaf(key)
	// A key that exists only as an inactive example line is replaced where
	// that line stands, so the rest of the file stays byte-identical. The
	// result is parsed back and checked before it is adopted.
	if d.raw != nil {
		if _, present := findKeyNode(d.root, key); !present {
			if replaced, ok := replacePlaceholderLine(d.raw, section, leaf, spec, v); ok {
				if parsed, err := parseConfigFile(replaced); err == nil {
					if got, ok := parsed.values[key]; ok && valueEqual(got, v) {
						d.document = parsed.document
						d.root = parsed.root
						d.values = parsed.values
						d.present = parsed.present
						d.invalid = parsed.invalid
						d.empty = parsed.empty
						d.raw = replaced
						d.values[key] = copyValue(v)
						d.present[key] = true
						delete(d.invalid, key)
						return nil
					}
				}
			}
		}
	}
	// Any node edit below re-encodes the whole document.
	d.raw = nil
	m, ok := findSection(d.root, section)
	if !ok {
		sectionNode := &yaml.Node{
			Kind:  yaml.MappingNode,
			Tag:   "!!map",
			Value: "",
		}
		keyNode := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: section}
		d.root.Content = append(d.root.Content, keyNode, sectionNode)
		m = sectionNode
	}
	if m == nil || m.Kind != yaml.MappingNode {
		return fmt.Errorf("%w: %s: section %q is not a mapping", ErrInvalidValue, key, section)
	}
	newValue := encodeValueNode(spec, v)
	var old *yaml.Node
	index := -1
	for i := 0; i+1 < len(m.Content); i += 2 {
		k := resolveAliasNode(m.Content[i])
		if k != nil && k.Kind == yaml.ScalarNode && k.Value == leaf {
			index = i + 1
			old = m.Content[i+1]
			break
		}
	}
	if index >= 0 {
		oldTarget := resolveAliasNode(old)
		if oldTarget != nil {
			newValue.HeadComment = oldTarget.HeadComment
			newValue.LineComment = oldTarget.LineComment
			newValue.FootComment = oldTarget.FootComment
			if newValue.Kind == yaml.ScalarNode && oldTarget.Style&yaml.DoubleQuotedStyle != 0 {
				newValue.Style = yaml.DoubleQuotedStyle
			}
		}
		m.Content[index] = newValue
	} else {
		keyNode := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: leaf}
		m.Content = append(m.Content, keyNode, newValue)
	}
	d.values[key] = copyValue(v)
	d.present[key] = true
	delete(d.invalid, key)
	return nil
}

func encodeValueNode(spec *keySpec, v Value) *yaml.Node {
	switch spec.Kind {
	case keyBool:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: strconv.FormatBool(v.Bool)}
	case keyInt:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!int", Value: fmt.Sprintf("%d", v.Int)}
	case keyDuration:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: v.Duration.String()}
	case keyStringList:
		n := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
		for _, item := range v.Strings {
			n.Content = append(n.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: item})
		}
		return n
	default: // keyString, keySecret
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: v.String}
	}
}

// encodeValueText encodes v as the YAML text that follows "<leaf>: " in a
// configuration file. Strings that YAML would read back as another type, such
// as ":17443" or "0666", come back quoted, so parsing the text yields the same
// Value. String lists are flow sequences, which both the file and SetConfig
// accept.
func encodeValueText(spec *keySpec, v Value) (string, error) {
	var plain any
	switch spec.Kind {
	case keyBool:
		plain = v.Bool
	case keyInt:
		plain = v.Int
	case keyDuration:
		plain = v.Duration.String()
	case keyStringList:
		node := encodeValueNode(spec, v)
		node.Style = yaml.FlowStyle
		data, err := yaml.Marshal(node)
		if err != nil {
			return "", err
		}
		return strings.TrimSuffix(string(data), "\n"), nil
	default: // keyString, keySecret
		plain = v.String
	}
	data, err := yaml.Marshal(plain)
	if err != nil {
		return "", err
	}
	text := strings.TrimSuffix(string(data), "\n")
	// A string value such as ":17443" is a legal plain YAML scalar, so the
	// encoder leaves it unquoted. Quote it anyway: a value that starts with
	// ':' reads like a port and should never depend on that subtlety.
	if (spec.Kind == keyString || spec.Kind == keySecret) && strings.HasPrefix(text, ":") {
		node := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: v.String, Style: yaml.DoubleQuotedStyle}
		data, err := yaml.Marshal(node)
		if err != nil {
			return "", err
		}
		return strings.TrimSuffix(string(data), "\n"), nil
	}
	return text, nil
}

func (d *configDocument) bytes() ([]byte, error) {
	if d.root == nil {
		*d = emptyDocument()
	}
	if d.raw != nil {
		out := d.raw
		if len(out) == 0 || out[len(out)-1] != '\n' {
			out = append(append([]byte(nil), out...), '\n')
		}
		if err := d.verify(out); err != nil {
			return nil, err
		}
		return out, nil
	}
	var buf bytes.Buffer
	if d.headComment != "" {
		buf.WriteString(strings.TrimRight(d.headComment, "\n"))
		buf.WriteString("\n")
	}
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(d.document); err != nil {
		return nil, fmt.Errorf("%w: cannot encode config file: %v", ErrFileWrite, err)
	}
	if err := enc.Close(); err != nil {
		return nil, fmt.Errorf("%w: cannot encode config file: %v", ErrFileWrite, err)
	}
	out := buf.Bytes()
	if len(out) == 0 || out[len(out)-1] != '\n' {
		out = append(out, '\n')
	}
	if err := d.verify(out); err != nil {
		return nil, err
	}
	return out, nil
}

// verify parses the encoded bytes again, so a node edit can never produce an
// invalid structure.
func (d *configDocument) verify(data []byte) error {
	back, err := parseConfigFile(data)
	if err != nil {
		return fmt.Errorf("%w: encoded config file is invalid: %v", ErrFileWrite, err)
	}
	for name, want := range d.values {
		got, ok := back.values[name]
		if !ok {
			return fmt.Errorf("%w: encoded config file lost key %s", ErrFileWrite, name)
		}
		if !valueEqual(want, got) {
			return fmt.Errorf("%w: encoded config file changed key %s", ErrFileWrite, name)
		}
	}
	return nil
}

func valueEqual(a, b Value) bool {
	if a.Kind != b.Kind || a.String != b.String || a.Bool != b.Bool ||
		a.Int != b.Int || a.Duration != b.Duration || a.SecretIsSet != b.SecretIsSet ||
		len(a.Strings) != len(b.Strings) {
		return false
	}
	for i := range a.Strings {
		if a.Strings[i] != b.Strings[i] {
			return false
		}
	}
	return true
}

// configFileDirMode returns the mode for the directory holding the config
// file when writeConfigFile has to create it. The deployment mode fixes the
// directory mode independently of the file mode: a user Broker writes 0600
// under a 0700 data directory, a system Broker writes 0600 under the
// group-readable /etc/peers (0755). The file itself is 0600 in both modes,
// because the Broker cannot know the administration group the installation
// gives the file.
func configFileDirMode(mode Mode) os.FileMode {
	if mode == ModeSystem {
		return 0o755
	}
	return 0o700
}

// openConfigFileForWrite opens the configuration file. It is a seam: a real
// test file cannot produce the create race or the mode failure.
var openConfigFileForWrite = os.OpenFile

// writeConfigFile rewrites path in place: existing ownership, group, mode and
// ACLs stay in place because the same inode is reused. A new file is created
// with mode 0600. O_NOFOLLOW makes a path that is a symbolic link fail instead
// of truncating the link's target, which could be a file outside the
// configuration directory.
func writeConfigFile(path string, data []byte, mode Mode) error {
	f, err := openConfigFileForWrite(path, os.O_RDWR|unix.O_NOFOLLOW, 0)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: %s: %v", ErrFileWrite, path, err)
		}
		if mkErr := os.MkdirAll(filepath.Dir(path), configFileDirMode(mode)); mkErr != nil {
			return fmt.Errorf("%w: %s: %v", ErrFileWrite, path, mkErr)
		}
		f, err = openConfigFileForWrite(path, os.O_RDWR|os.O_CREATE|os.O_EXCL|unix.O_NOFOLLOW, 0o600)
		if err != nil {
			if errors.Is(err, os.ErrExist) {
				f, err = openConfigFileForWrite(path, os.O_RDWR|unix.O_NOFOLLOW, 0)
			}
			if err != nil {
				return fmt.Errorf("%w: %s: %v", ErrFileWrite, path, err)
			}
		} else if chmodErr := f.Chmod(0o600); chmodErr != nil {
			_ = f.Close()
			return fmt.Errorf("%w: %s: %v", ErrFileWrite, path, chmodErr)
		}
	}
	defer func() { _ = f.Close() }()
	if err := f.Truncate(0); err != nil {
		return fmt.Errorf("%w: %s: %v", ErrFileWrite, path, err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("%w: %s: %v", ErrFileWrite, path, err)
	}
	n, err := f.Write(data)
	if err != nil {
		return fmt.Errorf("%w: %s: %v", ErrFileWrite, path, err)
	}
	if n != len(data) {
		return fmt.Errorf("%w: %s: short write", ErrFileWrite, path)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("%w: %s: %v", ErrFileWrite, path, err)
	}
	return nil
}
