package config

import (
	"fmt"
	"strings"
	"time"
)

// templateSections is the order in which the generated template lists the
// configuration sections.
var templateSections = []string{"broker", "pairing", "topics", "directory", "send", "link", "update", "log"}

const systemTemplateHeader = `# peers system Broker configuration.
#
# Every key is optional; a key that is absent takes its default value.
# YAML anchors, aliases and merge keys are not supported.
# Granting a group read access to this file lets its accounts read the
# configuration, the full status and the audit log; granting write access
# lets them change the configuration and manage pairing.
`

const userTemplateHeader = `# peers user Broker configuration.
#
# Every key is optional; a key that is absent takes its default value.
# YAML anchors, aliases and merge keys are not supported.
# This file belongs to this account only: only this account runs the Broker
# and reads or writes the file.
`

// templateExample is an inactive, valid example for a key that has no fixed
// default value.
type templateExample struct {
	value   string
	runtime bool   // the default is computed at runtime instead of being absent
	note    string // replaces the generic note when set
}

var templateExamples = map[string]templateExample{
	"broker.alias":              {value: "hostA.alice", runtime: true},
	"broker.advertise":          {value: "203.0.113.5:17443"},
	"broker.advertise_extra":    {value: "[203.0.113.5:17443]"},
	"broker.local_socket_group": {value: "peers"},
	"pairing.static_key": {
		value: `""`,
		note:  "Fill in 16 to 128 random characters, for example the output of `openssl rand -hex 32`. An empty value leaves the static key unset.",
	},
}

const systemOnlyNote = " This key applies to the system Broker only."

// Template returns the initial configuration file for mode. The file lists
// every configuration key with a one-line description. Keys with a fixed
// default carry that default as an active value; keys without one appear as an
// inactive example, so loading the file yields exactly the defaults.
func Template(mode Mode) []byte {
	var b strings.Builder
	if mode == ModeSystem {
		b.WriteString(systemTemplateHeader)
	} else {
		b.WriteString(userTemplateHeader)
	}
	names := allKeyNames()
	first := true
	for _, section := range templateSections {
		var sectionNames []string
		for _, name := range names {
			if s, _ := sectionAndLeaf(name); s == section {
				sectionNames = append(sectionNames, name)
			}
		}
		if len(sectionNames) == 0 {
			continue
		}
		if !first {
			b.WriteByte('\n')
		}
		first = false
		b.WriteString(section)
		b.WriteString(":\n")
		for _, name := range sectionNames {
			spec, _ := lookupKey(name)
			_, leaf := sectionAndLeaf(name)
			doc := spec.Doc
			if mode == ModeUser && (name == "broker.local_socket_group" || name == "broker.local_socket_mode") {
				doc += systemOnlyNote
			}
			if ex, ok := templateExamples[name]; ok {
				fmt.Fprintf(&b, "  # %s\n  # %s: %s\n", doc, leaf, ex.value)
				note := "No fixed default; this example is inactive."
				if ex.runtime {
					note = "The default is computed at runtime; this example is inactive."
				}
				if ex.note != "" {
					note = ex.note
				}
				fmt.Fprintf(&b, "  # %s\n", note)
				continue
			}
			v := defaultValue(spec, defaultContext{})
			text, err := encodeValueText(spec, v)
			if err != nil {
				panic("config: cannot encode the default of " + name + ": " + err.Error())
			}
			if spec.Kind == keyDuration {
				text = shortestDuration(v.Duration)
			}
			fmt.Fprintf(&b, "  # %s\n  %s: %s\n", doc, leaf, text)
		}
	}
	return []byte(b.String())
}

// shortestDuration renders d the way an operator would write it, dropping a
// zero seconds component: 1m0s becomes 1m and 1h0m0s becomes 1h. Parsing the
// text yields the same duration.
func shortestDuration(d time.Duration) string {
	s := d.String()
	if strings.HasSuffix(s, "m0s") {
		s = strings.TrimSuffix(s, "0s")
	}
	if strings.HasSuffix(s, "h0m") {
		s = strings.TrimSuffix(s, "0m")
	}
	return s
}
