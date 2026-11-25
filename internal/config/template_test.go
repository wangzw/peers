package config

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var updateGolden = flag.Bool("update", false, "rewrite the golden template files")

func modeName(mode Mode) string {
	if mode == ModeSystem {
		return "system"
	}
	return "user"
}

// TestTemplateModesParseAndLoadDefaults checks that each mode's template parses
// and yields exactly the defaults.
func TestTemplateModesParseAndLoadDefaults(t *testing.T) {
	for _, mode := range []Mode{ModeUser, ModeSystem} {
		mode := mode
		t.Run(modeName(mode), func(t *testing.T) {
			body := Template(mode)
			doc, err := parseConfigFile(body)
			if err != nil {
				t.Fatalf("template does not parse: %v\n%s", err, body)
			}
			if doc.empty {
				t.Fatalf("template parsed as an empty document:\n%s", body)
			}
			for name, err := range doc.invalid {
				t.Errorf("template value for %s is invalid: %v", name, err)
			}

			c, _ := newValueCore(t, string(body), "")
			ctx := defaultContext{mode: c.mode, hostname: c.hostnameString(), account: c.accountString()}
			for _, name := range allKeyNames() {
				spec, _ := lookupKey(name)
				want := defaultValue(spec, ctx)
				got, ok := c.values[name]
				if !ok {
					t.Errorf("%s has no effective value after Load", name)
					continue
				}
				if !valueEqual(got, want) {
					t.Errorf("%s = %+v after Load, want the default %+v", name, got, want)
				}
			}
		})
	}
}

// TestTemplateListsEachKeyOnceAndDocumentsIt checks that the template lists
// each key once, in section and key order, with a description line.
func TestTemplateListsEachKeyOnceAndDocumentsIt(t *testing.T) {
	for _, mode := range []Mode{ModeUser, ModeSystem} {
		body := string(Template(mode))
		for _, name := range allKeyNames() {
			spec, _ := lookupKey(name)
			if strings.TrimSpace(spec.Doc) == "" {
				t.Errorf("%s has no documentation line", name)
			}
			_, leaf := sectionAndLeaf(name)
			re := regexp.MustCompile(`(?m)^\s*#?\s*` + regexp.QuoteMeta(leaf) + `\s*:`)
			if n := len(re.FindAllStringIndex(body, -1)); n != 1 {
				t.Errorf("%s (%s mode) appears %d times in the template, want exactly once", name, modeName(mode), n)
			}
		}

		last := -1
		for _, section := range templateSections {
			i := strings.Index(body, "\n"+section+":\n")
			if i < 0 {
				t.Fatalf("%s mode template has no section %q:\n%s", modeName(mode), section, body)
			}
			if i <= last {
				t.Errorf("section %q is out of order in the %s template", section, modeName(mode))
			}
			last = i
		}

		for _, section := range templateSections {
			prev := -1
			for _, name := range allKeyNames() {
				if s, _ := sectionAndLeaf(name); s != section {
					continue
				}
				_, leaf := sectionAndLeaf(name)
				idx := strings.Index(body, "  "+leaf+": ")
				if idx < 0 {
					idx = strings.Index(body, "  # "+leaf+": ")
				}
				if idx < 0 {
					t.Errorf("entry for %s is missing from the %s template", name, modeName(mode))
					continue
				}
				if idx <= prev {
					t.Errorf("%s is out of key order in section %q", name, section)
				}
				prev = idx
			}
		}
	}
}

// TestTemplatePlaceholderExamplesValidate checks that uncommenting each
// example yields a valid effective value.
func TestTemplatePlaceholderExamplesValidate(t *testing.T) {
	body := string(Template(ModeUser))
	for _, name := range allKeyNames() {
		ex, ok := templateExamples[name]
		if !ok {
			continue
		}
		spec, _ := lookupKey(name)
		_, leaf := sectionAndLeaf(name)
		re := regexp.MustCompile(`(?m)^(\s*)#\s*` + regexp.QuoteMeta(leaf) + `:\s*(.*)$`)
		if !re.MatchString(body) {
			t.Errorf("%s has no inactive example line", name)
			continue
		}
		uncommented := re.ReplaceAllString(body, "${1}"+leaf+": ${2}")
		parsed, err := parseConfigFile([]byte(uncommented))
		if err != nil {
			t.Errorf("uncommenting %s makes the template unparseable: %v", name, err)
			continue
		}
		if _, ok := parsed.values[name]; !ok {
			t.Errorf("uncommenting %s did not yield an effective value", name)
			continue
		}
		if err := validateValue(spec, parsed.values[name]); err != nil {
			t.Errorf("uncommented example for %s is invalid: %v (example %q)", name, err, ex.value)
		}
	}
}

// TestTemplateGolden checks that each mode's generated template matches the
// checked-in copy, and that -update rewrites the copy.
func TestTemplateGolden(t *testing.T) {
	for _, mode := range []Mode{ModeSystem, ModeUser} {
		mode := mode
		t.Run(modeName(mode), func(t *testing.T) {
			path := filepath.Join("testdata", "template-"+modeName(mode)+".yaml")
			got := Template(mode)
			if *updateGolden {
				if err := os.WriteFile(path, got, 0o644); err != nil {
					t.Fatalf("updating %s: %v", path, err)
				}
				return
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("reading %s: %v", path, err)
			}
			if !bytes.Equal(got, want) {
				t.Errorf("the %s template differs from %s\n--- want ---\n%s\n--- got ---\n%s",
					modeName(mode), path, want, got)
			}
		})
	}
}

// TestTemplateStaticKeyPlaceholderStaysUnset checks that uncommenting the
// static_key example leaves the key unset.
func TestTemplateStaticKeyPlaceholderStaysUnset(t *testing.T) {
	body := string(Template(ModeUser))
	re := regexp.MustCompile(`(?m)^(\s*)#\s*static_key:\s*""\s*$`)
	if !re.MatchString(body) {
		t.Fatalf("template has no `# static_key: \"\"` placeholder:\n%s", body)
	}
	uncommented := re.ReplaceAllString(body, `${1}static_key: ""`)
	parsed, err := parseConfigFile([]byte(uncommented))
	if err != nil {
		t.Fatalf("uncommenting static_key makes the template unparseable: %v", err)
	}
	v, ok := parsed.values["pairing.static_key"]
	if !ok {
		t.Fatalf("pairing.static_key has no value after uncommenting:\n%s", uncommented)
	}
	if v.SecretIsSet || v.String != "" {
		t.Errorf("pairing.static_key is set to %q after uncommenting the empty placeholder", v.String)
	}
	spec, _ := lookupKey("pairing.static_key")
	if err := validateValue(spec, v); err != nil {
		t.Errorf("the uncommented empty static key is invalid: %v", err)
	}
}

// TestTemplateUserModeMarksSystemOnlyKeys checks that the user template marks
// the two system-only keys and the system template does not.
func TestTemplateUserModeMarksSystemOnlyKeys(t *testing.T) {
	user := string(Template(ModeUser))
	system := string(Template(ModeSystem))
	if n := strings.Count(user, strings.TrimSpace(systemOnlyNote)); n != 2 {
		t.Errorf("user template carries the system-only note %d times, want 2:\n%s", n, user)
	}
	if strings.Contains(system, strings.TrimSpace(systemOnlyNote)) {
		t.Errorf("system template carries the system-only note:\n%s", system)
	}
}

// TestTemplateQuotesDefaultsThatYAMLWouldRetype checks that the template quotes
// defaults YAML would otherwise retype.
func TestTemplateQuotesDefaultsThatYAMLWouldRetype(t *testing.T) {
	body := string(Template(ModeUser))
	for _, want := range []string{`  listen: ":17443"`, `  local_socket_mode: "0666"`} {
		if !strings.Contains(body, want) {
			t.Errorf("template does not contain %q:\n%s", want, body)
		}
	}
}
