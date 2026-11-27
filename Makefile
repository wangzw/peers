# Build gates for peers.
#
# Every parallel-capable target takes its job count from JOBS, which the caller
# sets explicitly (default 4). Nothing here probes the host's CPU count.

JOBS ?= 4

# Statement coverage for the two test rounds, off by default. When COVER_DIR
# names an absolute directory, each round wipes its own subdirectory (unit,
# testenv) and has the instrumented test binaries write Go's covdata files
# there, which cover-report merges. An empty value runs exactly the plain
# commands, so a developer's make test neither pays for instrumentation nor
# needs a coverage directory.
#
# make check collects into CHECK_COVER_DIR: $(CURDIR)/.cover unless the caller
# set COVER_DIR at all. $(origin ...) is read here, before the default below
# assigns COVER_DIR, so an explicit COVER_DIR= passes through as empty and
# makes check fail at its guard instead of running without data.
CHECK_COVER_DIR := $(if $(filter undefined,$(origin COVER_DIR)),$(CURDIR)/.cover,$(COVER_DIR))
COVER_DIR ?=
COVER_PKGS := github.com/wangzw/peers/internal/...,github.com/wangzw/peers/cmd/...

# Settings for cover-report and cover-check. COVER_INPUTS names the producer
# directories under COVER_DIR to merge; COVER_TOTAL and COVER_PACKAGE are the
# floors cover-check enforces, and COVER_FORMAT selects the plain table (text)
# or the Markdown table CI appends to the step summary. COVER_FORMAT and
# COVER_DIR are free: a caller may still choose them however it likes.
COVER_INPUTS ?= unit testenv e2e
COVER_TOTAL ?= 90
COVER_PACKAGE ?= 80
COVER_FORMAT ?= text

# The gate's own floors and input set. make check passes these to cover-check
# explicitly, and each honours only a command-line value: an exported
# COVER_TOTAL=0 or COVER_INPUTS= in the environment cannot loosen the gate.
CHECK_COVER_TOTAL := $(if $(filter command line,$(origin COVER_TOTAL)),$(COVER_TOTAL),90)
CHECK_COVER_PACKAGE := $(if $(filter command line,$(origin COVER_PACKAGE)),$(COVER_PACKAGE),80)
CHECK_COVER_INPUTS := $(if $(filter command line,$(origin COVER_INPUTS)),$(COVER_INPUTS),unit testenv e2e)

# Guard and prepare one coverage round: refuse a relative directory, then
# recreate the round's own subdirectory empty so no stale data survives. The
# shell body is a separate variable so the $(if) below never parses the ')' in
# its case patterns; both expand to nothing when coverage is off.
COVER_ROUND_PREP_ON = case "$(COVER_DIR)" in /*) ;; *) echo "test: COVER_DIR must name an absolute path (got '$(COVER_DIR)')" >&2; exit 2 ;; esac; rm -rf "$(COVER_DIR)/$(1)" && mkdir -p "$(COVER_DIR)/$(1)";
COVER_ROUND_PREP = $(if $(COVER_DIR),$(COVER_ROUND_PREP_ON))

# Instrumentation flags for a test round, and the trailing -args that sends the
# round's covdata directory to the test binary. -args passes everything after
# it to the binary, so it must stay last on the go test command line.
COVER_GO_FLAGS = $(if $(COVER_DIR), -cover -covermode=atomic -coverpkg=$(COVER_PKGS))
COVER_GO_ARGS = $(if $(COVER_DIR), -args -test.gocoverdir=$(COVER_DIR)/$(1))

# coverreport's -i value: every COVER_INPUTS name, which may be a relative
# subpath such as coverage-test/unit, prefixed with COVER_DIR/ and the results
# joined by commas. Recursive, so a command-line override of COVER_INPUTS is
# honoured; the cover-report and cover-check guards reject an empty COVER_DIR
# before the value is used.
COVER_EMPTY :=
COVER_SPACE := $(COVER_EMPTY) $(COVER_EMPTY)
COVER_COMMA := ,
COVER_INPUT_DIRS = $(subst $(COVER_SPACE),$(COVER_COMMA),$(strip $(foreach name,$(COVER_INPUTS),$(COVER_DIR)/$(name))))

# Optional directory for golangci-lint's cache. The global run lock is disabled
# in .golangci.yml (allow-parallel-runners), so concurrent gates share the
# default cache safely and need no separate directory here. A caller may still
# point this at a private cache to keep one experiment out of the others; the
# linter then also gets a private TMPDIR under this directory. When empty, the
# linter's own defaults are used.
GOLANGCI_LINT_CACHE ?=

# golangci-lint with the caller's private cache and TMPDIR.
GOLANGCI_LINT = $(if $(GOLANGCI_LINT_CACHE),env GOLANGCI_LINT_CACHE="$(GOLANGCI_LINT_CACHE)" TMPDIR="$(GOLANGCI_LINT_CACHE)/tmp" )golangci-lint

GIT ?= git

# Total timeout of the end-to-end test binary, passed to go test -timeout.
E2E_TIMEOUT ?= 30m

# Packages the e2e target runs and an optional -run regex selecting top-level
# tests. The defaults run every e2e package and every test. ci.yml sets both to
# run one shard; scripts/e2e-shards prints the values for that shard.
E2E_PKGS ?= ./test/e2e/...
E2E_RUN ?=

# Hugo version for the website. The site workflow reads this line, so the
# version is written once.
HUGO_VERSION := 0.170.1

SITE_DIR ?= docs/site

# Version whose released section in CHANGELOG.md the release notes check
# extracts.
RELEASE_NOTES_VERSION ?= v0.6.1

# govulncheck version for the vulncheck target, the latest release when the
# target was added.
GOVULNCHECK_VERSION := v1.10.0

.PHONY: gen gen-check lint release-notes-check archive-check third-party-check vulncheck proto-lint test test-root e2e e2e-darwin cover-report cover-check docs-check check site site-check

# Regenerate the committed generated artifacts: the protobuf code with buf and
# the man pages from the cobra command tree. Generated Go code is committed next
# to its .proto file, and the man pages under man/. This target is expected to
# leave the tree unchanged when the protos and the command tree agree with the
# committed output.
gen:
	buf generate
	bash scripts/gen-man.sh

# Regenerate the artifacts and fail when the working tree does not already
# contain exactly that output. scripts/gen-check.sh owns the comparison: it
# generates into a temporary directory and diffs the result against the
# committed code, so uncommitted hand-written Go files do not affect it.
gen-check:
	bash scripts/gen-check.sh

# Format, static analysis, shell script lint, module tidiness and the module
# dependency-direction check.
lint:
	@awk 'BEGIN { inrun = 0; found = 0 } \
		/^[^[:space:]#]/ { inrun = ($$0 ~ /^run:[[:space:]]*(#|$$)/) } \
		inrun && $$1 ~ /^allow-parallel-runners:/ { \
			v = $$0; \
			sub(/^[[:space:]]*allow-parallel-runners:[[:space:]]*/, "", v); \
			sub(/[[:space:]]+#.*$$/, "", v); \
			gsub(/"/, "", v); \
			sub(/[[:space:]]+$$/, "", v); \
			if (tolower(v) == "true") found = 1 \
		} \
		END { \
			if (!found) { \
				print "run.allow-parallel-runners must be true so concurrent make lint runs do not fail" > "/dev/stderr"; \
				exit 1 \
			} \
		}' .golangci.yml
	@if [ -n "$(GOLANGCI_LINT_CACHE)" ]; then mkdir -p "$(GOLANGCI_LINT_CACHE)/tmp"; fi
	@files="$$(find . -type f -name '*.go' \
		-not -path './third_party/*' \
		-not -path './.git/*' \
		-not -path './.claude/*')"; \
	if [ -n "$$files" ]; then \
		unformatted="$$(gofmt -l $$files)"; \
		if [ -n "$$unformatted" ]; then \
			printf '%s\n' "lint: gofmt found unformatted Go files:" "$$unformatted" >&2; \
			exit 1; \
		fi; \
	fi
	shellcheck scripts/install.sh scripts/remote-install.sh
	$(GOLANGCI_LINT) run ./...
	$(GOLANGCI_LINT) run --build-tags peers_testenv ./...
	$(GOLANGCI_LINT) run --build-tags e2e ./...
	GOOS=darwin $(GOLANGCI_LINT) run ./...
	GOOS=darwin $(GOLANGCI_LINT) run --build-tags e2e,e2e_darwin ./...
	go mod tidy -diff
	go run ./scripts/checksignals
	go run ./scripts/check-deps -goos linux
	go run ./scripts/check-deps -goos darwin
	go run ./scripts/check-deps -tags e2e
	go run ./scripts/check-workflows

# Check that the release notes extracted from CHANGELOG.md become the body of
# the release goreleaser publishes. The command runs a real release against a
# fake GitHub API; scripts/check-release-notes explains why a snapshot run
# cannot catch this regression.
release-notes-check:
	go run ./scripts/check-release-notes -version $(RELEASE_NOTES_VERSION)

# Build a snapshot and check that every archive holds exactly the release
# contents: the binary, install.sh, the archive README, the licenses and the
# third-party license inventory. The test hook string is covered by the
# goreleaser build hook, which fails the run before an archive is written.
archive-check:
	go run ./scripts/check-archives

# Compare every vendored library below third_party/ with the upstream module
# version third_party/UPSTREAM records, plus exactly the local changes it
# lists. The modules come through the Go module proxy and checksum database,
# so this target needs the network and is not part of make check.
third-party-check:
	go run ./scripts/check-third-party

# Scan the module graph for known vulnerabilities for both platforms the
# project supports. The vulnerability database is remote, so this target is
# not part of make check and runs as its own CI job. govulncheck is installed
# into a temporary directory first: GOOS must reach the go list it runs, not
# the build of govulncheck itself, or the scan would try to execute a darwin
# binary on the host.
vulncheck:
	@bin="$$(mktemp -d)"; trap 'rm -rf "$$bin"' EXIT HUP INT TERM; \
	GOBIN="$$bin" go install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) || exit $$?; \
	"$$bin/govulncheck" ./... || exit $$?; \
	GOOS=darwin "$$bin/govulncheck" ./...

proto-lint:
	buf lint

# The peers_testenv round discovers what to run instead of naming it. A
# package is included when its .GoFiles or .TestGoFiles differ under the tag,
# and a test function is included when it is declared in a file whose
# //go:build line requires peers_testenv without negating it. A new hook
# test is therefore executed without an edit here.
#
# go vet runs concurrently with the race-enabled tests, writing to its own
# temporary file so the two outputs do not interleave; vet's output is printed
# after the tests finish. A failure in either one fails the target.
test:
	go build ./...
	@$(call COVER_ROUND_PREP,unit)tmp="$$(mktemp -d)"; trap 'rm -rf "$$tmp"' EXIT HUP INT TERM; \
	go vet ./... > "$$tmp/vet.log" 2>&1 & \
	vetpid=$$!; \
	status=0; \
	if ! go build -o "$$tmp/testreport" ./scripts/testreport; then status=1; fi; \
	if [ $$status -eq 0 ]; then \
		bash -o pipefail -c 'go test -race -count=1 -json -p $(JOBS) -parallel $(JOBS)$(COVER_GO_FLAGS) ./...$(call COVER_GO_ARGS,unit) | '"$$tmp"'/testreport -label test -' || status=$$?; \
	fi; \
	wait "$$vetpid"; vetstatus=$$?; \
	cat "$$tmp/vet.log"; \
	if [ $$vetstatus -ne 0 ]; then echo "test: go vet failed" >&2; status=1; fi; \
	exit $$status
	@$(call COVER_ROUND_PREP,testenv)root="$$(mktemp -d)"; home="$$(mktemp -d)"; scratch="$$(mktemp -d)"; \
	cleanup() { status=$$1; rm -rf "$$root" "$$home" "$$scratch"; exit $$status; }; \
	go list -e -f '{{.ImportPath}}|{{join .GoFiles " "}}|{{join .TestGoFiles " "}}' ./... > "$$scratch/plain" || cleanup $$?; \
	go list -e -tags peers_testenv -f '{{.ImportPath}}|{{join .GoFiles " "}}|{{join .TestGoFiles " "}}' ./... > "$$scratch/tagged" || cleanup $$?; \
	pkgs="$$(diff "$$scratch/plain" "$$scratch/tagged" | sed -n 's/^[<>] \([^|]*\)|.*/\1/p' | sort -u)"; \
	$(GIT) ls-files '*.go' > "$$scratch/files" || cleanup $$?; \
	tests=""; \
	for file in $$(grep -v '^third_party/' "$$scratch/files"); do \
		if awk '/^package / { exit 1 } /^\/\/go:build / { if ($$0 ~ /peers_testenv/ && $$0 !~ /!peers_testenv/) exit 0; exit 1 }' "$$file"; then \
			for name in $$(sed -n 's/^func \(Test[A-Za-z0-9_]*\)(.*/\1/p' "$$file"); do \
				if [ -z "$$tests" ]; then tests="$$name"; else tests="$$tests|$$name"; fi; \
			done; \
		fi; \
	done; \
	if [ -z "$$pkgs" ] || [ -z "$$tests" ]; then \
		echo "test: no peers_testenv hook tests found"; \
		cleanup 0; \
	fi; \
	echo "test: peers_testenv packages:$$pkgs"; \
	echo "test: peers_testenv tests:^($$tests)$$"; \
	PEERS_TEST_RUNTIME_ROOT="$$root" PEERS_TEST_HOME="$$home" \
		go test -race -count=1 -json -tags peers_testenv -p $(JOBS) -parallel $(JOBS)$(COVER_GO_FLAGS) \
			-run "^($$tests)$$" $$pkgs$(call COVER_GO_ARGS,testenv) > "$$scratch/testenv.json"; \
	status=$$?; \
	go run ./scripts/testreport -label test-env "$$scratch/testenv.json" || cleanup $$?; \
	cleanup $$status
	@if [ "$$(go env GOOS)" = darwin ]; then \
		go -C $(CURDIR)/third_party/fsnotify test -race -count=1 -p $(JOBS) -parallel $(JOBS) -run '^Test(KqueueCloseDoesNotCloseAReusedDescriptor|AddWatchKeepsAnExistingDescriptorWhenReregisterFails|WriteAfterCreateIsReported|KqueueInternalReregisterKeepsUserFlags|KqueueInternalReregisterAfterUserAddKeepsFlags|KqueueUserAddSerializedAgainstInternalRegister|KqueueRemoveDuringCommitDoesNotRecordStaleDescriptor|KqueueUserAddFailsWhenRetriesAreExhausted|KqueueRetryAfterCloseReturnsErrClosed)$$' ./; \
	else \
		echo "skipping the kqueue backend tests: GOOS is not darwin"; \
	fi

# Tests that need root: they change file ownership or start a child as another
# account. Each test skips itself without root, so `make check` still passes on
# a developer account; CI runs this target under sudo. The three variables
# below name the default set; a caller may override them to run a different
# set, as the macOS job does for the two tests that need root there. The test
# names are asserted to pass, because a skip also makes `go test` succeed and
# would otherwise hide the regression the step exists to catch.
ROOT_TEST_PKGS ?= ./internal/broker/helper ./internal/pairing
ROOT_TEST_RUN ?= ^(TestAccessCheckChildKeepsRootRealIdentity|TestRootHelperReportsTargetIdentity|TestRootAccessCheckUsesTargetCredentials|TestIdentityRejectsForeignOwner)$$
ROOT_TEST_CASES ?= github.com/wangzw/peers/internal/broker/helper:TestAccessCheckChildKeepsRootRealIdentity github.com/wangzw/peers/internal/broker/helper:TestRootHelperReportsTargetIdentity github.com/wangzw/peers/internal/broker/helper:TestRootAccessCheckUsesTargetCredentials github.com/wangzw/peers/internal/pairing:TestIdentityRejectsForeignOwner

test-root:
	@tmp="$$(mktemp -d)"; trap 'rm -rf "$$tmp"' EXIT HUP INT TERM; \
	go build -o "$$tmp/testreport" ./scripts/testreport || exit $$?; \
	go test -race -count=1 -json -p $(JOBS) -parallel $(JOBS) -run '$(ROOT_TEST_RUN)' $(ROOT_TEST_PKGS) > "$$tmp/root.json"; \
	status=$$?; \
	"$$tmp/testreport" -label test-root "$$tmp/root.json" || status=$$?; \
	if [ $$status -eq 0 ]; then \
		for entry in $(ROOT_TEST_CASES); do \
			pkg="$${entry%%:*}"; name="$${entry#*:}"; \
			if grep -F '"Action":"pass"' "$$tmp/root.json" | grep -F "\"Package\":\"$$pkg\"" | grep -qF "\"Test\":\"$$name\""; then \
				echo "test-root: $$name passed"; \
			else \
				echo "test-root: $$name did not pass under root" >&2; \
				status=1; \
			fi; \
		done; \
	fi; \
	exit $$status

# The e2e run cleans up after itself even when go test is killed or times out.
# The cleanup is TestMain's: a no-test run removes the projects, networks,
# processes, images and runtime roots whose owner process is gone, and leaves a
# live suite's resources alone. Image tags carry the owner marker, so this does
# not remove an image a parallel suite still uses.
#
# A systemd container that did not boot is rebuilt at most once; the rebuild
# appends a line to SYSTEMD_REBUILD_LEDGER (see
# test/e2e/syscontainer/syscontainer.go), which this target prints and then
# empties so a gate without -v still shows how often and why it happened.
SYSTEMD_REBUILD_LEDGER ?= /tmp/ape2e-systemd-rebuilds.log

# With COVER_DIR set to an absolute path, the run collects e2e coverage into
# $(COVER_DIR)/e2e and exports E2E_COVER_DIR for the harness. The directory is
# world-writable and carries a default ACL for the invoking uid, because the
# instrumented processes run in containers and write under UMask=0077; their
# counter files then stay readable without sudo. That needs setfacl from the
# acl package. A standalone make e2e with COVER_DIR= collects no coverage,
# and E2E_COVER_DIR is cleared so a value inherited from the environment cannot
# turn it back on; make check rejects COVER_DIR= at its own guard instead.
# The directory starts at 0700 and is widened to 0777 only after setfacl
# succeeds, and a failed preparation removes it, so a failure leaves no
# world-writable directory behind. When the round ends, success or failure, the
# directory loses group and other write before cover-check reads it, and the
# exit trap does the same if the run is interrupted.
e2e:
	@tmp="$$(mktemp -d)"; \
	status=0; \
	coverdir='$(COVER_DIR)'; \
	cleanup() { rm -rf "$$tmp"; case "$$coverdir" in /*) if [ -d "$$coverdir/e2e" ]; then chmod go-w "$$coverdir/e2e" 2>/dev/null || true; fi ;; esac; }; \
	trap cleanup EXIT HUP INT TERM; \
	if [ -n '$(COVER_DIR)' ]; then \
		case '$(COVER_DIR)' in /*) ;; *) echo "e2e: COVER_DIR must name an absolute path (got '$(COVER_DIR)')" >&2; exit 2 ;; esac; \
		if ! command -v setfacl >/dev/null 2>&1; then \
			echo "e2e: COVER_DIR is set but setfacl is missing; install the acl package (for example sudo apt-get install -y acl). make check collects coverage and needs setfacl; to run the e2e suite alone without coverage, run make e2e with COVER_DIR unset." >&2; \
			exit 1; \
		fi; \
		rm -rf '$(COVER_DIR)/e2e' && mkdir -p '$(COVER_DIR)/e2e' && chmod 0700 '$(COVER_DIR)/e2e' && setfacl -d -m u:$$(id -u):rwX '$(COVER_DIR)/e2e' && chmod 0777 '$(COVER_DIR)/e2e' \
			|| { rm -rf '$(COVER_DIR)/e2e'; echo "e2e: preparing '$(COVER_DIR)/e2e' failed" >&2; exit 1; }; \
		export E2E_COVER_DIR='$(COVER_DIR)/e2e'; \
	else \
		unset E2E_COVER_DIR; \
	fi; \
	runarg=""; \
	if [ -n '$(E2E_RUN)' ]; then runarg="-run '$(E2E_RUN)'"; fi; \
	if go build -o "$$tmp/testreport" ./scripts/testreport; then \
		bash -o pipefail -c 'go test -tags e2e -count=1 -timeout $(E2E_TIMEOUT) -json -p $(JOBS) -parallel $(JOBS) '"$$runarg"' $(E2E_PKGS) | '"$$tmp"'/testreport -label e2e -progress -' || status=$$?; \
	else \
		status=$$?; \
	fi; \
	if [ -s $(SYSTEMD_REBUILD_LEDGER) ]; then \
		echo "e2e: systemd container rebuilds:"; \
		cat $(SYSTEMD_REBUILD_LEDGER); \
		: > $(SYSTEMD_REBUILD_LEDGER); \
	fi; \
	go test -tags e2e -count=1 -run '^$$' ./test/e2e/ >/dev/null 2>&1 || true; \
	if [ -n '$(COVER_DIR)' ] && [ -d '$(COVER_DIR)/e2e' ]; then \
		chmod go-w '$(COVER_DIR)/e2e' || { echo "e2e: removing write access from group and other on '$(COVER_DIR)/e2e' failed" >&2; if [ $$status -eq 0 ]; then status=1; fi; }; \
	fi; \
	exit $$status

# Merge the producer directories COVER_INPUTS names under COVER_DIR and print
# the coverage table. cover-check is the same report with the COVER_TOTAL and
# COVER_PACKAGE floors enforced, so a shortfall exits non-zero; it is the gate
# make check runs after e2e. Only the report reaches stdout, which lets CI
# append it to the step summary. Both need data the producers wrote first, and
# an empty COVER_DIR fails before coverreport runs. Both remove the previous
# profile first, so a run that fails leaves no stale coverage.txt behind.
cover-report:
	@if [ -z '$(COVER_DIR)' ]; then echo "cover-report: COVER_DIR is empty; set it to the absolute directory the producers wrote, for example COVER_DIR=\$$PWD/.cover" >&2; exit 2; fi; \
	rm -f '$(COVER_DIR)/coverage.txt'; \
	go run ./scripts/coverreport -i '$(COVER_INPUT_DIRS)' -root '$(CURDIR)' -o '$(COVER_DIR)/coverage.txt' -format '$(COVER_FORMAT)'

cover-check:
	@if [ -z '$(COVER_DIR)' ]; then echo "cover-check: COVER_DIR is empty; set it to the absolute directory the producers wrote, for example COVER_DIR=\$$PWD/.cover" >&2; exit 2; fi; \
	rm -f '$(COVER_DIR)/coverage.txt'; \
	go run ./scripts/coverreport -i '$(COVER_INPUT_DIRS)' -root '$(CURDIR)' -o '$(COVER_DIR)/coverage.txt' -format '$(COVER_FORMAT)' -total '$(COVER_TOTAL)' -package '$(COVER_PACKAGE)'

# The macOS service end-to-end tests exercise launchd on the host, so they run
# on macOS only and use the real launchctl and sudo. The system and
# user tests run serially because both use the same launchd label.
e2e-darwin:
	@if [ "$$(go env GOOS)" != darwin ]; then \
		echo "e2e-darwin: requires macOS (go env GOOS is $$(go env GOOS))" >&2; \
		exit 1; \
	fi
	go test -v -tags e2e_darwin -count=1 -timeout $(E2E_TIMEOUT) -p 1 -parallel 1 ./test/e2e/darwinservice

# Document standards plus a render of every Mermaid diagram in the docs that
# differ from origin/main. scripts/docs-check.sh owns the details, including
# how the two halves behave when their tooling or the comparison base is
# missing.
docs-check:
	bash scripts/docs-check.sh

# Preview the website locally.
site:
	@if ! command -v hugo >/dev/null 2>&1; then \
		echo "site: hugo is not installed; install Hugo $(HUGO_VERSION)" >&2; \
		exit 1; \
	fi
	@release="$$(git tag --list 'v*' --sort=-v:refname | grep -Ex 'v[0-9]+\.[0-9]+\.[0-9]+' | head -n 1)"; \
	if [ -z "$$release" ]; then \
		echo "site: no vX.Y.Z tag found; fetch the tags (git fetch --tags) so params.release can be set" >&2; \
		exit 1; \
	fi; \
	HUGO_PARAMS_RELEASE="$${release#v}" hugo server --source $(SITE_DIR)

# Check that the English and Chinese pages match, that no page writes a bare
# release number that equals a vX.Y.Z tag, build the website strictly, check
# that every generated index.html has an index.md next to it, check the
# description and sharing metadata of every page and the lastmod of every
# sitemap entry, and check the internal links of the generated pages. The link check uses the
# generated tree as its root, so a root-relative link resolves inside the
# site; page relative links resolve against the file that contains them. The
# build cleans the destination first, so a page that was deleted or renamed
# cannot keep passing the link check through its stale output.
site-check:
	@missing=""; \
	for tool in hugo lychee go; do \
		command -v $$tool >/dev/null 2>&1 || missing="$$missing $$tool"; \
	done; \
	if [ -n "$$missing" ]; then \
		echo "site-check: missing tools:$$missing (Hugo $(HUGO_VERSION))" >&2; \
		exit 1; \
	fi; \
	have="$$(hugo version 2>/dev/null | sed -n 's/^hugo v\([^ ]*\).*/\1/p')"; \
	case "$$have" in \
		"$(HUGO_VERSION)"|"$(HUGO_VERSION)"-*) ;; \
		*) echo "site-check: Hugo $(HUGO_VERSION) required, found '$$have'" >&2; exit 1 ;; \
	esac
	go run -p $(JOBS) ./scripts/site-parity -en $(SITE_DIR)/content/en -zh $(SITE_DIR)/content/zh
	go run -p $(JOBS) ./scripts/site-release-number -content $(SITE_DIR)/content -repo $(CURDIR)
	@release="$$(git tag --list 'v*' --sort=-v:refname | grep -Ex 'v[0-9]+\.[0-9]+\.[0-9]+' | head -n 1)"; \
	if [ -z "$$release" ]; then \
		echo "site-check: no vX.Y.Z tag found; fetch the tags (git fetch --tags) so params.release can be set" >&2; \
		exit 1; \
	fi; \
	GOMAXPROCS=$(JOBS) HUGO_PARAMS_RELEASE="$${release#v}" hugo --source $(SITE_DIR) --cleanDestinationDir --gc --minify --panicOnWarning
	@missing="$$(find $(SITE_DIR)/public -name index.html | sort | while read -r page; do \
		[ -f "$${page%.html}.md" ] || echo "$$page"; \
	done)"; \
	if [ -n "$$missing" ]; then \
		echo "site-check: pages without a Markdown rendition (index.md):" >&2; \
		echo "$$missing" >&2; \
		exit 1; \
	fi
	go run -p $(JOBS) ./scripts/site-meta -public $(SITE_DIR)/public
	lychee --offline --no-progress --max-concurrency $(JOBS) --root-dir "$(CURDIR)/$(SITE_DIR)/public" '$(SITE_DIR)/public/**/*.html'

# Run every gate in a fixed order. Each gate is a separate recursive make so
# the order also holds when the caller passes -j. The guard runs before any
# gate: an empty CHECK_COVER_DIR means the caller passed COVER_DIR=, and a
# relative one would otherwise fail in the test round only after lint,
# proto-lint and gen-check; both are rejected here. The test and e2e runs
# collect coverage into CHECK_COVER_DIR, which cover-check then enforces with
# the locked defaults; that collection makes them need setfacl from the acl
# package. The guard line carries '+' so make -n executes it too.
check:
	+@if [ -z '$(CHECK_COVER_DIR)' ]; then echo "check: COVER_DIR is empty; make check collects coverage and needs a directory, so pass an absolute COVER_DIR or leave COVER_DIR unset to use $(CURDIR)/.cover" >&2; exit 2; fi; \
	case '$(CHECK_COVER_DIR)' in /*) ;; *) echo "check: COVER_DIR must name an absolute path (got '$(CHECK_COVER_DIR)')" >&2; exit 2 ;; esac
	$(MAKE) --no-print-directory lint
	$(MAKE) --no-print-directory proto-lint
	$(MAKE) --no-print-directory gen-check
	$(MAKE) --no-print-directory test COVER_DIR='$(CHECK_COVER_DIR)'
	$(MAKE) --no-print-directory e2e COVER_DIR='$(CHECK_COVER_DIR)'
	$(MAKE) --no-print-directory cover-check COVER_DIR='$(CHECK_COVER_DIR)' COVER_TOTAL='$(CHECK_COVER_TOTAL)' COVER_PACKAGE='$(CHECK_COVER_PACKAGE)' COVER_INPUTS='$(CHECK_COVER_INPUTS)'
	$(MAKE) --no-print-directory docs-check
