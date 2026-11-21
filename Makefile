# Build gates for peers.
#
# Every parallel-capable target takes its job count from JOBS, which the caller
# sets explicitly (default 4). Nothing here probes the host's CPU count.

JOBS ?= 4

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
RELEASE_NOTES_VERSION ?= v0.5.0

.PHONY: gen gen-check lint release-notes-check archive-check proto-lint test test-root e2e e2e-darwin docs-check check site site-check

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

proto-lint:
	buf lint

# The peers_testenv round discovers what to run instead of naming it. A
# package is included when its .GoFiles or .TestGoFiles differ under the tag,
# and a test function is included when it is declared in a file whose
# //go:build line requires peers_testenv without negating it. A new hook
# test is therefore executed without an edit here.
test:
	go build ./...
	go vet ./...
	@tmp="$$(mktemp -d)"; trap 'rm -rf "$$tmp"' EXIT HUP INT TERM; \
	go build -o "$$tmp/testreport" ./scripts/testreport || exit $$?; \
	bash -o pipefail -c 'go test -race -count=1 -json -p $(JOBS) -parallel $(JOBS) ./... | '"$$tmp"'/testreport -label test -'
	@root="$$(mktemp -d)"; home="$$(mktemp -d)"; scratch="$$(mktemp -d)"; \
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
		go test -race -count=1 -json -tags peers_testenv -p $(JOBS) -parallel $(JOBS) \
			-run "^($$tests)$$" $$pkgs > "$$scratch/testenv.json"; \
	status=$$?; \
	go run ./scripts/testreport -label test-env "$$scratch/testenv.json" || cleanup $$?; \
	cleanup $$status
	@if [ "$$(go env GOOS)" = darwin ]; then \
		go -C $(CURDIR)/third_party/fsnotify test -race -count=1 -p $(JOBS) -parallel $(JOBS) -run '^Test(KqueueCloseDoesNotCloseAReusedDescriptor|AddWatchKeepsAnExistingDescriptorWhenReregisterFails|WriteAfterCreateIsReported)$$' ./; \
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

e2e:
	@tmp="$$(mktemp -d)"; trap 'rm -rf "$$tmp"' EXIT HUP INT TERM; \
	status=0; \
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
	exit $$status

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
	hugo server --source $(SITE_DIR)

# Check that the English and Chinese pages match, build the website strictly
# and check the internal links of the generated pages. The link check maps the
# /peers/ prefix onto the generated tree through a temporary root; page
# relative links still resolve against the file that contains them. The build
# cleans the destination first, so a page that was deleted or renamed cannot
# keep passing the link check through its stale output.
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
	GOMAXPROCS=$(JOBS) hugo --source $(SITE_DIR) --cleanDestinationDir --gc --minify --panicOnWarning
	@root="$$(mktemp -d)"; trap 'rm -rf "$$root"' EXIT; \
		ln -s "$(CURDIR)/$(SITE_DIR)/public" "$$root/peers"; \
		lychee --offline --no-progress --max-concurrency $(JOBS) --root-dir "$$root" '$(SITE_DIR)/public/**/*.html'

# Run every gate in a fixed order. Each gate is a separate recursive make so
# the order also holds when the caller passes -j.
check:
	$(MAKE) --no-print-directory lint
	$(MAKE) --no-print-directory proto-lint
	$(MAKE) --no-print-directory gen-check
	$(MAKE) --no-print-directory test
	$(MAKE) --no-print-directory e2e
	$(MAKE) --no-print-directory docs-check
