gh issue create -R "julianstephens/waldb" \
  --title "Scaffold repo skeleton + CI/CD pipeline (format/lint/test + release)" \
  --body "$(cat <<'EOF'
## Summary
Scaffold the `waldb` repository with a clean Go project structure and a complete CI/CD pipeline. This issue is intended for an agent to implement end-to-end.

Goals:
- Standard repo layout for a Go CLI + internal library
- Consistent formatting + linting
- Reliable unit test execution
- CI validation on PR
- Automated releases on tag push with build artifacts

Non-goals:
- Implement WALDB functionality (covered by other issues)
- Over-engineered build systems

---

## Requirements

### 1) Repo structure
Create a sane Go module layout:

```
waldb/
cmd/
waldb/
main.go
internal/
waldb/          # library package (core DB implementation lives here later)
cli/            # command handlers (thin)
docs/
design.md       # placeholder; filled by design issues
testing.md      # placeholder; filled by test-plan issue
scripts/
.github/
workflows/
pr.yml
release.yml
Makefile (optional)
go.mod
go.sum
README.md
LICENSE (MIT unless otherwise specified)

```

Notes:
- `cmd/waldb` should compile to a working binary that prints help.
- `internal/waldb` should expose a placeholder `Open()` and `Close()` so downstream compilation is stable.

### 2) CLI framework
Use Go’s standard library for CLI parsing (flag) OR a lightweight library already used elsewhere in your ecosystem (if you have a preference). Avoid adding many deps. Primary requirement: `waldb --help` works and subcommands are stubbed.

Stub commands (no real DB work yet, but commands should parse/route cleanly):
- `waldb init`
- `waldb get`
- `waldb put`
- `waldb del`
- `waldb batch`
- `waldb snapshot`
- `waldb stats`
- `waldb doctor`
- `waldb repair`

Each can return “not implemented” with exit code 2 for now, but must have help text and argument validation.

### 3) Formatting + linting
- `gofmt` enforced
- `go vet` enforced
- Add `golangci-lint` with a minimal, sane config:
  - enable: `govet`, `errcheck`, `staticcheck`, `ineffassign`, `unused`, `gosec` (optional), `revive` (optional)
  - avoid noisy/low-value linters unless you already use them elsewhere
- Add `.golangci.yml` at repo root

### 4) Testing
- `go test ./...` runs in CI
- Add at least one trivial unit test (e.g. placeholder `TestOpenClose` in `internal/waldb`) so the pipeline proves it’s wired.
- Ensure tests run with `-race` on CI (Linux), if feasible.

### 5) CI on PR (GitHub Actions)
Create `.github/workflows/pr.yml` that runs on:
- `pull_request`
- `push` to default branch

Steps:
1. Checkout
2. Setup Go (pin version via `actions/setup-go`)
3. `gofmt` check (fail if diff)
4. `go vet ./...`
5. `golangci-lint run`
6. `go test ./...` (include `-race` on linux)

### 6) Release on tag push
Create `.github/workflows/release.yml` that runs on:
- `push` tags matching `v*.*.*`

Behavior:
- Build binaries for:
  - linux/amd64
  - linux/arm64
  - darwin/amd64
  - darwin/arm64
  - windows/amd64
- Embed version info:
  - via `-ldflags "-X main.version=$TAG -X main.commit=$SHA -X main.date=$DATE"`
- Create GitHub Release and upload artifacts
  - Use `softprops/action-gh-release` or equivalent
- Artifacts should be named consistently, e.g.:
  - `waldb_${VERSION}_linux_amd64.tar.gz` etc.
  - windows zip

### 7) Developer ergonomics
Add `README.md` with:
- install/build instructions
- how to run lint/test locally
- how releases work (tagging)

Optional but nice:
- `make lint`, `make test`, `make fmt`, `make build`

---

## Acceptance Criteria
- `go test ./...` passes locally and in CI
- PR workflow enforces formatting, lint, and tests
- Release workflow produces multi-platform artifacts on tag push
- `waldb --help` works, subcommands are wired with stub handlers
- Repo structure matches the requirements above (minor variations ok if justified)

---

## Notes for the agent
- Keep dependencies minimal.
- Prefer standard tooling over bespoke scripts.
- Document any non-obvious choices in the README.
EOF
)"

