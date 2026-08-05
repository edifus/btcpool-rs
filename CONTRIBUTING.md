# Contributing to btcpool-rs

Thanks for your interest in improving btcpool-rs! This guide covers how to set
up, the conventions the project follows, and what a pull request needs to pass.

## Reporting issues

- **Bugs and feature requests:** open a GitHub issue. Include your version
  (`btcpool-rs --version` or the release tag), relevant config (redact your
  address/credentials), and log excerpts.
- **Security vulnerabilities:** do **not** open a public issue. Follow the
  private reporting process in [SECURITY.md](SECURITY.md).

## Development setup

You need a Rust toolchain and the ZMQ library (the `tmq` → `zmq` dependency
links against `libzmq`):

```bash
# Debian/Ubuntu
sudo apt-get install -y libzmq3-dev pkg-config

# build & test
cargo build
cargo test --all
RUST_LOG=debug cargo run -- config.toml
```

The minimum supported Rust version (MSRV) is **1.75.0** (edition 2021).

## Before you open a PR

CI runs the following on every pull request, with `RUSTFLAGS=-D warnings` — all
must pass, so run them locally first:

```bash
cargo fmt --all -- --check              # formatting
cargo clippy --all-targets --all-features   # lints (warnings are errors in CI)
cargo test --all                        # tests
cargo build --release                   # release build
```

## Commit messages

The project uses [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<optional scope>): <summary>
```

Types in use here include `feat`, `fix`, `docs`, `deps`, and `release`. Examples
from the history:

- `fix(jobs): seed job-id high bits per process to avoid post-restart stale mislabel`
- `feat(dashboard): embed favicon, serve at /favicon.ico`
- `docs: real security policy (supported versions, private reporting, scope)`

Keep the summary in the imperative mood and explain the *why* in the body when
it isn't obvious.

## Pull request workflow

1. Branch off `main` (e.g. `fix/stale-share-label` or `feat/sv2-noise`).
2. Make your change, keeping commits focused.
3. Add an entry under the `[Unreleased]` section of
   [CHANGELOG.md](CHANGELOG.md) describing the change (Added / Changed / Fixed).
4. Push your branch and open a PR against `main`. Describe what changed and how
   you tested it.
5. Make sure CI is green. Once reviewed and approved, the PR is merged into
   `main`.

Releases are cut separately from merges — see the **Releasing** section of the
[README](README.md#releasing).

## License

By contributing, you agree that your contributions will be dual-licensed under
the same terms as the project: [MIT](LICENSE-MIT) OR [Apache-2.0](LICENSE-APACHE).
