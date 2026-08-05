# Security Policy

`btcpool-rs` is a solo Bitcoin mining pool. It talks to a trusted `bitcoind`
over authenticated RPC and listens for miner connections (Stratum V1 in the
clear, Stratum V2 over a Noise-encrypted channel). It does **not** custody funds
— block rewards are paid directly to the `coinbase_address` in your config — but
a compromise could still misdirect rewards, leak the node's RPC credentials, or
crash the pool, so vulnerability reports are taken seriously.

## Supported Versions

This is a pre-1.0 project; only the latest minor release line receives security
fixes. Older lines are not patched — upgrade to the current release.

| Version | Supported          |
| ------- | ------------------ |
| 0.2.x   | :white_check_mark: |
| < 0.2   | :x:                |

## Reporting a Vulnerability

**Please do not open a public issue for security problems.**

Report privately via GitHub's private vulnerability reporting:
**[Security → Report a vulnerability](https://github.com/edifus/btcpool-rs/security/advisories/new)**
(Enable it under *Settings → Code security → Private vulnerability reporting* if
the link 404s.)

Include where you can:

- affected version / commit and platform,
- a description of the issue and its impact,
- steps to reproduce or a proof of concept,
- any suggested fix.

### What to expect

This project is maintained by one person in their spare time, so timelines are
best-effort, not guarantees:

- **Acknowledgement:** within ~7 days.
- **Assessment:** a severity call and whether it's accepted or declined, with
  reasoning, after triage.
- **Fix & disclosure:** accepted vulnerabilities are fixed on the supported
  release line and disclosed via a GitHub Security Advisory once a fix is
  available. Credit is given to reporters who want it.

## Scope

In scope: the pool daemon and its handling of RPC, Stratum (V1/V2), the Noise
handshake, share/block validation, the coinbase/template builder, and the
metrics/dashboard endpoint.

Out of scope: vulnerabilities in Bitcoin Core/Knots itself, your node or host
configuration, and exposing the dashboard/RPC to untrusted networks (bind them
to localhost or a trusted LAN — they are not hardened for public exposure).

## Dependencies

Dependency advisories are tracked with Dependabot and
[`cargo audit`](https://github.com/rustsec/rustsec) against the committed
`Cargo.lock`. If you find a vulnerable dependency, a report (or a PR bumping the
lockfile) is welcome.
