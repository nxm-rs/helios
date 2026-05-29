<p align="center">
  <img src=".github/banner.svg" alt="Nexum · helios — in-process Ethereum light client" width="100%" />
</p>

# Nexum · helios

The **nxm-rs fork** of [a16z/helios](https://github.com/a16z/helios) — an in-process Ethereum light client that converts an untrusted centralised RPC into an unmanipulable local one by independently verifying every value against beacon-chain sync-committee signatures.

This fork exists because the [Nexum wallet](https://github.com/nxm-rs/wallet) embeds Helios directly in its Rust core and needs a few changes that aren't yet upstream:

- `jsonrpc-server` made a feature flag so library consumers don't pull the binary stack ([#12](https://github.com/nxm-rs/helios/pull/12))
- `reqwest` defaults trimmed (no `charset`, no system proxy) — meaningful binary-size win on mobile ([#14](https://github.com/nxm-rs/helios/pull/14))
- `opstack` libp2p bump cherry-picked ahead of upstream ([#10](https://github.com/nxm-rs/helios/pull/10))

We track upstream `master` and intend to upstream changes where they make sense for a16z. Use this fork if you're embedding Helios inside Nexum or another constrained mobile/WASM target; use upstream for the standalone CLI.

> Looking for the org overview? See **[github.com/nxm-rs](https://github.com/nxm-rs)**.

---

## What this binary does

Helios is a trustless, portable, multichain light client written in Rust. It syncs in seconds, requires no storage, and is small enough to run on a phone.

Supported chains in this workspace:

- Ethereum mainnet (`ethereum/`)
- OP Stack — op-mainnet, Base (`opstack/`)
- Linea (`linea/`)

The full upstream usage docs (installer, CLI flags, RPC method support) are valid against this fork and live at **[a16z/helios](https://github.com/a16z/helios)** — see [`docs/`](./docs) and [`rpc.md`](./rpc.md) for the API surface.

---

## Build from source

```bash
git clone https://github.com/nxm-rs/helios
cd helios
cargo build --release
```

Binary lands at `target/release/helios`. To run against Ethereum mainnet:

```bash
helios ethereum --execution-rpc $ETH_RPC_URL
```

`$ETH_RPC_URL` must be a provider supporting `eth_getProof` (Alchemy, Infura, etc.). Helios then exposes a local verified RPC at `http://127.0.0.1:8545`.

---

## Embedding (the Nexum use case)

The Nexum mobile wallet depends on three crates from this fork, pinned exactly:

```toml
helios-ethereum = { git = "https://github.com/nxm-rs/helios", rev = "..." }
helios-common   = { git = "https://github.com/nxm-rs/helios", rev = "..." }
helios-core     = { git = "https://github.com/nxm-rs/helios", rev = "...", default-features = false }
```

We deliberately depend on `helios-ethereum` directly rather than the `helios` meta-crate, because the meta-crate pulls `helios-opstack`'s libp2p chain — which transitively hits a yanked `core2 0.4.0` in 0.11.1. If you embed Helios in a no-libp2p setting (mobile, WASM), do the same.

---

## Workspace layout

```
helios/
├── cli/                 ← `helios` binary, multi-chain dispatch
├── common/              ← shared types (network, errors)
├── core/                ← chain-agnostic light-client core
├── ethereum/            ← mainnet client (consensus + execution)
│   └── consensus-core/
├── opstack/             ← OP Stack chains (op-mainnet, base, …)
├── linea/               ← Linea client
├── helios-ts/           ← TypeScript / WASM bindings
├── verifiable-api/      ← server + client + types for the verified-RPC bridge
├── revm-utils/          ← revm helpers shared across chains
├── benches/             ← criterion benches
├── tests/               ← integration tests + utilities
└── heliosup/            ← upstream installer (we ship this too)
```

---

## Contributing

This fork is intentionally small. Net-new features should go upstream first; this repo carries downstream patches and pre-upstream cherry-picks.

If you're contributing a fix that's specific to the Nexum embedding (mobile / WASM / no-libp2p), open a PR here. For chain-protocol changes or general improvements, please open them on [a16z/helios](https://github.com/a16z/helios) — we'll pull them back in.

Conventional Commits required (`feat:`, `fix:`, `chore(deps):`, etc.). PR description should call out whether the change is intended to go upstream.

## Security

See [SECURITY.md](https://github.com/nxm-rs/.github/blob/main/SECURITY.md) on the org `.github` repo. For Helios-specific findings (sync-committee verification, proof handling, fork-detection), reporting via GitHub Security Advisories on this repo is preferred. For findings that originate in upstream code unchanged by this fork, please disclose to a16z as well.

## License

MIT — inherited from upstream a16z/helios. See [LICENSE](./LICENSE).

```
●  fork of a16z/helios  ·  embeddable in mobile/WASM  ·  upstream-track
```
