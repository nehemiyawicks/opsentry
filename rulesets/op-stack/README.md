# OP-Stack ruleset pack

Monitors for the system-layer contracts every OP-Stack rollup depends on. These are the invariants a rollup operator or a Superchain observer would want alerted on.

Two of the three files monitor L1 (Ethereum mainnet, chain id 1) because that's where the OptimismPortal and SystemConfig proxies live. The third monitors L2 (Base, chain id 8453 in the example) because `L2ToL1MessagePasser` is an L2 predeploy at `0x4200000000000000000000000000000000000016`.

For OP Mainnet (chain id 10) or any other OP-Stack chain, swap the L1 addresses (OptimismPortal proxy, SystemConfig proxy) for that chain's entries from [superchain-registry](https://github.com/ethereum-optimism/superchain-registry). The L2 predeploy address is the same on every OP-Stack chain.

## Files

### `base-portal-l1.yaml` — Base's OptimismPortal on L1

Watches the OptimismPortal proxy for Base at `0x49048044D57e1C92A77f79988d21Fa8fAF74E97e` on Ethereum mainnet. Fires on:

- **`Paused`** → severity `critical`. Somebody hit the emergency stop on Base's L1-to-L2 message pipe. Real ops incident.
- **`Unpaused`** → severity `critical`. The stop was released. Confirms who and when.
- **`TransactionDeposited` + `state.paused == true`** → severity `critical`. **Invariant violation**: the contract should refuse deposits while paused, so this event firing with the pause bit set means either a contract bug or the check was somehow bypassed. Uses the new `reads` config to read `paused()` via `eth_call` at each block, then references it in the rule expression as `event.state.paused`.

### `base-system-config-l1.yaml` — Base's SystemConfig on L1

Watches the SystemConfig proxy for Base at `0x73a79Fab69143498Ed3712e519A88a918e1f4072` on Ethereum mainnet. Fires on:

- **Any `ConfigUpdate`** → severity `critical`. Batcher key rotation, gas limit change, fee scalar change, unsafe block signer swap. These are always intentional protocol changes and always worth catching.

### `l2-message-passer.yaml` — L2ToL1MessagePasser on L2

Watches the `L2ToL1MessagePasser` predeploy at `0x4200000000000000000000000000000000000016` on L2. Fires on:

- **`MessagePassed`** with `value > 100 ETH` → severity `high`. Large native-ETH withdrawal from the rollup. Not necessarily malicious but worth attention.

## Address verification

Base's L1 addresses were sourced from [superchain-registry](https://github.com/ethereum-optimism/superchain-registry/blob/main/superchain/configs/mainnet/base.toml). L2 predeploys are documented in the [OP Stack specs](https://specs.optimism.io/protocol/predeploys.html).

Verify against those upstream sources before running these in production. A wrong OptimismPortal address means you silently miss a real pause.

## RPC notes

The example configs use public RPC endpoints (`https://mainnet.base.org`, `https://eth.llamarpc.com`). Public RPCs are fine for a demo but rate-limit under real monitoring load. Swap in an Alchemy, Infura, or QuickNode URL before production.
