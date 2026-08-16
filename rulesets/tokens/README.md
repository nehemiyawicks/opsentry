# Token ruleset pack

Ready-to-run monitors for high-volume ERC-20 tokens. Same pattern for any ERC-20: `abi: erc20` gets you `Transfer` and `Approval` events without needing to fetch or bundle an ABI.

## Files

### `usdc-base.yaml`

Watches native USDC at `0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913` on Base. Fires:

- **Transfer > 100,000 USDC** → severity `high`. Whale movement; useful for treasuries and market makers who want an early view of large flows.

Threshold is in USDC's raw units (6 decimals). Adjust `100000e6` in the rule to change the threshold.

### `weth-base.yaml`

Watches Base WETH at `0x4200000000000000000000000000000000000006` on Base. Fires:

- **Transfer > 100 WETH** → severity `high`. Same whale-movement pattern as USDC, but WETH-denominated.

Threshold is in wei (18 decimals). Adjust `100e18` in the rule to change.

## Adapting for other tokens

Any ERC-20 works: copy `usdc-base.yaml`, change:

- `address` to the token contract
- `chain` and `chain_id` to the token's chain
- The threshold value and units in the rule

`abi: erc20` decodes `Transfer(address indexed from, address indexed to, uint256 value)` without fetching, so no Sourcify hit and no proxy detection needed.

## RPC

Public RPCs (`https://mainnet.base.org`) work for a demo. For real ongoing monitoring, use a provider (Alchemy / Infura / QuickNode / your own node) to avoid rate limits.
