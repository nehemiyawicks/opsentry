# Aave V3 ruleset pack

Monitors for the Aave V3 lending pool. The mission spec called this pack out by name as an M2 deliverable.

## Files

### `pool-base.yaml`: Aave V3 Pool on Base

Watches Aave V3's `Pool` proxy at `0xA238Dd80C259a72e81d7e4664a9801593F98d1c5` on Base. Fires on:

- **`LiquidationCall`** → severity `high`. Someone's position was liquidated. Rare enough per-user that surfacing each one is useful for lenders monitoring reserve health.

Additional events are decoded but not thresholded by default (rulesets ship quiet; you tune them). Uncomment or add rules for:

- **`Borrow`** with a size threshold for tracking whale positions.
- **`Supply`** / **`Withdraw`** for reserve utilization changes.
- **`ReserveDataUpdated`** for rate-change alerts (fires frequently under load).

## A note on thresholds

Aave reserves have per-asset decimals (USDC 6, WETH 18, WBTC 8, etc.), so a numeric threshold on `event.params.amount` isn't reserve-agnostic. Either:

- Filter by `event.params.reserve == "0x<usdc-address>" && event.params.amount > 1000000000000` (per-reserve rules), or
- Look up the reserve's decimals via a state read (`reads:` config, upcoming) and compare a normalized value.

For the demo, the LiquidationCall rule doesn't need a threshold and fires per liquidation.

## Address verification

Aave V3 canonical deployment addresses live at [docs.aave.com](https://aave.com/docs/resources/addresses).
