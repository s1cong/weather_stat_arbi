# weather_stat_arbi

A Rust bot for multi-outcome event arbitrage on Polymarket, focused on daily temperature events.

## Features

1. **Event discovery by tags**: finds daily temperature events through Polymarket tags and refreshes the list on a set interval.
2. **Fast order book tracking**: tracks the best asks for every active daily temperature market and keeps them updated through WebSocket push.
3. **Batch order placement**: when an arbitrage shows up on a multi-outcome event, it places the orders for all outcomes in one batch.

## How it works

In a multi-outcome event, exactly one outcome resolves to YES. If the best asks across all outcomes add up to less than 1, buying every outcome locks in a profit. The bot trades when that sum is below a set threshold, which leaves room for fees and slippage.

## Warning

Order placement is not atomic. The bot sends all legs in one batch, but each leg can fill or not on its own. If some legs fill and others don't, the unfilled ones are left on the order book as resting limit orders. Until they fill or you cancel them, you're holding a partial position with real risk, not a locked-in arbitrage. Watch your open orders.
