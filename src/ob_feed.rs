use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use ordered_float::OrderedFloat;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::{broadcast, mpsc};
use tokio::time::{sleep, Duration, Instant};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, warn};

use crate::types::ObUpdate;

const WS_MARKET_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const SHARD_COUNT: usize = 10;
const OB_WS_LEVEL: u8 = 3;

#[derive(Debug, Default)]
struct ObFeedStats {
    book_messages: usize,
    subscribed_book_messages: usize,
    subscribed_book_messages_with_asks: usize,
    duplicate_book_messages: usize,
    unsubscribed_book_messages: usize,
    price_changes: usize,
    price_changes_before_book: usize,
    ob_updates_emitted: usize,
}

#[derive(Debug, Default)]
struct OrderBook {
    asks: BTreeMap<OrderedFloat<f64>, f64>,
    last_best_ask: Option<(f64, f64)>,
    initialized: bool,
}

impl OrderBook {
    fn replace_asks(&mut self, asks: Vec<WsLevel>) -> Result<Option<(f64, f64)>> {
        self.asks.clear();
        for ask in asks {
            self.asks
                .insert(OrderedFloat(ask.price.parse()?), ask.size.parse()?);
        }
        self.initialized = true;
        Ok(self.changed_best_ask())
    }

    fn update_ask(&mut self, price: &str, size: &str) -> Result<Option<(f64, f64)>> {
        if !self.initialized {
            return Ok(None);
        }

        self.apply_ask_change(price, size)?;
        Ok(self.changed_best_ask())
    }

    fn apply_ask_change(&mut self, price: &str, size: &str) -> Result<()> {
        let price = price.parse::<f64>()?;
        let size = size.parse::<f64>()?;
        if size == 0.0 {
            self.asks.remove(&OrderedFloat(price));
        } else {
            self.asks.insert(OrderedFloat(price), size);
        }
        Ok(())
    }

    fn changed_best_ask(&mut self) -> Option<(f64, f64)> {
        let new_best_ask = self
            .asks
            .iter()
            .next()
            .map(|(price, size)| (price.into_inner(), *size));

        if new_best_ask != self.last_best_ask {
            self.last_best_ask = new_best_ask;
            new_best_ask
        } else {
            None
        }
    }
}

pub async fn run_ob_feed(
    mut subscription_rx: mpsc::Receiver<Vec<String>>,
    update_tx: broadcast::Sender<ObUpdate>,
) -> Result<()> {
    let Some(initial_ids) = subscription_rx.recv().await else {
        return Ok(());
    };

    let mut shard_txs = Vec::with_capacity(SHARD_COUNT);
    let mut initial_shards = vec![Vec::new(); SHARD_COUNT];
    for asset_id in initial_ids {
        initial_shards[shard_for_token(&asset_id)].push(asset_id);
    }

    for (shard_id, asset_ids) in initial_shards.into_iter().enumerate() {
        let (shard_tx, shard_rx) = mpsc::channel::<Vec<String>>(32);
        shard_txs.push(shard_tx);
        let update_tx = update_tx.clone();
        tokio::spawn(async move {
            run_ob_feed_shard(shard_id, asset_ids, shard_rx, update_tx).await;
        });
    }

    while let Some(new_ids) = subscription_rx.recv().await {
        let mut shard_updates = vec![Vec::new(); SHARD_COUNT];
        for asset_id in new_ids {
            shard_updates[shard_for_token(&asset_id)].push(asset_id);
        }

        for (shard_id, asset_ids) in shard_updates.into_iter().enumerate() {
            if asset_ids.is_empty() {
                continue;
            }
            if let Some(shard_tx) = shard_txs.get(shard_id) {
                if shard_tx.send(asset_ids).await.is_err() {
                    warn!(shard_id, "OB shard subscription channel closed");
                }
            }
        }
    }

    Ok(())
}

async fn run_ob_feed_shard(
    shard_id: usize,
    initial_asset_ids: Vec<String>,
    mut subscription_rx: mpsc::Receiver<Vec<String>>,
    update_tx: broadcast::Sender<ObUpdate>,
) {
    let mut asset_ids = initial_asset_ids;

    loop {
        while let Ok(new_ids) = subscription_rx.try_recv() {
            asset_ids.extend(new_ids);
        }

        match connect_and_stream(shard_id, &asset_ids, &mut subscription_rx, &update_tx).await {
            Ok(()) => return,
            Err(err) => {
                warn!(
                    shard_id,
                    ?err,
                    "OB websocket disconnected; reconnecting in 10s"
                );
                sleep(Duration::from_secs(10)).await;
            }
        }
    }
}

fn shard_for_token(token_id: &str) -> usize {
    token_id
        .as_bytes()
        .iter()
        .rev()
        .find(|byte| byte.is_ascii_digit())
        .map(|byte| (byte - b'0') as usize)
        .unwrap_or(0)
}

async fn connect_and_stream(
    shard_id: usize,
    asset_ids: &[String],
    subscription_rx: &mut mpsc::Receiver<Vec<String>>,
    update_tx: &broadcast::Sender<ObUpdate>,
) -> Result<()> {
    let (mut ws, _) = connect_async(WS_MARKET_URL)
        .await
        .context("connect Polymarket market websocket")?;
    let mut subscribed = asset_ids.iter().cloned().collect::<HashSet<_>>();
    send_initial_subscription(&mut ws, shard_id, asset_ids).await?;

    let mut books: HashMap<String, OrderBook> = HashMap::new();
    let mut stats = ObFeedStats::default();
    let mut next_stats_log = Instant::now() + Duration::from_secs(30);

    loop {
        tokio::select! {
            maybe_new_ids = subscription_rx.recv() => {
                let Some(new_ids) = maybe_new_ids else {
                    return Ok(());
                };
                for asset_id in &new_ids {
                    subscribed.insert(asset_id.clone());
                }
                if !new_ids.is_empty() {
                    send_subscription_update(&mut ws, shard_id, new_ids).await?;
                }
            }
            maybe_msg = ws.next() => {
                let Some(msg) = maybe_msg else {
                    return Err(anyhow::anyhow!("websocket closed"));
                };
                let msg = msg.context("read websocket message")?;
                if let Message::Text(text) = msg {
                    handle_ws_text(&text, &subscribed, &mut books, update_tx, &mut stats)?;
                }
                if Instant::now() >= next_stats_log {
                    info!(
                        subscribed_tokens = subscribed.len(),
                        shard_id,
                        book_messages = stats.book_messages,
                        subscribed_book_messages = stats.subscribed_book_messages,
                        subscribed_book_messages_with_asks = stats.subscribed_book_messages_with_asks,
                        duplicate_book_messages = stats.duplicate_book_messages,
                        unsubscribed_book_messages = stats.unsubscribed_book_messages,
                        price_changes = stats.price_changes,
                        price_changes_before_book = stats.price_changes_before_book,
                        ob_updates_emitted = stats.ob_updates_emitted,
                        initialized_books = books.values().filter(|book| book.initialized).count(),
                        "OB feed stats"
                    );
                    next_stats_log = Instant::now() + Duration::from_secs(30);
                }
            }
        }
    }
}

async fn send_initial_subscription<S>(
    ws: &mut S,
    shard_id: usize,
    asset_ids: &[String],
) -> Result<()>
where
    S: SinkExt<Message> + Unpin,
    <S as futures_util::Sink<Message>>::Error: std::error::Error + Send + Sync + 'static,
{
    let mut ids = asset_ids.to_vec();
    ids.sort();
    let asset_count = ids.len();
    let payload = json!({
        "type": "market",
        "assets_ids": ids,
        "level": OB_WS_LEVEL,
    });
    ws.send(Message::Text(payload.to_string().into()))
        .await
        .context("send websocket subscription")?;
    info!(
        shard_id,
        asset_count,
        level = OB_WS_LEVEL,
        "sent OB websocket subscription"
    );
    Ok(())
}

async fn send_subscription_update<S>(
    ws: &mut S,
    shard_id: usize,
    mut asset_ids: Vec<String>,
) -> Result<()>
where
    S: SinkExt<Message> + Unpin,
    <S as futures_util::Sink<Message>>::Error: std::error::Error + Send + Sync + 'static,
{
    asset_ids.sort();
    let asset_count = asset_ids.len();
    let payload = json!({
        "operation": "subscribe",
        "assets_ids": asset_ids,
        "level": OB_WS_LEVEL,
    });
    ws.send(Message::Text(payload.to_string().into()))
        .await
        .context("send websocket subscription update")?;
    info!(
        shard_id,
        asset_count,
        level = OB_WS_LEVEL,
        "sent OB websocket subscription update"
    );
    Ok(())
}

fn handle_ws_text(
    text: &str,
    subscribed: &HashSet<String>,
    books: &mut HashMap<String, OrderBook>,
    update_tx: &broadcast::Sender<ObUpdate>,
    stats: &mut ObFeedStats,
) -> Result<()> {
    let value: serde_json::Value = match serde_json::from_str(text) {
        Ok(value) => value,
        Err(err) => {
            debug!(?err, text, "ignoring non-JSON websocket text");
            return Ok(());
        }
    };
    handle_ws_value(value, subscribed, books, update_tx, stats)
}

fn handle_ws_value(
    value: serde_json::Value,
    subscribed: &HashSet<String>,
    books: &mut HashMap<String, OrderBook>,
    update_tx: &broadcast::Sender<ObUpdate>,
    stats: &mut ObFeedStats,
) -> Result<()> {
    if let serde_json::Value::Array(items) = value {
        for item in items {
            handle_ws_value(item, subscribed, books, update_tx, stats)?;
        }
        return Ok(());
    }

    let event_type = value
        .get("event_type")
        .or_else(|| value.get("type"))
        .and_then(|v| v.as_str())
        .unwrap_or("");

    match event_type {
        "book" => {
            let msg: WsBook = serde_json::from_value(value).context("decode book snapshot")?;
            stats.book_messages += 1;
            if !subscribed.contains(&msg.asset_id) {
                stats.unsubscribed_book_messages += 1;
                return Ok(());
            }
            stats.subscribed_book_messages += 1;
            if !msg.asks.is_empty() {
                stats.subscribed_book_messages_with_asks += 1;
            }
            let book = books.entry(msg.asset_id.clone()).or_default();
            if book.initialized {
                stats.duplicate_book_messages += 1;
            }
            if let Some((price, size)) = book.replace_asks(msg.asks)? {
                stats.ob_updates_emitted += 1;
                debug!(
                    token_id = %msg.asset_id,
                    ask1_price = price,
                    ask1_size = size,
                    "OB ask1 changed"
                );
                let _ = update_tx.send(ObUpdate {
                    token_id: msg.asset_id,
                    ask1_price: price,
                    ask1_size: size,
                });
            }
        }
        "price_change" => {
            let msg: WsPriceChange =
                serde_json::from_value(value).context("decode price change")?;
            for change in msg.price_changes {
                if !subscribed.contains(&change.asset_id) {
                    continue;
                }
                stats.price_changes += 1;

                if change.side != "SELL" {
                    continue;
                }

                let book = books.entry(change.asset_id.clone()).or_default();
                if !book.initialized {
                    stats.price_changes_before_book += 1;
                }
                if let Some((price, size)) = book.update_ask(&change.price, &change.size)? {
                    stats.ob_updates_emitted += 1;
                    debug!(
                        token_id = %change.asset_id,
                        ask1_price = price,
                        ask1_size = size,
                        "OB ask1 changed"
                    );
                    let _ = update_tx.send(ObUpdate {
                        token_id: change.asset_id,
                        ask1_price: price,
                        ask1_size: size,
                    });
                }
            }
        }
        _ => {}
    }

    Ok(())
}

#[derive(Debug, Deserialize)]
struct WsBook {
    asset_id: String,
    #[serde(default)]
    asks: Vec<WsLevel>,
}

#[derive(Debug, Deserialize)]
struct WsLevel {
    price: String,
    size: String,
}

#[derive(Debug, Deserialize)]
struct WsPriceChange {
    #[serde(alias = "changes")]
    #[serde(rename = "price_changes")]
    #[serde(default)]
    price_changes: Vec<WsChange>,
}

#[derive(Debug, Deserialize)]
struct WsChange {
    asset_id: String,
    side: String,
    price: String,
    size: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn best_ask_is_lowest_ask_even_when_snapshot_is_descending() {
        let mut book = OrderBook::default();
        let changed = book
            .replace_asks(vec![
                WsLevel {
                    price: "0.999".into(),
                    size: "10".into(),
                },
                WsLevel {
                    price: "0.124".into(),
                    size: "30".into(),
                },
                WsLevel {
                    price: "0.1".into(),
                    size: "17.86".into(),
                },
            ])
            .unwrap();

        assert_eq!(changed, Some((0.1, 17.86)));
    }

    #[test]
    fn price_change_before_snapshot_does_not_emit_or_initialize() {
        let mut book = OrderBook::default();

        assert_eq!(book.update_ask("0.51", "35").unwrap(), None);
        assert_eq!(book.changed_best_ask(), None);

        let changed = book
            .replace_asks(vec![WsLevel {
                price: "0.35".into(),
                size: "25".into(),
            }])
            .unwrap();
        assert_eq!(changed, Some((0.35, 25.0)));
    }

    #[test]
    fn non_best_ask_price_change_does_not_emit() {
        let mut book = OrderBook::default();
        assert_eq!(
            book.replace_asks(vec![
                WsLevel {
                    price: "0.50".into(),
                    size: "10".into(),
                },
                WsLevel {
                    price: "0.40".into(),
                    size: "20".into(),
                },
            ])
            .unwrap(),
            Some((0.40, 20.0))
        );

        assert_eq!(book.update_ask("0.45", "30").unwrap(), None);
        assert_eq!(book.update_ask("0.60", "40").unwrap(), None);
    }

    #[test]
    fn better_ask_price_change_emits_new_best_ask() {
        let mut book = OrderBook::default();
        assert_eq!(
            book.replace_asks(vec![WsLevel {
                price: "0.40".into(),
                size: "20".into(),
            }])
            .unwrap(),
            Some((0.40, 20.0))
        );

        assert_eq!(book.update_ask("0.35", "25").unwrap(), Some((0.35, 25.0)));
    }

    #[test]
    fn removing_best_ask_emits_next_best_ask() {
        let mut book = OrderBook::default();
        assert_eq!(
            book.replace_asks(vec![
                WsLevel {
                    price: "0.50".into(),
                    size: "10".into(),
                },
                WsLevel {
                    price: "0.40".into(),
                    size: "20".into(),
                },
            ])
            .unwrap(),
            Some((0.40, 20.0))
        );

        assert_eq!(book.update_ask("0.40", "0").unwrap(), Some((0.50, 10.0)));
    }

    #[test]
    fn best_ask_size_change_emits() {
        let mut book = OrderBook::default();
        assert_eq!(
            book.replace_asks(vec![WsLevel {
                price: "0.40".into(),
                size: "20".into(),
            }])
            .unwrap(),
            Some((0.40, 20.0))
        );

        assert_eq!(book.update_ask("0.40", "15").unwrap(), Some((0.40, 15.0)));
    }

    #[test]
    fn shard_for_token_uses_last_digit() {
        assert_eq!(shard_for_token("1234567890"), 0);
        assert_eq!(shard_for_token("1234567897"), 7);
        assert_eq!(shard_for_token("not-a-token"), 0);
    }
}
