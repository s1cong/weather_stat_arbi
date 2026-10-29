use std::collections::{HashMap, HashSet};

use chrono::NaiveDate;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, info, warn};

use crate::types::{Bucket, ObUpdate, OrderSignal, SingleOrder, TemperatureEventKind};

pub struct EventStrategy {
    event_key: String,
    slug: String,
    event_date: NaiveDate,
    kind: TemperatureEventKind,
    active_buckets: HashSet<String>,
    bucket_temp: HashMap<String, Option<i32>>,
    best_ask: HashMap<String, f64>,
    ask1_size: HashMap<String, f64>,
    price_sum: f64,
    threshold: f64,
}

impl EventStrategy {
    pub fn new(
        event_key: String,
        slug: String,
        event_date: NaiveDate,
        kind: TemperatureEventKind,
        buckets: Vec<Bucket>,
        threshold: f64,
    ) -> Self {
        let mut active_buckets = HashSet::new();
        let mut bucket_temp = HashMap::new();

        for bucket in buckets {
            active_buckets.insert(bucket.token_id.clone());
            bucket_temp.insert(bucket.token_id, bucket.temp);
        }

        Self {
            event_key,
            slug,
            event_date,
            kind,
            active_buckets,
            bucket_temp,
            best_ask: HashMap::new(),
            ask1_size: HashMap::new(),
            price_sum: 0.0,
            threshold,
        }
    }

    pub fn on_ob_update(&mut self, update: ObUpdate) -> Option<OrderSignal> {
        if !self.active_buckets.contains(&update.token_id) {
            return None;
        }

        let was_unpriced = !self.best_ask.contains_key(&update.token_id);
        let old_price = self
            .best_ask
            .insert(update.token_id.clone(), update.ask1_price)
            .unwrap_or(0.0);
        self.price_sum += update.ask1_price - old_price;
        self.ask1_size
            .insert(update.token_id.clone(), update.ask1_size);

        if was_unpriced {
            info!(
                event_key = %self.event_key,
                slug = %self.slug,
                event_date = %self.event_date,
                token_id = %update.token_id,
                ask1_price = update.ask1_price,
                ask1_size = update.ask1_size,
                price_sum = self.price_sum,
                active_buckets = self.active_buckets.len(),
                priced_buckets = self.best_ask.len(),
                threshold = self.threshold,
                "strategy OB state"
            );
        } else {
            debug!(
                event_key = %self.event_key,
                slug = %self.slug,
                event_date = %self.event_date,
                token_id = %update.token_id,
                ask1_price = update.ask1_price,
                ask1_size = update.ask1_size,
                price_sum = self.price_sum,
                active_buckets = self.active_buckets.len(),
                priced_buckets = self.best_ask.len(),
                threshold = self.threshold,
                "strategy OB update"
            );
        }

        self.maybe_signal()
    }

    pub fn on_temperature_so_far_update(&mut self, value: f64) {
        match self.kind {
            TemperatureEventKind::Highest => self.on_max_so_far_update(value),
            TemperatureEventKind::Lowest => self.on_min_so_far_update(value),
        }
    }

    fn on_max_so_far_update(&mut self, max_so_far: f64) {
        let exclude_below_temp = max_so_far.floor() as i32;
        let mut removed = Vec::new();

        self.active_buckets.retain(|token_id| {
            match self.bucket_temp.get(token_id).copied().flatten() {
                Some(temp) if temp < exclude_below_temp => {
                    removed.push(token_id.clone());
                    false
                }
                Some(_) => true,
                None => {
                    warn!(
                        token_id,
                        "bucket temp unknown; keeping active until confirmed"
                    );
                    true
                }
            }
        });

        for token_id in removed {
            self.price_sum -= self.best_ask.remove(&token_id).unwrap_or(0.0);
            self.ask1_size.remove(&token_id);
        }

        info!(
            event_date = %self.event_date,
            kind = ?self.kind,
            max_so_far,
            exclude_below_temp,
            active_buckets = self.active_buckets.len(),
            price_sum = self.price_sum,
            "strategy max-so-far update"
        );
    }

    fn on_min_so_far_update(&mut self, min_so_far: f64) {
        let exclude_above_temp = min_so_far.ceil() as i32;
        let mut removed = Vec::new();

        self.active_buckets.retain(|token_id| {
            match self.bucket_temp.get(token_id).copied().flatten() {
                Some(temp) if temp > exclude_above_temp => {
                    removed.push(token_id.clone());
                    false
                }
                Some(_) => true,
                None => {
                    warn!(
                        token_id,
                        "bucket temp unknown; keeping active until confirmed"
                    );
                    true
                }
            }
        });

        for token_id in removed {
            self.price_sum -= self.best_ask.remove(&token_id).unwrap_or(0.0);
            self.ask1_size.remove(&token_id);
        }

        info!(
            event_date = %self.event_date,
            kind = ?self.kind,
            min_so_far,
            exclude_above_temp,
            active_buckets = self.active_buckets.len(),
            price_sum = self.price_sum,
            "strategy min-so-far update"
        );
    }

    fn maybe_signal(&self) -> Option<OrderSignal> {
        if self.active_buckets.is_empty() || self.price_sum >= self.threshold {
            return None;
        }

        if !self.active_buckets.iter().all(|token_id| {
            self.best_ask.contains_key(token_id) && self.ask1_size.contains_key(token_id)
        }) {
            return None;
        }

        let target_size = self
            .active_buckets
            .iter()
            .filter_map(|token_id| self.ask1_size.get(token_id).copied())
            .fold(f64::MAX, f64::min);

        if !target_size.is_finite() || target_size <= 0.0 {
            return None;
        }

        let orders = self
            .active_buckets
            .iter()
            .filter_map(|token_id| {
                self.best_ask.get(token_id).map(|price| SingleOrder {
                    token_id: token_id.clone(),
                    price: *price,
                    size: target_size,
                })
            })
            .collect();

        let signal = OrderSignal {
            event_key: self.event_key.clone(),
            slug: self.slug.clone(),
            event_date: self.event_date,
            price_sum: self.price_sum,
            target_size,
            gross_profit: target_size * (1.0 - self.price_sum),
            orders,
        };

        info!(
            event_key = %signal.event_key,
            slug = %signal.slug,
            event_date = %signal.event_date,
            order_count = signal.orders.len(),
            price_sum = signal.price_sum,
            target_size = signal.target_size,
            gross_profit = signal.gross_profit,
            threshold = self.threshold,
            "strategy generated order signal"
        );

        Some(signal)
    }
}

pub async fn run_event_strategy(
    mut strategy: EventStrategy,
    mut ob_rx: broadcast::Receiver<ObUpdate>,
    mut hko_rx: Option<mpsc::Receiver<f64>>,
    order_tx: mpsc::Sender<OrderSignal>,
) {
    loop {
        tokio::select! {
            received = ob_rx.recv() => {
                match received {
                    Ok(update) => {
                    if let Some(signal) = strategy.on_ob_update(update) {
                        if order_tx.send(signal).await.is_err() {
                            return;
                        }
                    }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(skipped, "strategy lagged behind OB updates");
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
            maybe_max = async {
                match hko_rx.as_mut() {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                match maybe_max {
                    Some(max_so_far) => strategy.on_temperature_so_far_update(max_so_far),
                    None => hko_rx = None,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::*;

    #[test]
    fn emits_signal_when_sum_below_threshold() {
        let date = NaiveDate::from_ymd_opt(2026, 4, 27).unwrap();
        let buckets = vec![
            Bucket {
                token_id: "a".into(),
                name: "23".into(),
                temp: Some(23),
            },
            Bucket {
                token_id: "b".into(),
                name: "24".into(),
                temp: Some(24),
            },
        ];
        let mut strategy = EventStrategy::new(
            "test-high".into(),
            "test-high-slug".into(),
            date,
            TemperatureEventKind::Highest,
            buckets,
            0.975,
        );

        assert!(strategy
            .on_ob_update(ObUpdate {
                token_id: "a".into(),
                ask1_price: 0.40,
                ask1_size: 9.0
            })
            .is_none());
        let signal = strategy
            .on_ob_update(ObUpdate {
                token_id: "b".into(),
                ask1_price: 0.50,
                ask1_size: 7.0,
            })
            .unwrap();

        assert_eq!(signal.orders.len(), 2);
        assert!(signal.orders.iter().all(|order| order.size == 7.0));
    }

    #[test]
    fn hko_update_only_removes_buckets_below_current_integer_temp() {
        let date = NaiveDate::from_ymd_opt(2026, 4, 27).unwrap();
        let buckets = vec![
            Bucket {
                token_id: "a".into(),
                name: "22".into(),
                temp: Some(22),
            },
            Bucket {
                token_id: "b".into(),
                name: "23".into(),
                temp: Some(23),
            },
            Bucket {
                token_id: "c".into(),
                name: "24".into(),
                temp: Some(24),
            },
        ];
        let mut strategy = EventStrategy::new(
            "test-high".into(),
            "test-high-slug".into(),
            date,
            TemperatureEventKind::Highest,
            buckets,
            0.975,
        );
        strategy.on_ob_update(ObUpdate {
            token_id: "a".into(),
            ask1_price: 0.40,
            ask1_size: 9.0,
        });
        strategy.on_ob_update(ObUpdate {
            token_id: "b".into(),
            ask1_price: 0.50,
            ask1_size: 7.0,
        });
        strategy.on_ob_update(ObUpdate {
            token_id: "c".into(),
            ask1_price: 0.05,
            ask1_size: 7.0,
        });

        strategy.on_temperature_so_far_update(23.9);

        assert!(!strategy.active_buckets.contains("a"));
        assert!(strategy.active_buckets.contains("b"));
        assert!(strategy.active_buckets.contains("c"));
        assert_eq!(strategy.price_sum, 0.55);
    }

    #[test]
    fn min_so_far_only_removes_buckets_above_current_integer_temp() {
        let date = NaiveDate::from_ymd_opt(2026, 4, 27).unwrap();
        let buckets = vec![
            Bucket {
                token_id: "a".into(),
                name: "18".into(),
                temp: Some(18),
            },
            Bucket {
                token_id: "b".into(),
                name: "19".into(),
                temp: Some(19),
            },
            Bucket {
                token_id: "c".into(),
                name: "20".into(),
                temp: Some(20),
            },
        ];
        let mut strategy = EventStrategy::new(
            "test-low".into(),
            "test-low-slug".into(),
            date,
            TemperatureEventKind::Lowest,
            buckets,
            0.975,
        );
        strategy.on_ob_update(ObUpdate {
            token_id: "a".into(),
            ask1_price: 0.10,
            ask1_size: 9.0,
        });
        strategy.on_ob_update(ObUpdate {
            token_id: "b".into(),
            ask1_price: 0.20,
            ask1_size: 7.0,
        });
        strategy.on_ob_update(ObUpdate {
            token_id: "c".into(),
            ask1_price: 0.30,
            ask1_size: 7.0,
        });

        strategy.on_temperature_so_far_update(18.6);

        assert!(strategy.active_buckets.contains("a"));
        assert!(strategy.active_buckets.contains("b"));
        assert!(!strategy.active_buckets.contains("c"));
        assert!((strategy.price_sum - 0.30).abs() < 1e-9);
    }
}
