use std::collections::HashSet;
use std::str::FromStr;

use anyhow::{Context, Result};
use polymarket_client_sdk_v2::auth::{state::Authenticated, LocalSigner, Normal, Signer as _};
use polymarket_client_sdk_v2::clob::types::{OrderType, Side, SignatureType};
use polymarket_client_sdk_v2::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk_v2::types::{Decimal, U256};
use polymarket_client_sdk_v2::{POLYGON, PRIVATE_KEY_VAR};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::types::{OrderConfig, OrderSignal};

type AuthClobClient = ClobClient<Authenticated<Normal>>;

const MIN_MARKETABLE_BUY_NOTIONAL: f64 = 1.0;

struct TradingClient<S> {
    client: AuthClobClient,
    signer: S,
}

pub async fn run_order_placement(mut rx: mpsc::Receiver<OrderSignal>, config: OrderConfig) {
    if config.dry_run {
        let mut logged_signals = HashSet::new();
        while let Some(signal) = rx.recv().await {
            let key = signal_key(&signal);
            if logged_signals.contains(&key) {
                continue;
            }
            logged_signals.insert(key);

            match validate_signal(&config, &signal) {
                Ok(()) => log_dry_run_opportunity(&signal),
                Err(err) => warn!(
                    ?err,
                    event_key = %signal.event_key,
                    slug = %signal.slug,
                    event_date = %signal.event_date,
                    price_sum = signal.price_sum,
                    target_size = signal.target_size,
                    gross_profit = signal.gross_profit,
                    "DRY_RUN_ARBITRAGE_SKIPPED"
                ),
            }
        }
        return;
    }

    let trading_client = match build_client(&config).await {
        Ok(client) => client,
        Err(err) => {
            error!(
                ?err,
                "real order placement disabled: failed to initialize CLOB client"
            );
            return;
        }
    };

    let mut attempted_signals = HashSet::new();
    while let Some(signal) = rx.recv().await {
        let key = signal_key(&signal);
        if attempted_signals.contains(&key) {
            info!(
                event_key = %signal.event_key,
                slug = %signal.slug,
                event_date = %signal.event_date,
                "duplicate order signal skipped after prior attempt"
            );
            continue;
        }

        attempted_signals.insert(key);
        match place_signal(&trading_client, &config, signal).await {
            Ok(()) => {}
            Err(err) => error!(
                ?err,
                "failed to place order signal; identical signal will not retry"
            ),
        }
    }
}

async fn build_client(
    config: &OrderConfig,
) -> Result<TradingClient<impl polymarket_client_sdk_v2::auth::Signer>> {
    let private_key = std::env::var(PRIVATE_KEY_VAR)
        .with_context(|| format!("{PRIVATE_KEY_VAR} is required when order.dry_run=false"))?;
    if private_key.trim().is_empty() {
        anyhow::bail!("{PRIVATE_KEY_VAR} is set but empty");
    }

    let signer = LocalSigner::from_str(&private_key)?.with_chain_id(Some(POLYGON));
    let client = ClobClient::new(&config.clob_host, ClobConfig::default())?
        .authentication_builder(&signer)
        .signature_type(SignatureType::GnosisSafe)
        .authenticate()
        .await?;

    Ok(TradingClient { client, signer })
}

async fn place_signal<S>(
    trading_client: &TradingClient<S>,
    config: &OrderConfig,
    signal: OrderSignal,
) -> Result<()>
where
    S: polymarket_client_sdk_v2::auth::Signer,
{
    validate_signal(config, &signal)?;

    let order_type = parse_order_type(&config.order_type)?;
    let mut signed_orders = Vec::with_capacity(signal.orders.len());
    info!(
        event_key = %signal.event_key,
        slug = %signal.slug,
        event_date = %signal.event_date,
        order_count = signal.orders.len(),
        price_sum = signal.price_sum,
        target_size = signal.target_size,
        gross_profit = signal.gross_profit,
        order_type = %config.order_type,
        "placing order signal"
    );

    for order in &signal.orders {
        let unsigned = trading_client
            .client
            .limit_order()
            .token_id(U256::from_str(&order.token_id).context("parse token_id")?)
            .side(Side::Buy)
            .price(parse_decimal(order.price).context("parse order price")?)
            .size(parse_decimal(order.size).context("parse order size")?)
            .order_type(order_type.clone())
            .build()
            .await?;
        signed_orders.push(
            trading_client
                .client
                .sign(&trading_client.signer, unsigned)
                .await?,
        );
    }

    let responses = trading_client.client.post_orders(signed_orders).await?;
    if responses.len() != signal.orders.len() {
        anyhow::bail!(
            "CLOB response count {} does not match order count {}",
            responses.len(),
            signal.orders.len()
        );
    }
    let all_success = responses.iter().all(order_response_accepted);

    info!(
        event_key = %signal.event_key,
        slug = %signal.slug,
        event_date = %signal.event_date,
        order_count = responses.len(),
        all_success,
        responses = ?responses,
        "posted order signal"
    );

    if !all_success {
        warn!(
            event_key = %signal.event_key,
            slug = %signal.slug,
            event_date = %signal.event_date,
            responses = ?responses,
            order_type = %config.order_type,
            "one or more orders were rejected; manual position check may be needed"
        );
        anyhow::bail!("one or more orders were rejected");
    }

    Ok(())
}

fn order_response_accepted(
    response: &polymarket_client_sdk_v2::clob::types::response::PostOrderResponse,
) -> bool {
    response.success
        && !response.order_id.is_empty()
        && response
            .error_msg
            .as_deref()
            .is_none_or(|message| message.trim().is_empty())
}

fn parse_order_type(value: &str) -> Result<OrderType> {
    match value.trim().to_ascii_uppercase().as_str() {
        "GTC" => Ok(OrderType::GTC),
        "FAK" => Ok(OrderType::FAK),
        "FOK" => Ok(OrderType::FOK),
        "GTD" => anyhow::bail!("order_type=GTD is not supported without an expiration"),
        other => anyhow::bail!("unsupported order_type {other:?}; expected GTC, FAK, or FOK"),
    }
}

fn validate_signal(config: &OrderConfig, signal: &OrderSignal) -> Result<()> {
    parse_order_type(&config.order_type)?;

    if signal.orders.is_empty() {
        anyhow::bail!("empty order signal");
    }

    for order in &signal.orders {
        if order.size <= 0.0 {
            anyhow::bail!("invalid order size {:.4}", order.size);
        }
        if order.size < config.min_order_size {
            anyhow::bail!(
                "order size {:.4} below min_order_size {:.4}",
                order.size,
                config.min_order_size
            );
        }

        let notional = order.price * order.size;
        if notional < MIN_MARKETABLE_BUY_NOTIONAL {
            anyhow::bail!(
                "order notional {:.6} below min marketable BUY notional {:.2} (price {:.6} * size {:.4})",
                notional,
                MIN_MARKETABLE_BUY_NOTIONAL,
                order.price,
                order.size
            );
        }
    }

    Ok(())
}

fn log_dry_run_opportunity(signal: &OrderSignal) {
    info!(
        event_key = %signal.event_key,
        slug = %signal.slug,
        event_date = %signal.event_date,
        order_count = signal.orders.len(),
        price_sum = signal.price_sum,
        target_size = signal.target_size,
        gross_profit = signal.gross_profit,
        orders = ?signal.orders,
        "DRY_RUN_ARBITRAGE_OPPORTUNITY"
    );

    println!(
        "DRY_RUN_ARBITRAGE_OPPORTUNITY event_key={} date={} price_sum={:.6} target_size={:.4} gross_profit={:.6} slug={}",
        signal.event_key,
        signal.event_date,
        signal.price_sum,
        signal.target_size,
        signal.gross_profit,
        signal.slug
    );
    for order in &signal.orders {
        println!(
            "  BUY token_id={} price={:.6} size={:.4}",
            order.token_id, order.price, order.size
        );
    }
}

fn signal_key(signal: &OrderSignal) -> String {
    let mut orders = signal
        .orders
        .iter()
        .map(|order| {
            format!(
                "{}:{}:{}",
                order.token_id,
                decimal_string(order.price),
                decimal_string(order.size)
            )
        })
        .collect::<Vec<_>>();
    orders.sort();
    format!(
        "{}|{}|{}|{}",
        signal.event_key,
        signal.slug,
        signal.event_date,
        orders.join("|")
    )
}

fn parse_decimal(value: f64) -> Result<Decimal> {
    Decimal::from_str(&decimal_string(value)).context("convert f64 to decimal")
}

fn decimal_string(value: f64) -> String {
    let mut rendered = format!("{value:.8}");
    while rendered.contains('.') && rendered.ends_with('0') {
        rendered.pop();
    }
    if rendered.ends_with('.') {
        rendered.pop();
    }
    rendered
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::*;
    use crate::types::SingleOrder;

    #[test]
    fn duplicate_key_is_order_independent() {
        let date = NaiveDate::from_ymd_opt(2026, 4, 28).unwrap();
        let a = OrderSignal {
            event_key: "test".into(),
            slug: "test-slug".into(),
            event_date: date,
            price_sum: 0.5,
            target_size: 3.0,
            gross_profit: 1.5,
            orders: vec![
                SingleOrder {
                    token_id: "b".into(),
                    price: 0.2,
                    size: 3.0,
                },
                SingleOrder {
                    token_id: "a".into(),
                    price: 0.3,
                    size: 3.0,
                },
            ],
        };
        let b = OrderSignal {
            event_key: "test".into(),
            slug: "test-slug".into(),
            event_date: date,
            price_sum: 0.5,
            target_size: 3.0,
            gross_profit: 1.5,
            orders: vec![
                SingleOrder {
                    token_id: "a".into(),
                    price: 0.3,
                    size: 3.0,
                },
                SingleOrder {
                    token_id: "b".into(),
                    price: 0.2,
                    size: 3.0,
                },
            ],
        };

        assert_eq!(signal_key(&a), signal_key(&b));
    }

    #[test]
    fn validates_order_type() {
        assert_eq!(parse_order_type("GTC").unwrap(), OrderType::GTC);
        assert_eq!(parse_order_type("fak").unwrap(), OrderType::FAK);
        assert!(parse_order_type("GTD").is_err());
        assert!(parse_order_type("bad").is_err());
    }

    #[test]
    fn validates_min_order_size() {
        let config = OrderConfig {
            dry_run: false,
            clob_host: "https://clob.polymarket.com".into(),
            order_type: "GTC".into(),
            min_order_size: 5.0,
        };
        let signal = OrderSignal {
            event_key: "test".into(),
            slug: "test-slug".into(),
            event_date: NaiveDate::from_ymd_opt(2026, 4, 28).unwrap(),
            price_sum: 0.5,
            target_size: 4.99,
            gross_profit: 1.5,
            orders: vec![SingleOrder {
                token_id: "1".into(),
                price: 0.5,
                size: 4.99,
            }],
        };

        assert!(validate_signal(&config, &signal).is_err());
    }

    #[test]
    fn validates_min_marketable_buy_notional() {
        let config = OrderConfig {
            dry_run: false,
            clob_host: "https://clob.polymarket.com".into(),
            order_type: "GTC".into(),
            min_order_size: 5.0,
        };
        let date = NaiveDate::from_ymd_opt(2026, 4, 28).unwrap();
        let rejected = OrderSignal {
            event_key: "test".into(),
            slug: "test-slug".into(),
            event_date: date,
            price_sum: 0.5,
            target_size: 5.0,
            gross_profit: 1.5,
            orders: vec![SingleOrder {
                token_id: "1".into(),
                price: 0.1,
                size: 5.0,
            }],
        };
        let accepted = OrderSignal {
            orders: vec![SingleOrder {
                token_id: "1".into(),
                price: 0.2,
                size: 5.0,
            }],
            ..rejected.clone()
        };

        assert!(validate_signal(&config, &rejected).is_err());
        assert!(validate_signal(&config, &accepted).is_ok());
    }
}
