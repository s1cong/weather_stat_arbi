use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::time::Instant;

mod event_discovery;
mod hko_feed;
mod ob_feed;
mod order_placement;
mod strategy;
mod types;

use anyhow::{Context, Result};
use tokio::sync::{broadcast, mpsc};
use tokio::time::{self, timeout, Duration};
use tracing::{error, info, warn};

use crate::types::{Bucket, Config, ObUpdate};
use polymarket_client_sdk_v2::auth::{LocalSigner, Signer as _};
use polymarket_client_sdk_v2::clob::types::request::BalanceAllowanceRequest;
use polymarket_client_sdk_v2::clob::types::{AssetType, OrderType, Side, SignatureType};
use polymarket_client_sdk_v2::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk_v2::types::{Decimal, U256};
use polymarket_client_sdk_v2::{POLYGON, PRIVATE_KEY_VAR};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "weather_stat_arbi=info,info".into()),
        )
        .init();

    let config = load_config().context("load config.toml")?;
    let args: Vec<String> = std::env::args().collect();

    match args.get(1).map(String::as_str) {
        Some("hko-once") => return hko_once(&config).await,
        Some("temperature-discover-once") => return temperature_discover_once().await,
        Some("temperature-token-list") => return temperature_token_list(&config).await,
        Some("temperature-registry-report") => return temperature_registry_report(&config).await,
        Some("temperature-scan") => return run_temperature_scan(config, true).await,
        Some("temperature-live") => return run_temperature_scan(config, false).await,
        Some("strategy-fixture") => return strategy_fixture(&config),
        Some("ob-once") => {
            let token_id = args
                .get(2)
                .context("usage: cargo run -- ob-once <token_id>")?;
            return ob_once(token_id.clone()).await;
        }
        Some("order-build-test") => {
            let token_id = args
                .get(2)
                .context("usage: cargo run -- order-build-test <token_id> <price> <size>")?;
            let price = args
                .get(3)
                .context("usage: cargo run -- order-build-test <token_id> <price> <size>")?;
            let size = args
                .get(4)
                .context("usage: cargo run -- order-build-test <token_id> <price> <size>")?;
            return order_build_test(&config, token_id, price, size).await;
        }
        Some("order-post-test") => {
            let token_id = args
                .get(2)
                .context("usage: cargo run -- order-post-test <token_id> <price> <size>")?;
            let price = args
                .get(3)
                .context("usage: cargo run -- order-post-test <token_id> <price> <size>")?;
            let size = args
                .get(4)
                .context("usage: cargo run -- order-post-test <token_id> <price> <size>")?;
            return order_post_test(&config, token_id, price, size).await;
        }
        Some("account-check") => return account_check(&config).await,
        Some(other) => warn!(
            command = other,
            "unknown command; starting all-temperature scanner"
        ),
        None => {}
    }

    run_temperature_scan(config, false).await
}

fn load_config() -> Result<Config> {
    let body = std::fs::read_to_string("config.toml")?;
    Ok(toml::from_str(&body)?)
}

async fn hko_once(config: &Config) -> Result<()> {
    let hko = config
        .hko
        .as_ref()
        .context("hko-once requires an [hko] section in config.toml")?;
    let client = reqwest::Client::new();
    let obs = hko_feed::fetch_hko_max(&client, &hko.station).await?;
    println!(
        "HKO station={} observed_at={} max_so_far={}",
        hko.station,
        obs.observed_at.format("%Y-%m-%d %H:%M"),
        obs.max_so_far
    );
    Ok(())
}

async fn temperature_registry_report(config: &Config) -> Result<()> {
    let discovery = event_discovery::EventDiscovery::new();
    let events = discovery.discover_daily_temperature_events(500).await?;
    let configured: HashMap<_, _> = config
        .temperature_events
        .iter()
        .map(|spec| (spec.event_key.as_str(), spec))
        .collect();

    let mut discovered_keys = HashSet::new();
    let mut known_enabled = Vec::new();
    let mut known_disabled = Vec::new();
    let mut unconfigured = Vec::new();

    for event in &events {
        if !discovered_keys.insert(event.event_key.clone()) {
            continue;
        }

        match configured.get(event.event_key.as_str()) {
            Some(spec) if spec.trade_enabled => known_enabled.push(event),
            Some(_) => known_disabled.push(event),
            None => unconfigured.push(event),
        }
    }

    println!("temperature_events_seen={}", events.len());
    println!("families_seen={}", discovered_keys.len());
    println!("known_enabled={}", known_enabled.len());
    for event in known_enabled {
        let id = configured
            .get(event.event_key.as_str())
            .map(|spec| spec.id.as_str())
            .unwrap_or("unknown");
        println!(
            "  ENABLED id={} {} sample_slug={}",
            id, event.event_key, event.slug
        );
    }
    println!("known_disabled={}", known_disabled.len());
    for event in known_disabled {
        let id = configured
            .get(event.event_key.as_str())
            .map(|spec| spec.id.as_str())
            .unwrap_or("unknown");
        println!(
            "  DISABLED id={} {} sample_slug={}",
            id, event.event_key, event.slug
        );
    }
    println!("unconfigured={}", unconfigured.len());
    for event in unconfigured {
        println!(
            "  NEW {} kind={:?} city={} unit={:?} sample_slug={} source={}",
            event.event_key,
            event.kind,
            event.city_slug,
            event.unit,
            event.slug,
            event.resolution_source.as_deref().unwrap_or("NA")
        );
    }

    Ok(())
}

fn is_tracked_event_date(
    today: chrono::NaiveDate,
    track_days: u64,
    event_date: chrono::NaiveDate,
) -> bool {
    let Some(last_date) = today.checked_add_days(chrono::Days::new(track_days.saturating_sub(1)))
    else {
        return false;
    };
    event_date >= today && event_date <= last_date
}

async fn run_temperature_scan(mut config: Config, force_dry_run: bool) -> Result<()> {
    if force_dry_run {
        config.order.dry_run = true;
    }

    let (ob_update_tx, _) = broadcast::channel::<ObUpdate>(8192);
    let (subscription_tx, subscription_rx) = mpsc::channel::<Vec<String>>(32);
    let (order_tx, order_rx) = mpsc::channel(128);

    info!(
        dry_run = config.order.dry_run,
        order_type = %config.order.order_type,
        track_days = config.event.track_days,
        threshold = config.strategy.price_sum_threshold,
        "starting all-temperature opportunity scanner"
    );

    let ob_feed_update_tx = ob_update_tx.clone();
    tokio::spawn(async move {
        if let Err(err) = ob_feed::run_ob_feed(subscription_rx, ob_feed_update_tx).await {
            error!(?err, "OB feed task exited");
        }
    });
    tokio::spawn(order_placement::run_order_placement(
        order_rx,
        config.order.clone(),
    ));

    let discovery = event_discovery::EventDiscovery::new();
    let mut discovery_interval =
        time::interval(Duration::from_secs(config.event.discovery_interval_secs));
    let mut seen_event_slugs = HashSet::new();

    loop {
        tokio::select! {
            _ = discovery_interval.tick() => {
                let today_hkt = today_hkt();
                match discovery.discover_daily_temperature_events(500).await {
                    Ok(events) => {
                        let events = events
                            .into_iter()
                            .filter(|event| {
                                is_tracked_event_date(
                                    today_hkt,
                                    config.event.track_days,
                                    event.event_date,
                                )
                            })
                            .collect::<Vec<_>>();
                        let total_buckets = events
                            .iter()
                            .map(|event| event.buckets.len())
                            .sum::<usize>();
                        let events_with_skipped_markets = events
                            .iter()
                            .filter(|event| event.skipped_market_count != 0)
                            .count();
                        let unique_yes_tokens = events
                            .iter()
                            .flat_map(|event| event.buckets.iter().map(|bucket| &bucket.token_id))
                            .collect::<HashSet<_>>()
                            .len();
                        let mut token_occurrences: HashMap<String, Vec<String>> = HashMap::new();
                        for event in &events {
                            for bucket in &event.buckets {
                                token_occurrences.entry(bucket.token_id.clone()).or_default().push(
                                    format!("{}|{}|{}", event.slug, event.event_key, bucket.name),
                                );
                            }
                        }
                        let duplicate_token_buckets = token_occurrences
                            .values()
                            .map(|occurrences| occurrences.len().saturating_sub(1))
                            .sum::<usize>();
                        let duplicate_token_ids = token_occurrences
                            .values()
                            .filter(|occurrences| occurrences.len() > 1)
                            .count();
                        let duplicate_token_samples = token_occurrences
                            .iter()
                            .filter(|(_, occurrences)| occurrences.len() > 1)
                            .take(5)
                            .map(|(token_id, occurrences)| {
                                format!("{} -> {}", token_id, occurrences.join(" || "))
                            })
                            .collect::<Vec<_>>();
                        info!(
                            event_count = events.len(),
                            events_with_skipped_markets,
                            total_buckets,
                            unique_yes_tokens,
                            duplicate_token_buckets,
                            duplicate_token_ids,
                            duplicate_token_samples = ?duplicate_token_samples,
                            "temperature scan discovery cycle finished"
                        );

                        let mut cycle_new_token_ids = Vec::new();
                        for event in events {
                            if !seen_event_slugs.insert(event.slug.clone()) {
                                continue;
                            }

                            let event_token_count = event.buckets.len();
                            cycle_new_token_ids.extend(
                                event.buckets.iter().map(|bucket| bucket.token_id.clone()),
                            );

                            if event_token_count != 0 {
                                info!(
                                    event_date = %event.event_date,
                                    event_key = %event.event_key,
                                    city = %event.city_slug,
                                    kind = ?event.kind,
                                    slug = %event.slug,
                                    yes_tokens = event_token_count,
                                    "scanner found new YES tokens"
                                );
                            }

                            let strategy = strategy::EventStrategy::new(
                                event.event_key.clone(),
                                event.slug.clone(),
                                event.event_date,
                                event.kind,
                                event.buckets,
                                config.strategy.price_sum_threshold,
                            );
                            tokio::spawn(strategy::run_event_strategy(
                                strategy,
                                ob_update_tx.subscribe(),
                                None,
                                order_tx.clone(),
                            ));
                        }

                        if !cycle_new_token_ids.is_empty() {
                            info!(
                                new_tokens = cycle_new_token_ids.len(),
                                "scanner subscribing cycle YES tokens"
                            );
                            subscription_tx.send(cycle_new_token_ids).await?;
                        }
                    }
                    Err(err) => warn!(?err, "temperature scan discovery failed"),
                }
            }
            result = tokio::signal::ctrl_c() => {
                result?;
                info!("temperature scanner shutdown requested");
                return Ok(());
            }
        }
    }
}

async fn temperature_discover_once() -> Result<()> {
    let discovery = event_discovery::EventDiscovery::new();
    let events = discovery.discover_daily_temperature_events(500).await?;

    println!("daily_temperature_events={}", events.len());
    for event in events {
        println!(
            "event_key={} city={} kind={:?} event_date={} unit={:?} slug={} buckets={} skipped={} source={}",
            event.event_key,
            event.city_slug,
            event.kind,
            event.event_date,
            event.unit,
            event.slug,
            event.buckets.len(),
            event.skipped_market_count,
            event.resolution_source.as_deref().unwrap_or("NA")
        );
    }
    Ok(())
}

async fn temperature_token_list(config: &Config) -> Result<()> {
    let today_hkt = today_hkt();
    let discovery = event_discovery::EventDiscovery::new();
    let events = discovery.discover_daily_temperature_events(500).await?;
    let events = events
        .into_iter()
        .filter(|event| is_tracked_event_date(today_hkt, config.event.track_days, event.event_date))
        .collect::<Vec<_>>();
    let total_buckets = events
        .iter()
        .map(|event| event.buckets.len())
        .sum::<usize>();

    println!("event_count={}", events.len());
    println!("yes_token_count={}", total_buckets);
    println!("event_date,event_key,slug,bucket_name,yes_clob_token_id");
    for event in events {
        for bucket in event.buckets {
            println!(
                "{},{},{},{},{}",
                event.event_date,
                event.event_key,
                event.slug,
                bucket.name.replace(',', " "),
                bucket.token_id
            );
        }
    }

    Ok(())
}

fn strategy_fixture(config: &Config) -> Result<()> {
    let mut strategy = strategy::EventStrategy::new(
        "fixture-high".into(),
        "fixture-high-slug".into(),
        today_hkt(),
        crate::types::TemperatureEventKind::Highest,
        vec![
            Bucket {
                token_id: "token-23".to_string(),
                name: "23 C bucket".to_string(),
                temp: Some(23),
            },
            Bucket {
                token_id: "token-24".to_string(),
                name: "24 C bucket".to_string(),
                temp: Some(24),
            },
        ],
        config.strategy.price_sum_threshold,
    );

    let first = strategy.on_ob_update(ObUpdate {
        token_id: "token-23".to_string(),
        ask1_price: 0.40,
        ask1_size: 10.0,
    });
    let second = strategy.on_ob_update(ObUpdate {
        token_id: "token-24".to_string(),
        ask1_price: 0.50,
        ask1_size: 7.0,
    });

    println!("after_first_update={first:?}");
    println!("after_second_update={second:?}");
    Ok(())
}

async fn ob_once(token_id: String) -> Result<()> {
    let (subscription_tx, subscription_rx) = mpsc::channel::<Vec<String>>(1);
    let (ob_update_tx, mut ob_update_rx) = broadcast::channel::<ObUpdate>(16);

    tokio::spawn(ob_feed::run_ob_feed(subscription_rx, ob_update_tx));
    subscription_tx.send(vec![token_id]).await?;

    match timeout(Duration::from_secs(30), ob_update_rx.recv()).await {
        Ok(Ok(update)) => {
            println!("ob_update={update:?}");
            Ok(())
        }
        Ok(Err(err)) => Err(err.into()),
        Err(_) => anyhow::bail!("timed out waiting for OB update"),
    }
}

async fn order_build_test(config: &Config, token_id: &str, price: &str, size: &str) -> Result<()> {
    let total_started = Instant::now();
    let private_key = std::env::var(PRIVATE_KEY_VAR)
        .with_context(|| format!("{PRIVATE_KEY_VAR} is required for order-build-test"))?;
    if private_key.trim().is_empty() {
        anyhow::bail!("{PRIVATE_KEY_VAR} is set but empty");
    }
    let signer = LocalSigner::from_str(&private_key)?.with_chain_id(Some(POLYGON));
    let token_id = U256::from_str(token_id).context("parse token_id")?;
    let price = Decimal::from_str(price).context("parse price")?;
    let size = Decimal::from_str(size).context("parse size")?;

    let auth_started = Instant::now();
    let client = ClobClient::new(&clob_host(config), ClobConfig::default())?
        .authentication_builder(&signer)
        .signature_type(SignatureType::GnosisSafe)
        .authenticate()
        .await?;
    let auth_elapsed = auth_started.elapsed();

    let build_started = Instant::now();
    let order = client
        .limit_order()
        .token_id(token_id)
        .side(Side::Buy)
        .price(price)
        .size(size)
        .order_type(OrderType::GTC)
        .build()
        .await?;
    let build_elapsed = build_started.elapsed();

    let sign_started = Instant::now();
    let _signed = client.sign(&signer, order).await?;
    let sign_elapsed = sign_started.elapsed();

    println!(
        "order-build-test=ok signed=true posted=false auth_ms={} build_ms={} sign_ms={} total_ms={}",
        auth_elapsed.as_millis(),
        build_elapsed.as_millis(),
        sign_elapsed.as_millis(),
        total_started.elapsed().as_millis()
    );
    Ok(())
}

async fn order_post_test(config: &Config, token_id: &str, price: &str, size: &str) -> Result<()> {
    let total_started = Instant::now();
    let private_key = std::env::var(PRIVATE_KEY_VAR)
        .with_context(|| format!("{PRIVATE_KEY_VAR} is required for order-post-test"))?;
    if private_key.trim().is_empty() {
        anyhow::bail!("{PRIVATE_KEY_VAR} is set but empty");
    }
    let signer = LocalSigner::from_str(&private_key)?.with_chain_id(Some(POLYGON));
    let token_id = U256::from_str(token_id).context("parse token_id")?;
    let price = Decimal::from_str(price).context("parse price")?;
    let size = Decimal::from_str(size).context("parse size")?;

    let auth_started = Instant::now();
    let client = ClobClient::new(&clob_host(config), ClobConfig::default())?
        .authentication_builder(&signer)
        .signature_type(SignatureType::GnosisSafe)
        .authenticate()
        .await?;
    let auth_elapsed = auth_started.elapsed();

    let build_started = Instant::now();
    let order = client
        .limit_order()
        .token_id(token_id)
        .side(Side::Buy)
        .price(price)
        .size(size)
        .order_type(OrderType::GTC)
        .build()
        .await?;
    let build_elapsed = build_started.elapsed();

    let sign_started = Instant::now();
    let signed = client.sign(&signer, order).await?;
    let sign_elapsed = sign_started.elapsed();

    let post_started = Instant::now();
    match client.post_order(signed).await {
        Ok(resp) => {
            let post_elapsed = post_started.elapsed();
            println!(
                "order-post-test=ok success={} status={} order_id={} error_msg={:?} auth_ms={} build_ms={} sign_ms={} post_ms={} total_before_cancel_ms={}",
                resp.success,
                resp.status,
                resp.order_id,
                resp.error_msg,
                auth_elapsed.as_millis(),
                build_elapsed.as_millis(),
                sign_elapsed.as_millis(),
                post_elapsed.as_millis(),
                total_started.elapsed().as_millis()
            );
            if resp.success && !resp.order_id.is_empty() {
                let cancel_started = Instant::now();
                match client.cancel_order(&resp.order_id).await {
                    Ok(cancel) => println!(
                        "order-cancel-test=ok cancel_ms={} total_ms={} response={cancel:?}",
                        cancel_started.elapsed().as_millis(),
                        total_started.elapsed().as_millis()
                    ),
                    Err(err) => println!(
                        "order-cancel-test=err cancel_ms={} total_ms={} error={err:#}",
                        cancel_started.elapsed().as_millis(),
                        total_started.elapsed().as_millis()
                    ),
                }
            }
        }
        Err(err) => {
            println!(
                "order-post-test=err auth_ms={} build_ms={} sign_ms={} post_ms={} total_ms={} error={err:#}",
                auth_elapsed.as_millis(),
                build_elapsed.as_millis(),
                sign_elapsed.as_millis(),
                post_started.elapsed().as_millis(),
                total_started.elapsed().as_millis()
            );
        }
    }

    Ok(())
}

async fn account_check(config: &Config) -> Result<()> {
    let private_key = std::env::var(PRIVATE_KEY_VAR)
        .with_context(|| format!("{PRIVATE_KEY_VAR} is required for account-check"))?;
    if private_key.trim().is_empty() {
        anyhow::bail!("{PRIVATE_KEY_VAR} is set but empty");
    }
    let signer = LocalSigner::from_str(&private_key)?.with_chain_id(Some(POLYGON));
    let signer_address = signer.address().to_string();
    let env_address = std::env::var("POLY_ADDRESS").ok();

    println!("signer_address={signer_address}");
    match env_address {
        Some(address) => println!(
            "poly_address_env={} matches_signer={}",
            address,
            address.eq_ignore_ascii_case(&signer_address)
        ),
        None => println!("poly_address_env=<missing>"),
    }

    let client = ClobClient::new(&clob_host(config), ClobConfig::default())?
        .authentication_builder(&signer)
        .authenticate()
        .await?;

    for signature_type in [
        SignatureType::Eoa,
        SignatureType::Proxy,
        SignatureType::GnosisSafe,
        SignatureType::Poly1271,
    ] {
        match client
            .balance_allowance(
                BalanceAllowanceRequest::builder()
                    .asset_type(AssetType::Collateral)
                    .signature_type(signature_type)
                    .build(),
            )
            .await
        {
            Ok(resp) => println!(
                "balance_allowance signature_type={signature_type:?} balance={} allowances={:?}",
                resp.balance, resp.allowances
            ),
            Err(err) => println!("balance_allowance signature_type={signature_type:?} err={err:#}"),
        }
    }

    Ok(())
}

fn today_hkt() -> chrono::NaiveDate {
    chrono::Utc::now()
        .with_timezone(&chrono::FixedOffset::east_opt(8 * 3600).unwrap())
        .date_naive()
}

fn clob_host(config: &Config) -> String {
    std::env::var("CLOB_API_URL").unwrap_or_else(|_| config.order.clob_host.clone())
}
