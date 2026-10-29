use chrono::NaiveDate;
use serde::Deserialize;

#[derive(Debug, Clone, PartialEq)]
pub struct ObUpdate {
    pub token_id: String,
    pub ask1_price: f64,
    pub ask1_size: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderSignal {
    pub event_key: String,
    pub slug: String,
    pub event_date: NaiveDate,
    pub price_sum: f64,
    pub target_size: f64,
    pub gross_profit: f64,
    pub orders: Vec<SingleOrder>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SingleOrder {
    pub token_id: String,
    pub price: f64,
    pub size: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EventMarket {
    pub event_key: String,
    pub city_slug: String,
    pub kind: TemperatureEventKind,
    pub event_date: NaiveDate,
    pub slug: String,
    pub unit: TemperatureUnit,
    pub resolution_source: Option<String>,
    pub buckets: Vec<Bucket>,
    pub skipped_market_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemperatureEventKind {
    Highest,
    Lowest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemperatureUnit {
    Celsius,
    Fahrenheit,
    Unknown,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Bucket {
    pub token_id: String,
    pub name: String,
    pub temp: Option<i32>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub event: EventConfig,
    pub strategy: StrategyConfig,
    #[serde(default)]
    pub hko: Option<HkoConfig>,
    pub order: OrderConfig,
    #[serde(default)]
    pub temperature_events: Vec<TemperatureEventConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EventConfig {
    pub track_days: u64,
    pub discovery_interval_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StrategyConfig {
    pub price_sum_threshold: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct HkoConfig {
    pub poll_interval_secs: u64,
    pub station: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OrderConfig {
    pub dry_run: bool,
    pub clob_host: String,
    pub order_type: String,
    #[serde(default = "default_min_order_size")]
    pub min_order_size: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct TemperatureEventConfig {
    pub id: String,
    pub event_key: String,
    #[serde(default)]
    pub trade_enabled: bool,
    #[serde(default)]
    pub so_far: Option<SoFarConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct SoFarConfig {
    pub metric: String,
    pub provider: String,
    pub station: Option<String>,
}

fn default_min_order_size() -> f64 {
    5.0
}
