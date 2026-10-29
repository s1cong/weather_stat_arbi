use anyhow::{anyhow, Context, Result};
use chrono::NaiveDate;
use serde::Deserialize;
use serde_json::Value;
use tracing::{debug, warn};

use crate::types::{Bucket, EventMarket, TemperatureEventKind, TemperatureUnit};

const GAMMA_BASE_URL: &str = "https://gamma-api.polymarket.com";
const DAILY_TEMPERATURE_TAG_ID: &str = "103040";

#[derive(Debug)]
pub struct EventDiscovery {
    client: reqwest::Client,
}

impl EventDiscovery {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    pub async fn discover_daily_temperature_events(
        &self,
        limit: usize,
    ) -> Result<Vec<EventMarket>> {
        let mut events = Vec::new();

        let query = vec![
            ("tag_id", DAILY_TEMPERATURE_TAG_ID.to_string()),
            ("active", "true".to_string()),
            ("closed", "false".to_string()),
            ("limit", limit.to_string()),
        ];

        let page = self
            .client
            .get(format!("{GAMMA_BASE_URL}/events/keyset"))
            .query(&query)
            .send()
            .await
            .context("request Gamma daily temperature events")?
            .error_for_status()
            .context("Gamma daily temperature events status")?
            .json::<GammaKeysetPage>()
            .await
            .context("decode Gamma daily temperature events")?;

        for event in page.events {
            match event.into_event_market() {
                Ok(event) => events.push(event),
                Err(err) => warn!(?err, "skip malformed temperature event from keyset"),
            }
        }

        Ok(events)
    }
}

fn parse_gamma_string_array(value: &Value) -> Result<Vec<String>> {
    match value {
        Value::Array(items) => Ok(items
            .iter()
            .filter_map(|item| item.as_str().map(ToOwned::to_owned))
            .collect()),
        Value::String(s) => {
            serde_json::from_str::<Vec<String>>(s).context("parse stringified JSON array")
        }
        _ => Ok(Vec::new()),
    }
}

fn parse_buckets(markets: Vec<GammaMarket>) -> Result<(Vec<Bucket>, usize)> {
    let mut buckets = Vec::new();
    let mut skipped_market_count = 0;
    for market in markets {
        if !market.is_tradeable() {
            skipped_market_count += 1;
            debug!(
                question = %market.question,
                active = ?market.active,
                closed = ?market.closed,
                accepting_orders = ?market.accepting_orders,
                enable_order_book = ?market.enable_order_book,
                has_reviewed_dates = ?market.has_reviewed_dates,
                "skip non-tradeable temperature market"
            );
            continue;
        }

        let outcomes = parse_gamma_string_array(&market.outcomes)
            .with_context(|| format!("parse outcomes for market {}", market.question))?;
        let token_ids = parse_gamma_string_array(&market.clob_token_ids)
            .with_context(|| format!("parse clobTokenIds for market {}", market.question))?;

        for (idx, outcome) in outcomes.iter().enumerate() {
            if outcome == "Yes" {
                let Some(token_id) = token_ids.get(idx).cloned() else {
                    warn!(question = market.question, idx, "missing YES clob token id");
                    continue;
                };

                buckets.push(Bucket {
                    token_id,
                    name: market.question.clone(),
                    temp: parse_bucket_temp(&market.question),
                });
            }
        }
    }
    Ok((buckets, skipped_market_count))
}

fn parse_temperature_slug(slug: &str) -> Option<(TemperatureEventKind, String, NaiveDate)> {
    let (kind, rest) = if let Some(rest) = slug.strip_prefix("highest-temperature-in-") {
        (TemperatureEventKind::Highest, rest)
    } else if let Some(rest) = slug.strip_prefix("lowest-temperature-in-") {
        (TemperatureEventKind::Lowest, rest)
    } else {
        return None;
    };

    let (city_slug, date_slug) = rest.rsplit_once("-on-")?;
    let mut parts = date_slug.split('-');
    let month = month_number(parts.next()?)?;
    let day = parts.next()?.parse::<u32>().ok()?;
    let year = parts.next()?.parse::<i32>().ok()?;
    if parts.next().is_some() {
        return None;
    }

    Some((
        kind,
        city_slug.to_string(),
        NaiveDate::from_ymd_opt(year, month, day)?,
    ))
}

fn month_number(month: &str) -> Option<u32> {
    match month {
        "january" => Some(1),
        "february" => Some(2),
        "march" => Some(3),
        "april" => Some(4),
        "may" => Some(5),
        "june" => Some(6),
        "july" => Some(7),
        "august" => Some(8),
        "september" => Some(9),
        "october" => Some(10),
        "november" => Some(11),
        "december" => Some(12),
        _ => None,
    }
}

fn detect_unit(event: &GammaEvent) -> TemperatureUnit {
    let question = event
        .markets
        .first()
        .map(|market| market.question.as_str())
        .unwrap_or_default();
    if question.contains("°F") {
        return TemperatureUnit::Fahrenheit;
    }
    if question.contains("°C") {
        return TemperatureUnit::Celsius;
    }

    let text = format!(
        "{} {}",
        event.title,
        event.description.as_deref().unwrap_or_default()
    );
    if text.contains("whole degrees Fahrenheit") {
        TemperatureUnit::Fahrenheit
    } else if text.contains("whole degrees Celsius") {
        TemperatureUnit::Celsius
    } else {
        TemperatureUnit::Unknown
    }
}

pub fn parse_bucket_temp(question: &str) -> Option<i32> {
    let chars: Vec<char> = question.chars().collect();
    let mut idx = 0;

    while idx < chars.len() {
        let ch = chars[idx];
        if ch.is_ascii_digit() {
            let start = idx;
            let mut end = idx + 1;
            while end < chars.len() && chars[end].is_ascii_digit() {
                end += 1;
            }

            let suffix: String = chars[end..chars.len().min(end + 12)].iter().collect();
            if suffix.contains('°') || suffix.to_ascii_lowercase().contains("degree") {
                let number: String = chars[start..end].iter().collect();
                return number.parse().ok();
            }

            idx = end;
        } else {
            idx += 1;
        }
    }

    None
}

#[derive(Debug, Deserialize)]
struct GammaEvent {
    slug: String,
    title: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default, rename = "resolutionSource")]
    resolution_source: Option<String>,
    #[serde(default)]
    markets: Vec<GammaMarket>,
}

impl GammaEvent {
    fn into_event_market(self) -> Result<EventMarket> {
        let (kind, city_slug, event_date) = parse_temperature_slug(&self.slug)
            .ok_or_else(|| anyhow!("unsupported temperature event slug: {}", self.slug))?;
        let unit = detect_unit(&self);
        let event_key = format!(
            "{}:{}",
            match kind {
                TemperatureEventKind::Highest => "highest-temperature",
                TemperatureEventKind::Lowest => "lowest-temperature",
            },
            city_slug
        );
        let slug = self.slug;
        let resolution_source = self.resolution_source.filter(|source| !source.is_empty());
        let (buckets, skipped_market_count) = parse_buckets(self.markets)?;

        Ok(EventMarket {
            event_key,
            city_slug,
            kind,
            event_date,
            slug,
            unit,
            resolution_source,
            buckets,
            skipped_market_count,
        })
    }
}

#[derive(Debug, Deserialize)]
struct GammaKeysetPage {
    #[serde(default)]
    events: Vec<GammaEvent>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaMarket {
    question: String,
    outcomes: Value,
    clob_token_ids: Value,
    active: Option<bool>,
    closed: Option<bool>,
    #[serde(default, rename = "acceptingOrders")]
    accepting_orders: Option<bool>,
    #[serde(default, rename = "enableOrderBook")]
    enable_order_book: Option<bool>,
    #[serde(default, rename = "hasReviewedDates")]
    has_reviewed_dates: Option<bool>,
}

impl GammaMarket {
    fn is_tradeable(&self) -> bool {
        self.active == Some(true)
            && self.closed != Some(true)
            && self.accepting_orders == Some(true)
            && self.enable_order_book == Some(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_bucket_temp_stays_unknown() {
        assert_eq!(parse_bucket_temp("highest temperature bucket TBD"), None);
    }

    #[test]
    fn parses_temperature_event_slug() {
        let parsed =
            parse_temperature_slug("lowest-temperature-in-new-york-city-on-april-29-2026").unwrap();
        assert_eq!(parsed.0, TemperatureEventKind::Lowest);
        assert_eq!(parsed.1, "new-york-city");
        assert_eq!(parsed.2, NaiveDate::from_ymd_opt(2026, 4, 29).unwrap());
    }
}
