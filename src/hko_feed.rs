use anyhow::{anyhow, Context, Result};
use chrono::NaiveDateTime;
use tokio::sync::mpsc;
use tokio::time::{self, Duration};
use tracing::{info, warn};

const HKO_MAXMIN_CSV_URL: &str =
    "https://data.weather.gov.hk/weatherAPI/hko_data/regional-weather/latest_since_midnight_maxmin.csv";

#[derive(Debug, Clone, PartialEq)]
pub struct HkoMax {
    pub observed_at: NaiveDateTime,
    pub max_so_far: f64,
}

#[allow(dead_code)]
pub async fn run_hko_feed(
    station: String,
    poll_interval: Duration,
    tx: mpsc::Sender<f64>,
) -> Result<()> {
    let client = reqwest::Client::new();
    let mut last_max: Option<f64> = None;
    let mut interval = time::interval(poll_interval);

    loop {
        interval.tick().await;

        match fetch_hko_max(&client, &station).await {
            Ok(obs) => {
                if last_max != Some(obs.max_so_far) {
                    last_max = Some(obs.max_so_far);
                    info!(station, max_so_far = obs.max_so_far, "hko max changed");
                    if tx.send(obs.max_so_far).await.is_err() {
                        return Ok(());
                    }
                }
            }
            Err(err) => warn!(?err, "hko fetch failed; keeping previous max"),
        }
    }
}

pub async fn fetch_hko_max(client: &reqwest::Client, station: &str) -> Result<HkoMax> {
    let body = client
        .get(HKO_MAXMIN_CSV_URL)
        .send()
        .await
        .context("request HKO CSV")?
        .error_for_status()
        .context("HKO CSV status")?
        .text()
        .await
        .context("read HKO CSV body")?;

    parse_hko_csv(&body, station)
}

pub fn parse_hko_csv(csv_body: &str, station: &str) -> Result<HkoMax> {
    let mut rdr = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_reader(csv_body.as_bytes());

    for record in rdr.records() {
        let record = record.context("read HKO CSV record")?;
        if record.get(1) != Some(station) {
            continue;
        }

        let observed_at = NaiveDateTime::parse_from_str(
            record
                .get(0)
                .ok_or_else(|| anyhow!("missing HKO datetime"))?,
            "%Y%m%d%H%M",
        )
        .context("parse HKO datetime")?;
        let max_so_far = record
            .get(2)
            .ok_or_else(|| anyhow!("missing HKO max temperature"))?
            .parse::<f64>()
            .context("parse HKO max temperature")?;

        return Ok(HkoMax {
            observed_at,
            max_so_far,
        });
    }

    Err(anyhow!("station not found in HKO CSV: {station}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_hko_csv_for_observatory() {
        let csv = "Date time, Automatic Weather Station, Maximum Air Temperature Since Midnight(degree Celsius), Minimum Air Temperature Since Midnight(degree Celsius)\n\
                   202604232230, HK Observatory, 29.0, 23.5\n";

        let parsed = parse_hko_csv(csv, "HK Observatory").unwrap();
        assert_eq!(parsed.max_so_far, 29.0);
        assert_eq!(
            parsed.observed_at.format("%Y%m%d%H%M").to_string(),
            "202604232230"
        );
    }
}
