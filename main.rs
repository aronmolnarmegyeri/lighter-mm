use std::{
    collections::{HashMap, HashSet},
    env, fs,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
    process,
    str::FromStr,
    sync::Arc,
    time::{Duration as StdDuration, Instant},
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use futures_util::StreamExt;
use lighter_client::{
    lighter_client::{LighterClient, OrderSide},
    models,
    types::{AccountId, ApiKeyIndex, BaseQty, Expiry, MarketId, Price},
    ws_client::{OrderBookState, WsEvent},
};
use rust_decimal::{
    Decimal,
    prelude::{FromPrimitive, ToPrimitive},
};
use serde::Deserialize;
use time::Duration as TimeDuration;
use tokio::{pin, signal, sync::watch};
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,lighter_client=warn")),
        )
        .with_target(false)
        .init();

    let cli = CliArgs::parse()?;
    let config = BotConfig::from_path(&cli.config_path)?;
    let client = build_authenticated_client().await?;

    let mut maker = BasicMarketMaker::bootstrap(client, config).await?;
    maker.run().await
}

async fn build_authenticated_client() -> Result<LighterClient> {
    let api_url = env::var("LIGHTER_API_URL")
        .unwrap_or_else(|_| "https://mainnet.zklighter.elliot.ai".to_string());
    let private_key = required_env("LIGHTER_PRIVATE_KEY")?;
    let account_index = required_env("LIGHTER_ACCOUNT_INDEX")?
        .parse::<i64>()
        .context("LIGHTER_ACCOUNT_INDEX must be an integer")?;
    let api_key_index = required_env("LIGHTER_API_KEY_INDEX")?
        .parse::<i32>()
        .context("LIGHTER_API_KEY_INDEX must be an integer")?;
    let signer_lib_path = env::var("LIGHTER_SIGNER_LIB_PATH")
        .unwrap_or_else(|_| "/root/lighter-mm/signer-amd64.so".to_string());

    LighterClient::builder()
        .api_url(api_url)
        .private_key(private_key)
        .account_index(AccountId::new(account_index))
        .api_key_index(ApiKeyIndex::new(api_key_index))
        .signer_library_path(signer_lib_path)
        .build()
        .await
        .context("failed to initialize lighter client")
}

fn required_env(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("set the {name} environment variable"))
}

#[derive(Debug)]
struct CliArgs {
    config_path: PathBuf,
}

impl CliArgs {
    fn parse() -> Result<Self> {
        let mut config_path = PathBuf::from("config.json");
        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "-c" | "--config" => {
                    let value = args
                        .next()
                        .ok_or_else(|| anyhow!("--config expects a file path argument"))?;
                    config_path = PathBuf::from(value);
                }
                "-h" | "--help" => {
                    Self::print_help();
                    process::exit(0);
                }
                unknown => bail!("unknown argument {unknown}; use --help for usage"),
            }
        }

        Ok(Self { config_path })
    }

    fn print_help() {
        println!(concat!(
            "lighter-mm – basic Lighter market making bot\n\n",
            "USAGE:\n",
            "    cargo run --release -- [OPTIONS]\n\n",
            "OPTIONS:\n",
            "    -c, --config <FILE>    Path to config file (default: config.json)\n",
            "    -h, --help             Print this help message\n"
        ));
    }
}

#[derive(Debug, Clone, Deserialize)]
struct BotConfig {
    market_id: i32,
    order_quantity_native: i64,
    quote_update_interval: u64,
    #[serde(default = "default_order_expiry_ms")]
    order_expiry_ms: u64,
    #[serde(default = "default_min_spread_ticks")]
    min_spread_ticks: i64,
    #[serde(default = "default_max_spread_ticks")]
    max_spread_ticks: i64,
    #[serde(default)]
    tight_spread_threshold_ticks: Option<i64>,
    #[serde(default)]
    tight_spread_padding_ticks: Option<i64>,
    #[serde(default = "default_min_tick_move")]
    min_tick_move_for_refresh: i64,
    #[serde(default = "default_quote_staleness_ms")]
    quote_staleness_ms: u64,
    #[serde(default = "default_inventory_target_base_pct")]
    inventory_target_base_pct: f64,
    #[serde(default = "default_inventory_risk_aversion")]
    inventory_risk_aversion: f64,
    #[serde(default)]
    inventory_max_skew_ticks: Option<i64>,
    #[serde(default = "default_inventory_disable_side_on_hard_limit")]
    inventory_disable_side_on_hard_limit: bool,
    #[serde(default = "default_ewma_lambda")]
    ewma_lambda: f64,
    #[serde(default = "default_volatility_spread_multiplier")]
    volatility_spread_multiplier: f64,
    #[serde(default = "default_soft_inventory_threshold")]
    soft_inventory_threshold: f64,
    #[serde(default = "default_hard_inventory_threshold")]
    hard_inventory_threshold: f64,
    #[serde(default = "default_mass_cancel_threshold")]
    mass_cancel_threshold: i64,
    #[serde(default = "default_avellaneda_gamma")]
    avellaneda_gamma: f64,
    #[serde(default = "default_avellaneda_kappa")]
    avellaneda_kappa: f64,
    #[serde(default = "default_avellaneda_time_horizon_secs")]
    avellaneda_time_horizon_secs: f64,
    #[serde(default = "default_order_levels")]
    order_levels: usize,
    #[serde(default = "default_order_level_spacing_ticks")]
    order_level_spacing_ticks: i64,
    #[serde(default = "default_order_level_size_multiplier")]
    order_level_size_multiplier: f64,
}

impl BotConfig {
    fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let contents = fs::read_to_string(&path)
            .with_context(|| format!("failed to read config file {}", path.as_ref().display()))?;
        let mut cfg: BotConfig = serde_json::from_str(&contents)
            .with_context(|| format!("invalid JSON in {}", path.as_ref().display()))?;
        cfg.normalize();
        cfg.validate()?;
        Ok(cfg)
    }

    fn normalize(&mut self) {
        if self.min_spread_ticks < 1 {
            self.min_spread_ticks = 1;
        }
        if self.max_spread_ticks < self.min_spread_ticks {
            self.max_spread_ticks = self.min_spread_ticks;
        }
        if self.min_tick_move_for_refresh < 1 {
            self.min_tick_move_for_refresh = 1;
        }
        if !self.inventory_target_base_pct.is_finite() {
            self.inventory_target_base_pct = default_inventory_target_base_pct();
        }
        self.inventory_target_base_pct = self.inventory_target_base_pct.clamp(0.0, 100.0);
        if !self.inventory_risk_aversion.is_finite() {
            self.inventory_risk_aversion = default_inventory_risk_aversion();
        }
        if self.inventory_risk_aversion < 0.0 {
            self.inventory_risk_aversion = 0.0;
        }
        if let Some(skew) = &mut self.inventory_max_skew_ticks {
            if *skew < 0 {
                *skew = 0;
            }
        }

        // Normalize new parameters
        if !self.ewma_lambda.is_finite() || self.ewma_lambda <= 0.0 || self.ewma_lambda > 1.0 {
            self.ewma_lambda = default_ewma_lambda();
        }
        if !self.volatility_spread_multiplier.is_finite()
            || self.volatility_spread_multiplier <= 0.0
        {
            self.volatility_spread_multiplier = default_volatility_spread_multiplier();
        }
        if !self.soft_inventory_threshold.is_finite()
            || self.soft_inventory_threshold <= 0.0
            || self.soft_inventory_threshold >= 1.0
        {
            self.soft_inventory_threshold = default_soft_inventory_threshold();
        }
        if !self.hard_inventory_threshold.is_finite()
            || self.hard_inventory_threshold <= 0.0
            || self.hard_inventory_threshold >= 1.0
        {
            self.hard_inventory_threshold = default_hard_inventory_threshold();
        }
        if self.hard_inventory_threshold <= self.soft_inventory_threshold {
            self.hard_inventory_threshold = self.soft_inventory_threshold + 0.1;
            if self.hard_inventory_threshold > 1.0 {
                self.hard_inventory_threshold = 1.0;
            }
        }
        if self.mass_cancel_threshold < 0 {
            self.mass_cancel_threshold = default_mass_cancel_threshold();
        }
        if !self.avellaneda_gamma.is_finite() || self.avellaneda_gamma <= 0.0 {
            self.avellaneda_gamma = default_avellaneda_gamma();
        }
        if !self.avellaneda_kappa.is_finite() || self.avellaneda_kappa <= 0.0 {
            self.avellaneda_kappa = default_avellaneda_kappa();
        }
        if !self.avellaneda_time_horizon_secs.is_finite()
            || self.avellaneda_time_horizon_secs <= 0.0
        {
            self.avellaneda_time_horizon_secs = default_avellaneda_time_horizon_secs();
        }
        if self.order_levels == 0 {
            self.order_levels = default_order_levels();
        }
        if self.order_level_spacing_ticks < 1 {
            self.order_level_spacing_ticks = default_order_level_spacing_ticks();
        }
        if !self.order_level_size_multiplier.is_finite() {
            self.order_level_size_multiplier = default_order_level_size_multiplier();
        }
        if self.order_level_size_multiplier < 0.0 {
            self.order_level_size_multiplier = 0.0;
        }
        if self.order_level_size_multiplier > 5.0 {
            self.order_level_size_multiplier = 5.0;
        }
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            self.order_quantity_native > 0,
            "order_quantity_native must be a positive integer"
        );
        ensure!(
            self.quote_update_interval > 0,
            "quote_update_interval must be greater than zero"
        );
        ensure!(self.market_id >= 0, "market_id must be non-negative");
        Ok(())
    }

    fn quote_interval(&self) -> StdDuration {
        StdDuration::from_millis(self.quote_update_interval)
    }

    fn quote_staleness(&self) -> StdDuration {
        let fallback = self
            .quote_update_interval
            .saturating_mul(4)
            .max(self.quote_update_interval);
        let ms = if self.quote_staleness_ms == 0 {
            fallback
        } else {
            self.quote_staleness_ms
        };
        StdDuration::from_millis(ms)
    }

    fn inventory_enabled(&self) -> bool {
        self.inventory_risk_aversion > f64::EPSILON
    }

    fn inventory_target_fraction(&self) -> f64 {
        let clamped = self.inventory_target_base_pct.clamp(0.0, 100.0);
        (clamped / 50.0) - 1.0
    }

    fn inventory_disable_side_on_hard_limit(&self) -> bool {
        self.inventory_disable_side_on_hard_limit
    }

    fn max_inventory_skew(&self) -> i64 {
        self.inventory_max_skew_ticks
            .unwrap_or(self.max_spread_ticks)
            .max(0)
    }

    fn ewma_lambda(&self) -> f64 {
        self.ewma_lambda
    }

    fn volatility_spread_multiplier(&self) -> f64 {
        self.volatility_spread_multiplier
    }

    fn soft_inventory_threshold(&self) -> f64 {
        self.soft_inventory_threshold
    }

    fn hard_inventory_threshold(&self) -> f64 {
        self.hard_inventory_threshold
    }

    fn mass_cancel_threshold(&self) -> i64 {
        self.mass_cancel_threshold
    }

    fn avellaneda_gamma(&self) -> f64 {
        self.avellaneda_gamma
    }

    fn avellaneda_kappa(&self) -> f64 {
        self.avellaneda_kappa
    }

    fn avellaneda_time_horizon_secs(&self) -> f64 {
        self.avellaneda_time_horizon_secs
    }

    fn order_levels(&self) -> usize {
        self.order_levels
    }

    fn order_level_spacing_ticks(&self) -> i64 {
        self.order_level_spacing_ticks
    }

    fn order_level_size_multiplier(&self) -> f64 {
        self.order_level_size_multiplier
    }
}

#[derive(Debug, Clone)]
struct MarketMetadata {
    symbol: String,
    price_scale: i64,
    size_scale: i64,
    min_base_ticks: i64,
}

impl MarketMetadata {
    async fn load(client: &LighterClient, market: MarketId) -> Result<Self> {
        let details = client
            .orders()
            .book_details(Some(market))
            .await
            .context("failed to fetch market metadata")?;

        let market_i32 = market.into_inner();
        let detail = details
            .order_book_details
            .into_iter()
            .find(|detail| detail.market_id == market_i32)
            .ok_or_else(|| anyhow!("metadata for market {market_i32} was not returned"))?;

        ensure!(
            detail.price_decimals >= 0,
            "price decimals cannot be negative for market {}",
            detail.market_id
        );
        let decimals = detail.price_decimals as u32;
        let scale_i128 = 10_i128.pow(decimals);
        ensure!(
            scale_i128 <= i64::MAX as i128,
            "price scale would overflow for market {}",
            detail.market_id
        );
        let price_scale = scale_i128 as i64;

        ensure!(
            detail.size_decimals >= 0,
            "size decimals cannot be negative for market {}",
            detail.market_id
        );
        let size_decimals = detail.size_decimals as u32;
        let size_scale_i128 = 10_i128.pow(size_decimals);
        ensure!(
            size_scale_i128 <= i64::MAX as i128,
            "size scale would overflow for market {}",
            detail.market_id
        );
        let size_scale = size_scale_i128 as i64;

        let min_base_ticks =
            parse_scaled_amount(&detail.min_base_amount, size_scale, "min base amount")
                .context("failed to parse minimum base amount")?;
        ensure!(
            min_base_ticks > 0,
            "market {} reported non-positive minimum base amount",
            detail.market_id
        );

        info!(
            market = detail.market_id,
            symbol = %detail.symbol,
            price_decimals = detail.price_decimals,
            price_scale,
            size_decimals = detail.size_decimals,
            size_scale,
            min_base_amount = %detail.min_base_amount,
            min_quote_amount = %detail.min_quote_amount,
            "Loaded market metadata"
        );

        Ok(Self {
            symbol: detail.symbol,
            price_scale,
            size_scale,
            min_base_ticks,
        })
    }

    fn price_scale(&self) -> i64 {
        self.price_scale
    }

    fn size_scale(&self) -> i64 {
        self.size_scale
    }

    fn min_base_ticks(&self) -> i64 {
        self.min_base_ticks
    }

    fn min_size_scale(&self, base_qty_ticks: i64) -> f64 {
        if base_qty_ticks <= 0 {
            return 1.0;
        }
        (self.min_base_ticks as f64 / base_qty_ticks as f64).clamp(0.0, 1.0)
    }

    fn symbol(&self) -> &str {
        &self.symbol
    }
}

const MIN_VOLATILITY_PCT: f64 = 1e-6;
const MAX_VOLATILITY_PCT: f64 = 0.05;
const DEFAULT_VOLATILITY_PCT: f64 = 1e-4;

#[derive(Debug, Clone)]
struct EwmaVolatilityCalculator {
    lambda: f64,
    current_variance: f64,
    last_mid_price: Option<f64>,
    last_volatility: f64,
}

impl EwmaVolatilityCalculator {
    fn new(lambda: f64) -> Self {
        Self {
            lambda,
            current_variance: 0.0,
            last_mid_price: None,
            last_volatility: DEFAULT_VOLATILITY_PCT,
        }
    }

    fn update(&mut self, mid_price: f64) -> f64 {
        if !mid_price.is_finite() || mid_price <= 0.0 {
            return self.last_volatility;
        }

        let sigma = match self.last_mid_price {
            None => {
                self.last_mid_price = Some(mid_price);
                self.last_volatility
            }
            Some(last_mid) => {
                if !last_mid.is_finite() || last_mid <= 0.0 {
                    self.last_mid_price = Some(mid_price);
                    return self.last_volatility;
                }

                let ratio = (mid_price / last_mid).max(f64::MIN_POSITIVE);
                let log_return = ratio.ln();
                if !log_return.is_finite() {
                    self.last_mid_price = Some(mid_price);
                    return self.last_volatility;
                }

                let squared = log_return * log_return;
                self.current_variance =
                    self.lambda * self.current_variance + (1.0 - self.lambda) * squared;
                if !self.current_variance.is_finite() || self.current_variance.is_sign_negative() {
                    self.current_variance = 0.0;
                }
                self.last_mid_price = Some(mid_price);
                self.current_variance.sqrt()
            }
        };

        self.sanitize(sigma)
    }

    fn sanitize(&mut self, candidate: f64) -> f64 {
        if candidate.is_finite() && candidate > 0.0 {
            let clamped = candidate.clamp(MIN_VOLATILITY_PCT, MAX_VOLATILITY_PCT);
            self.last_volatility = clamped;
            clamped
        } else {
            self.last_volatility
        }
    }
}

#[derive(Clone)]
struct OrderBookFeed {
    tx: watch::Sender<Option<TimedBookSnapshot>>,
    rx: watch::Receiver<Option<TimedBookSnapshot>>,
}

impl OrderBookFeed {
    fn start(client: Arc<LighterClient>, market: MarketId, price_scale: i64) -> Self {
        let (tx, rx) = watch::channel(None);
        Self::spawn_ws_loop(tx.clone(), client, market, price_scale);
        Self { tx, rx }
    }

    fn latest_if_fresh(&self, max_age: StdDuration) -> Option<BookSnapshot> {
        let snapshot = self.rx.borrow().clone()?;
        if snapshot.updated_at.elapsed() <= max_age {
            Some(snapshot.snapshot)
        } else {
            None
        }
    }

    fn publish(&self, snapshot: BookSnapshot) {
        let _ = self.tx.send(Some(TimedBookSnapshot::new(snapshot)));
    }

    fn spawn_ws_loop(
        tx: watch::Sender<Option<TimedBookSnapshot>>,
        client: Arc<LighterClient>,
        market: MarketId,
        price_scale: i64,
    ) {
        tokio::spawn(async move {
            loop {
                match client.ws().subscribe_order_book(market).connect().await {
                    Ok(mut stream) => {
                        info!(
                            market = market.into_inner(),
                            "Connected to order book stream"
                        );
                        while let Some(event) = stream.next().await {
                            match event {
                                Ok(WsEvent::OrderBook(update)) => {
                                    match BookSnapshot::from_order_book_state(
                                        &update.state,
                                        price_scale,
                                    ) {
                                        Ok(snapshot) => {
                                            if tx
                                                .send(Some(TimedBookSnapshot::new(snapshot)))
                                                .is_err()
                                            {
                                                return;
                                            }
                                        }
                                        Err(err) => warn!(
                                            ?err,
                                            "failed to parse websocket order book update"
                                        ),
                                    }
                                }
                                Ok(WsEvent::Connected) | Ok(WsEvent::Pong) => {}
                                Ok(WsEvent::Account(_)) => {}
                                Ok(WsEvent::Unknown(payload)) => {
                                    warn!(payload, "Unhandled websocket event")
                                }
                                Ok(WsEvent::Closed(frame)) => {
                                    if let Some(info) = frame {
                                        warn!(
                                            code = info.code,
                                            reason = %info.reason,
                                            "Order book stream closed"
                                        );
                                    } else {
                                        warn!("Order book stream closed");
                                    }
                                    break;
                                }
                                Err(err) => {
                                    warn!(?err, "Order book stream error");
                                    break;
                                }
                            }
                        }
                    }
                    Err(err) => warn!(?err, "failed to connect order book stream"),
                }

                if tx.is_closed() {
                    break;
                }
                tokio::time::sleep(StdDuration::from_secs(1)).await;
            }
        });
    }
}

#[derive(Clone, Copy, Debug)]
struct TimedBookSnapshot {
    snapshot: BookSnapshot,
    updated_at: Instant,
}

impl TimedBookSnapshot {
    fn new(snapshot: BookSnapshot) -> Self {
        Self {
            snapshot,
            updated_at: Instant::now(),
        }
    }
}

struct BasicMarketMaker {
    client: Arc<LighterClient>,
    cfg: BotConfig,
    market: MarketId,
    base_qty: BaseQty,
    expiry_ttl: TimeDuration,
    quote_interval: StdDuration,
    quote_staleness: StdDuration,
    market_meta: MarketMetadata,
    order_book_feed: OrderBookFeed,
    volatility_calculator: EwmaVolatilityCalculator,
    last_mid: Option<i64>,
    last_refresh: Option<Instant>,
    cancel_count: i64,
}

impl BasicMarketMaker {
    async fn bootstrap(client: LighterClient, cfg: BotConfig) -> Result<Self> {
        let client = Arc::new(client);
        let market = MarketId::new(cfg.market_id);
        let market_meta = MarketMetadata::load(client.as_ref(), market).await?;
        Self::new(client, cfg, market_meta)
    }

    fn new(
        client: Arc<LighterClient>,
        cfg: BotConfig,
        market_meta: MarketMetadata,
    ) -> Result<Self> {
        let market = MarketId::new(cfg.market_id);
        let base_qty = BaseQty::try_from(cfg.order_quantity_native).map_err(|err| anyhow!(err))?;
        ensure!(
            base_qty.into_inner() >= market_meta.min_base_ticks(),
            "order_quantity_native {} is below market minimum {}",
            base_qty.into_inner(),
            market_meta.min_base_ticks()
        );
        let expiry_ttl = if cfg.order_expiry_ms == 0 {
            TimeDuration::minutes(5)
        } else {
            TimeDuration::milliseconds((cfg.order_expiry_ms).min(i64::MAX as u64) as i64)
        };
        let order_book_feed =
            OrderBookFeed::start(client.clone(), market, market_meta.price_scale());
        let volatility_calculator = EwmaVolatilityCalculator::new(cfg.ewma_lambda());

        info!(
            market = cfg.market_id,
            qty = cfg.order_quantity_native,
            min_spread = cfg.min_spread_ticks,
            max_spread = cfg.max_spread_ticks,
            price_scale = market_meta.price_scale(),
            ewma_lambda = cfg.ewma_lambda(),
            "Configured basic market maker"
        );

        Ok(Self {
            client,
            market,
            base_qty,
            quote_interval: cfg.quote_interval(),
            quote_staleness: cfg.quote_staleness(),
            expiry_ttl,
            cfg,
            market_meta,
            order_book_feed,
            volatility_calculator,
            last_mid: None,
            last_refresh: None,
            cancel_count: 0,
        })
    }

    async fn run(&mut self) -> Result<()> {
        info!(
            market = self.market.into_inner(),
            interval_ms = self.quote_interval.as_millis(),
            "Starting market making loop"
        );

        let shutdown = signal::ctrl_c();
        pin!(shutdown);
        let mut interval = tokio::time::interval(self.quote_interval);

        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    info!("Shutdown signal received; cancelling resting orders");
                    if let Err(err) = self.cancel_active_market_orders().await {
                        warn!(?err, "failed to cancel resting orders on shutdown");
                    }
                    break;
                }
                _ = interval.tick() => {
                    if let Err(err) = self.tick().await {
                        warn!(?err, "tick failed");
                    }
                }
            }
        }

        Ok(())
    }

    async fn tick(&mut self) -> Result<()> {
        let snapshot =
            if let Some(snapshot) = self.order_book_feed.latest_if_fresh(self.quote_staleness) {
                snapshot
            } else {
                info!(
                    market = self.market.into_inner(),
                    "Order book stream stale; refreshing via REST"
                );
                let book = self.client.orders().book(self.market, 25).await?;
                match BookSnapshot::from_order_book(&book, self.market_meta.price_scale()) {
                    Ok(value) => {
                        self.order_book_feed.publish(value);
                        value
                    }
                    Err(err) => {
                        warn!(?err, "skipping tick due to incomplete order book");
                        return Ok(());
                    }
                }
            };

        let inventory = if self.cfg.inventory_enabled() {
            match self.inventory_state().await {
                Ok(state) => Some(state),
                Err(err) => {
                    warn!(
                        ?err,
                        "failed to fetch inventory state; continuing without skew"
                    );
                    None
                }
            }
        } else {
            None
        };

        // Update volatility with current mid price
        let mid_price =
            snapshot.best_bid as f64 + ((snapshot.best_ask - snapshot.best_bid) as f64 / 2.0);
        let current_volatility = self.volatility_calculator.update(mid_price);

        let plan = self.build_quote_plan(&snapshot, inventory.as_ref(), current_volatility);
        if let Some(state) = inventory.as_ref() {
            self.log_inventory_state(state, &plan);
        }

        if !plan.has_orders() {
            info!(
                reservation_mid = plan.reservation_mid,
                "No actionable quotes; inventory guard engaged"
            );
            self.cancel_active_market_orders().await?;
            self.last_mid = Some(plan.reservation_mid);
            return Ok(());
        }
        let force_refresh = self.should_force_refresh(plan.reservation_mid);

        let updated = self.reconcile_orders(&plan, force_refresh).await?;
        self.last_mid = Some(plan.reservation_mid);
        if updated {
            self.last_refresh = Some(Instant::now());
            self.log_quote_refresh(&plan, current_volatility);
        }

        Ok(())
    }

    async fn inventory_state(&self) -> Result<InventoryState> {
        let details = self.client.account().details().await?;
        let account = details
            .accounts
            .first()
            .ok_or_else(|| anyhow!("account details response contained no accounts"))?;

        let collateral = parse_decimal_value(&account.collateral, "collateral")?;
        let collateral = collateral.max(Decimal::ZERO);
        let target_fraction = self.cfg.inventory_target_fraction();

        let maybe_position = account
            .positions
            .iter()
            .find(|position| position.market_id == self.market.into_inner());

        let (position_qty, position_value_abs) = if let Some(position) = maybe_position {
            let qty = parse_decimal_value(&position.position, "position")?;
            let value = parse_decimal_value(&position.position_value, "position_value")?.abs();
            (qty, value)
        } else {
            (Decimal::ZERO, Decimal::ZERO)
        };

        let total_value = collateral + position_value_abs;
        let ratio = if total_value.is_zero() {
            Decimal::ZERO
        } else {
            position_value_abs / total_value
        };
        let ratio = ratio
            .to_f64()
            .ok_or_else(|| anyhow!("inventory ratio exceeds f64 range"))?;

        let direction = if position_qty.is_zero() {
            0.0
        } else if position_qty.is_sign_negative() {
            -1.0
        } else {
            1.0
        };

        let allocation_fraction = (ratio * direction).clamp(-1.0, 1.0);
        let diff = (allocation_fraction - target_fraction).clamp(-1.0, 1.0);
        let skew_ticks = self.compute_inventory_skew(diff);

        Ok(InventoryState {
            position_qty,
            position_value_abs,
            collateral,
            allocation_fraction,
            target_pct: self.cfg.inventory_target_base_pct,
            diff_fraction: diff,
            skew_ticks,
        })
    }

    fn compute_inventory_skew(&self, diff_fraction: f64) -> i64 {
        let abs_diff = diff_fraction.abs().clamp(0.0, 1.0);
        if abs_diff <= f64::EPSILON {
            return 0;
        }

        let max_skew = self.cfg.max_inventory_skew() as f64;
        if max_skew <= 0.0 {
            return 0;
        }

        let hard = self.cfg.hard_inventory_threshold().clamp(1e-6, 1.0);
        if hard <= 0.0 {
            return 0;
        }

        let raw_soft = self.cfg.soft_inventory_threshold().clamp(0.0, hard);
        let soft_target = self.cfg.inventory_risk_aversion.max(0.0).min(max_skew);

        let skew = if abs_diff >= hard {
            max_skew
        } else if raw_soft <= f64::EPSILON {
            let ratio = (abs_diff / hard).clamp(0.0, 1.0);
            ratio * max_skew
        } else if abs_diff <= raw_soft {
            let ratio = (abs_diff / raw_soft).clamp(0.0, 1.0);
            ratio * soft_target
        } else {
            let range = (hard - raw_soft).max(f64::EPSILON);
            let ratio = ((abs_diff - raw_soft) / range).clamp(0.0, 1.0);
            let baseline = soft_target.min(max_skew);
            baseline + ratio * (max_skew - baseline)
        };

        let skew_ticks = skew.clamp(0.0, max_skew).round() as i64;

        if diff_fraction.is_sign_negative() {
            -skew_ticks
        } else {
            skew_ticks
        }
    }

    fn log_inventory_state(&self, state: &InventoryState, plan: &QuotePlan) {
        info!(
            market = self.market.into_inner(),
            symbol = %self.market_meta.symbol(),
            position_qty = %state.position_qty,
            position_value = %state.position_value_abs,
            collateral = %state.collateral,
            allocation_pct = format_args!("{:.2}", state.allocation_pct()),
            target_pct = format_args!("{:.2}", state.target_pct),
            diff_pct = format_args!("{:.2}", state.diff_pct()),
            skew_ticks = state.skew_ticks,
            applied_skew = plan.inventory_skew_ticks,
            "Inventory assessment"
        );
    }

    fn build_quote_plan(
        &self,
        snapshot: &BookSnapshot,
        inventory: Option<&InventoryState>,
        volatility: f64,
    ) -> QuotePlan {
        let (reservation_mid, avellaneda_spread) =
            self.avellaneda_quote_targets(snapshot, inventory, volatility);
        let spread = self.derive_spread(snapshot.spread(), volatility, avellaneda_spread);
        let mut plan = QuotePlan::new(snapshot.mid(), spread);
        plan.reservation_mid = reservation_mid;

        if let Some(state) = inventory {
            let disable_on_hard = self.cfg.inventory_disable_side_on_hard_limit();
            let min_size_scale = self.market_meta.min_size_scale(self.base_qty.into_inner());
            plan.apply_as_style_skew(
                state.diff_fraction,
                self.cfg.soft_inventory_threshold(),
                self.cfg.hard_inventory_threshold(),
                disable_on_hard,
                min_size_scale,
            );
        }

        plan.build_levels(self.base_qty, &self.cfg, snapshot);
        plan.inventory_skew_ticks = plan.mid - plan.reservation_mid;
        plan
    }

    fn derive_spread(&self, book_spread: i64, volatility: f64, avellaneda_spread: i64) -> i64 {
        let mut spread = avellaneda_spread.max(self.cfg.min_spread_ticks);

        let volatility_component =
            (volatility.abs() * self.cfg.volatility_spread_multiplier() * 1_000.0).round() as i64;
        spread = spread.max(volatility_component);

        if let Some(threshold) = self.cfg.tight_spread_threshold_ticks {
            if book_spread <= threshold {
                let padding = self
                    .cfg
                    .tight_spread_padding_ticks
                    .unwrap_or_default()
                    .max(0);
                spread = spread.max(threshold + padding);
            }
        }

        spread = spread.clamp(self.cfg.min_spread_ticks, self.cfg.max_spread_ticks);
        spread.max(1)
    }

    fn avellaneda_quote_targets(
        &self,
        snapshot: &BookSnapshot,
        inventory: Option<&InventoryState>,
        volatility: f64,
    ) -> (i64, i64) {
        let mid = snapshot.mid();
        let inventory_fraction = inventory
            .map(|state| state.allocation_fraction)
            .unwrap_or(0.0);
        let gamma = self.cfg.avellaneda_gamma().max(f64::EPSILON);
        let kappa = self.cfg.avellaneda_kappa().max(f64::EPSILON);
        let horizon = self.cfg.avellaneda_time_horizon_secs().max(1.0);
        let sigma_pct = if volatility.is_finite() {
            volatility.abs()
        } else {
            0.0
        };
        let sigma_abs = if sigma_pct > 0.0 {
            sigma_pct * mid as f64
        } else {
            snapshot.spread() as f64
        };

        let risk_term = gamma * sigma_abs * horizon;
        let poisson_term = (2.0 / gamma) * (1.0 + (gamma / kappa)).ln();
        let avellaneda_spread = (risk_term + poisson_term)
            .round()
            .clamp(1.0, i64::MAX as f64) as i64;

        let shift = (inventory_fraction * gamma * sigma_abs * horizon)
            .round()
            .clamp(-(i64::MAX as f64), i64::MAX as f64) as i64;
        let reservation_mid = if shift >= 0 {
            mid.saturating_sub(shift)
        } else {
            mid.saturating_add(shift.abs())
        };

        (reservation_mid.max(1), avellaneda_spread.max(1))
    }

    fn should_force_refresh(&self, new_reservation_mid: i64) -> bool {
        if self.last_refresh.is_none() || self.last_mid.is_none() {
            return true;
        }

        if let Some(previous_mid) = self.last_mid {
            if (previous_mid - new_reservation_mid).abs() >= self.cfg.min_tick_move_for_refresh {
                return true;
            }
        }

        if let Some(last_refresh) = self.last_refresh {
            if last_refresh.elapsed() >= self.quote_staleness {
                return true;
            }
        }

        false
    }

    fn log_quote_refresh(&self, plan: &QuotePlan, volatility: f64) {
        info!(
            bid_levels = %format_levels(&plan.bids),
            ask_levels = %format_levels(&plan.asks),
            reservation_mid = plan.reservation_mid,
            spread = plan.spread,
            inv_skew = plan.inventory_skew_ticks,
            bid_enabled = plan.bid_enabled,
            ask_enabled = plan.ask_enabled,
            bid_scale = format_args!("{:.2}", plan.bid_size_scale),
            ask_scale = format_args!("{:.2}", plan.ask_size_scale),
            volatility = format_args!("{:.6}", volatility),
            "Quotes refreshed"
        );
        if let Some(reason) = plan.bid_disabled_reason {
            info!(reason, "Bid side disabled");
        }
        if let Some(reason) = plan.ask_disabled_reason {
            info!(reason, "Ask side disabled");
        }
    }

    fn log_active_order_snapshot(&self, orders: &[models::Order]) {
        if orders.is_empty() {
            info!(
                market = self.market.into_inner(),
                "Fetched active orders snapshot (none resting)"
            );
            return;
        }

        let entries: Vec<String> = orders
            .iter()
            .map(|order| {
                let side = if order.is_ask { "Ask" } else { "Bid" };
                format!(
                    "{}#{}:{}@{} rem={}",
                    side,
                    order.order_index,
                    order.base_size,
                    order.price,
                    order.remaining_base_amount
                )
            })
            .collect();

        info!(
            market = self.market.into_inner(),
            count = orders.len(),
            orders = %entries.join(","),
            "Fetched active orders snapshot"
        );
    }

    async fn reconcile_orders(&mut self, plan: &QuotePlan, force_refresh: bool) -> Result<bool> {
        let active = self.client.account().active_orders(self.market).await?;
        self.log_active_order_snapshot(&active.orders);
        let mut cancel_ids = Vec::new();
        let mut satisfied = HashSet::new();
        let desired: HashMap<QuoteKey, PlannedQuote> = plan
            .bids
            .iter()
            .chain(plan.asks.iter())
            .map(|quote| (quote.key(), *quote))
            .collect();
        let mut active_views = Vec::with_capacity(active.orders.len());

        for order in &active.orders {
            match ActiveOrderView::from_order(order, &self.market_meta) {
                Ok(value) => active_views.push(value),
                Err(err) => {
                    warn!(
                        ?err,
                        order_index = order.order_index,
                        "malformed active order; scheduling cancel"
                    );
                    cancel_ids.push(order.order_index);
                }
            };
        }

        if force_refresh
            && cancel_ids.is_empty()
            && self.plan_matches_active(&desired, &active_views)
        {
            debug!(
                market = self.market.into_inner(),
                "Quotes unchanged; skipping forced refresh"
            );
            return Ok(false);
        }

        for view in &active_views {
            if force_refresh {
                cancel_ids.push(view.order_index);
                continue;
            }

            let key = view.key();
            if let Some(target) = desired.get(&key) {
                if view.remaining >= target.qty_ticks() {
                    satisfied.insert(key);
                    continue;
                }
            }

            cancel_ids.push(view.order_index);
        }

        let mut changed = false;
        if !cancel_ids.is_empty() {
            self.execute_cancellations(&cancel_ids).await?;
            changed = true;
        }

        let mut missing = Vec::new();
        for quote in &plan.bids {
            if !satisfied.contains(&quote.key()) {
                missing.push(*quote);
            }
        }
        for quote in &plan.asks {
            if !satisfied.contains(&quote.key()) {
                missing.push(*quote);
            }
        }

        for quote in missing {
            self.place_order(quote.side, quote.base_qty, quote.price)
                .await?;
            changed = true;
        }

        Ok(changed)
    }

    fn plan_matches_active(
        &self,
        desired: &HashMap<QuoteKey, PlannedQuote>,
        active: &[ActiveOrderView],
    ) -> bool {
        if desired.len() != active.len() {
            return false;
        }

        for view in active {
            match desired.get(&view.key()) {
                Some(target) if view.remaining >= target.qty_ticks() => continue,
                _ => return false,
            }
        }

        true
    }

    async fn cancel_active_market_orders(&mut self) -> Result<()> {
        let active = self.client.account().active_orders(self.market).await?;
        if active.orders.is_empty() {
            return Ok(());
        }
        let ids: Vec<i64> = active
            .orders
            .iter()
            .map(|order| order.order_index)
            .collect();
        self.execute_cancellations(&ids).await
    }

    async fn execute_cancellations(&mut self, order_ids: &[i64]) -> Result<()> {
        info!(
            count = order_ids.len(),
            "Cancelling {} resting orders",
            order_ids.len()
        );
        for order_id in order_ids {
            match self.client.cancel(self.market, *order_id).submit().await {
                Ok(_) => info!(order_index = *order_id, "Cancelled order"),
                Err(err) => warn!(?err, order_index = *order_id, "failed to cancel order"),
            }
        }
        if !order_ids.is_empty() {
            self.cancel_count += order_ids.len() as i64;
            if self.cancel_count >= self.cfg.mass_cancel_threshold() {
                warn!(
                    cancel_count = self.cancel_count,
                    threshold = self.cfg.mass_cancel_threshold(),
                    "High cancellation rate detected; check price/volume filters"
                );
                self.cancel_count = 0;
            }
        } else {
            self.cancel_count = 0;
        }
        Ok(())
    }

    async fn place_order(&self, side: OrderSide, qty: BaseQty, price: Price) -> Result<()> {
        let builder = self.client.order(self.market);
        let builder = match side {
            OrderSide::Bid => builder.buy(),
            OrderSide::Ask => builder.sell(),
        };

        let submission = builder
            .auto_client_id()
            .qty(qty)
            .limit(price)
            .post_only()
            .expires_at(self.order_expiry())
            .submit()
            .await
            .context("failed to submit limit order")?;

        info!(
            side = ?side,
            price = price.into_ticks(),
            qty = qty.into_inner(),
            tx = submission.response().tx_hash,
            "Submitted limit order"
        );

        Ok(())
    }

    fn order_expiry(&self) -> Expiry {
        Expiry::from_now(self.expiry_ttl)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct QuoteKey {
    side: OrderSide,
    price_ticks: i64,
}

impl Hash for QuoteKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let side_tag: u8 = match self.side {
            OrderSide::Bid => 0,
            OrderSide::Ask => 1,
        };
        state.write_u8(side_tag);
        self.price_ticks.hash(state);
    }
}

#[derive(Debug, Clone, Copy)]
struct PlannedQuote {
    side: OrderSide,
    price: Price,
    price_ticks: i64,
    base_qty: BaseQty,
    level: usize,
}

impl PlannedQuote {
    fn key(&self) -> QuoteKey {
        QuoteKey {
            side: self.side,
            price_ticks: self.price_ticks,
        }
    }

    fn qty_ticks(&self) -> i64 {
        self.base_qty.into_inner()
    }
}

#[derive(Debug)]
struct QuotePlan {
    mid: i64,
    spread: i64,
    reservation_mid: i64,
    inventory_skew_ticks: i64,
    bid_enabled: bool,
    ask_enabled: bool,
    bid_disabled_reason: Option<&'static str>,
    ask_disabled_reason: Option<&'static str>,
    bid_size_scale: f64,
    ask_size_scale: f64,
    bids: Vec<PlannedQuote>,
    asks: Vec<PlannedQuote>,
}

impl QuotePlan {
    fn new(mid: i64, spread: i64) -> Self {
        Self {
            mid,
            spread: spread.max(1),
            reservation_mid: mid.max(1),
            inventory_skew_ticks: 0,
            bid_enabled: true,
            ask_enabled: true,
            bid_disabled_reason: None,
            ask_disabled_reason: None,
            bid_size_scale: 1.0,
            ask_size_scale: 1.0,
            bids: Vec::new(),
            asks: Vec::new(),
        }
    }

    fn disable_side(&mut self, side: OrderSide, reason: &'static str) {
        match side {
            OrderSide::Bid => {
                self.bid_enabled = false;
                self.bid_disabled_reason = Some(reason);
                self.bids.clear();
            }
            OrderSide::Ask => {
                self.ask_enabled = false;
                self.ask_disabled_reason = Some(reason);
                self.asks.clear();
            }
        }
    }

    fn apply_as_style_skew(
        &mut self,
        diff_fraction: f64,
        soft_threshold: f64,
        hard_threshold: f64,
        disable_on_hard_limit: bool,
        min_size_scale: f64,
    ) {
        self.bid_size_scale = 1.0;
        self.ask_size_scale = 1.0;

        let abs_diff = diff_fraction.abs();

        if hard_threshold > 0.0 && abs_diff >= hard_threshold {
            if disable_on_hard_limit {
                if diff_fraction > 0.0 {
                    self.disable_side(OrderSide::Bid, "inventory hard limit");
                } else if diff_fraction < 0.0 {
                    self.disable_side(OrderSide::Ask, "inventory hard limit");
                }
            } else {
                self.apply_hard_limit_bias(diff_fraction, min_size_scale);
            }
            return;
        }

        if soft_threshold > 0.0 && hard_threshold > soft_threshold && abs_diff >= soft_threshold {
            let denominator = (hard_threshold - soft_threshold).max(f64::EPSILON);
            let skew_factor = ((abs_diff - soft_threshold) / denominator).clamp(0.0, 1.0);
            let size_scale = inventory_size_scale(skew_factor, min_size_scale);
            let boosted_scale = inventory_boost_scale(skew_factor);

            if diff_fraction > 0.0 {
                self.bid_size_scale = size_scale;
                self.ask_size_scale = boosted_scale;
            } else if diff_fraction < 0.0 {
                self.ask_size_scale = size_scale;
                self.bid_size_scale = boosted_scale;
            }
        }
    }

    fn apply_hard_limit_bias(&mut self, diff_fraction: f64, min_size_scale: f64) {
        const HARD_LIMIT_OFFLOAD_BOOST: f64 = 1.0;
        let limited_scale = min_size_scale.clamp(0.05, 1.0);
        let boosted_scale = 1.0 + HARD_LIMIT_OFFLOAD_BOOST;

        if diff_fraction > 0.0 {
            self.bid_size_scale = limited_scale;
            self.ask_size_scale = boosted_scale;
        } else if diff_fraction < 0.0 {
            self.ask_size_scale = limited_scale;
            self.bid_size_scale = boosted_scale;
        }
    }

    fn build_levels(&mut self, base_qty: BaseQty, cfg: &BotConfig, snapshot: &BookSnapshot) {
        self.bids.clear();
        self.asks.clear();

        if self.bid_enabled {
            self.bids = Self::build_side(
                OrderSide::Bid,
                base_qty,
                cfg,
                self.reservation_mid,
                self.spread,
                snapshot,
                self.bid_size_scale,
            );
        }

        if self.ask_enabled {
            self.asks = Self::build_side(
                OrderSide::Ask,
                base_qty,
                cfg,
                self.reservation_mid,
                self.spread,
                snapshot,
                self.ask_size_scale,
            );
        }
    }

    fn build_side(
        side: OrderSide,
        base_qty: BaseQty,
        cfg: &BotConfig,
        reservation_mid: i64,
        spread: i64,
        snapshot: &BookSnapshot,
        side_size_scale: f64,
    ) -> Vec<PlannedQuote> {
        let mut levels = Vec::new();
        let half = spread / 2;
        let inverse_half = spread - half;
        let side_scale = side_size_scale.clamp(0.05, 10.0);

        for level in 0..cfg.order_levels() {
            let spacing = cfg.order_level_spacing_ticks().saturating_mul(level as i64);
            let price_ticks = match side {
                OrderSide::Bid => reservation_mid
                    .saturating_sub(half + spacing)
                    .max(1)
                    .min(snapshot.best_ask.saturating_sub(1)),
                OrderSide::Ask => reservation_mid
                    .saturating_add(inverse_half + spacing)
                    .max(snapshot.best_bid.saturating_add(1)),
            };

            if side == OrderSide::Bid && price_ticks >= snapshot.best_ask {
                warn!(
                    side = "Bid",
                    level,
                    price_ticks,
                    best_ask = snapshot.best_ask,
                    "Bid level would cross or touch best ask; skipping remaining bid levels"
                );
                break;
            }
            if side == OrderSide::Ask && price_ticks <= snapshot.best_bid {
                warn!(
                    side = "Ask",
                    level,
                    price_ticks,
                    best_bid = snapshot.best_bid,
                    "Ask level would cross or touch best bid; skipping remaining ask levels"
                );
                break;
            }

            let level_multiplier =
                (1.0 + (level as f64 * cfg.order_level_size_multiplier())).max(0.0) * side_scale;
            let qty = match scale_base_qty(base_qty, level_multiplier) {
                Some(qty) => qty,
                None => continue,
            };
            levels.push(PlannedQuote {
                side,
                price: Price::ticks(price_ticks),
                price_ticks,
                base_qty: qty,
                level,
            });
        }

        levels
    }

    fn has_orders(&self) -> bool {
        !self.bids.is_empty() || !self.asks.is_empty()
    }
}

fn scale_base_qty(base: BaseQty, multiplier: f64) -> Option<BaseQty> {
    if !multiplier.is_finite() || multiplier <= 0.0 {
        return None;
    }
    if (multiplier - 1.0).abs() < f64::EPSILON {
        return Some(base);
    }
    let scaled = (base.into_inner() as f64 * multiplier)
        .round()
        .clamp(1.0, i64::MAX as f64) as i64;
    BaseQty::try_from(scaled).ok()
}

fn inventory_size_scale(skew_factor: f64, min_size_scale: f64) -> f64 {
    const ABS_MIN_SCALE: f64 = 0.15;
    let floor = ABS_MIN_SCALE.max(min_size_scale.clamp(0.0, 1.0));
    (1.0 - skew_factor).clamp(floor, 1.0)
}

fn inventory_boost_scale(skew_factor: f64) -> f64 {
    (1.0 + skew_factor * 0.5).clamp(1.0, 5.0)
}

fn format_levels(levels: &[PlannedQuote]) -> String {
    if levels.is_empty() {
        return "-".to_string();
    }
    levels
        .iter()
        .map(|quote| {
            format!(
                "L{}:{}@{}",
                quote.level,
                quote.price_ticks,
                quote.base_qty.into_inner()
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

#[derive(Debug, Clone, Copy)]
struct BookSnapshot {
    best_bid: i64,
    best_ask: i64,
}

impl BookSnapshot {
    fn from_order_book(book: &models::OrderBookOrders, price_scale: i64) -> Result<Self> {
        let best_bid_order = book
            .bids
            .first()
            .ok_or_else(|| anyhow!("order book did not contain bids"))?;
        let best_ask_order = book
            .asks
            .first()
            .ok_or_else(|| anyhow!("order book did not contain asks"))?;
        Self::from_price_pair(&best_bid_order.price, &best_ask_order.price, price_scale)
    }

    fn from_order_book_state(state: &OrderBookState, price_scale: i64) -> Result<Self> {
        let best_bid_level = state
            .bids
            .first()
            .ok_or_else(|| anyhow!("order book stream did not contain bids"))?;
        let best_ask_level = state
            .asks
            .first()
            .ok_or_else(|| anyhow!("order book stream did not contain asks"))?;
        Self::from_price_pair(&best_bid_level.price, &best_ask_level.price, price_scale)
    }

    fn from_price_pair(bid: &str, ask: &str, price_scale: i64) -> Result<Self> {
        let best_bid =
            parse_price_ticks(bid, price_scale).context("failed to parse best bid price")?;
        let mut best_ask =
            parse_price_ticks(ask, price_scale).context("failed to parse best ask price")?;
        if best_ask <= best_bid {
            let original = best_ask;
            best_ask = best_bid + 1;
            warn!(
                best_bid,
                best_ask = original,
                adjusted_ask = best_ask,
                "Order book reported non-positive spread; bumping ask by one tick"
            );
        }

        Ok(Self { best_bid, best_ask })
    }

    fn mid(&self) -> i64 {
        self.best_bid + ((self.best_ask - self.best_bid) / 2)
    }

    fn spread(&self) -> i64 {
        self.best_ask - self.best_bid
    }
}

#[derive(Debug)]
struct ActiveOrderView {
    order_index: i64,
    side: OrderSide,
    price: i64,
    remaining: i64,
}

impl ActiveOrderView {
    fn key(&self) -> QuoteKey {
        QuoteKey {
            side: self.side,
            price_ticks: self.price,
        }
    }
}

impl ActiveOrderView {
    fn from_order(order: &models::Order, meta: &MarketMetadata) -> Result<Self> {
        let price = parse_price_ticks(&order.price, meta.price_scale())
            .context("invalid order price")?;
        let remaining = parse_scaled_amount(
            &order.remaining_base_amount,
            meta.size_scale(),
            "remaining base amount",
        )
        .context("invalid remaining amount")?;
        Ok(Self {
            order_index: order.order_index,
            side: if order.is_ask {
                OrderSide::Ask
            } else {
                OrderSide::Bid
            },
            price,
            remaining,
        })
    }
}

#[derive(Debug, Clone)]
struct InventoryState {
    position_qty: Decimal,
    position_value_abs: Decimal,
    collateral: Decimal,
    allocation_fraction: f64,
    target_pct: f64,
    diff_fraction: f64,
    skew_ticks: i64,
}

impl InventoryState {
    fn allocation_pct(&self) -> f64 {
        fraction_to_pct(self.allocation_fraction)
    }

    fn diff_pct(&self) -> f64 {
        self.diff_fraction * 50.0
    }
}

fn parse_price_ticks(value: &str, scale: i64) -> Result<i64> {
    parse_scaled_amount(value, scale, "price")
}

fn parse_scaled_amount(value: &str, scale: i64, label: &str) -> Result<i64> {
    ensure!(scale > 0, "{label} scale must be positive");
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("numeric string was empty");
    }

    let decimal =
        Decimal::from_str(trimmed).with_context(|| format!("unable to parse number {trimmed}"))?;
    let scale_decimal =
        Decimal::from_i64(scale).ok_or_else(|| anyhow!("{label} scale {scale} is invalid"))?;
    let scaled = decimal
        .checked_mul(scale_decimal)
        .ok_or_else(|| anyhow!("value {trimmed} is too large when scaled for {label}"))?;
    scaled
        .round()
        .to_i64()
        .ok_or_else(|| anyhow!("scaled {label} value for {trimmed} exceeds i64 range"))
}

fn parse_decimal_value(value: &str, label: &str) -> Result<Decimal> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("{label} string was empty");
    }
    Decimal::from_str(trimmed).with_context(|| format!("unable to parse {label} value {trimmed}"))
}

fn fraction_to_pct(fraction: f64) -> f64 {
    (((fraction + 1.0).clamp(0.0, 2.0)) / 2.0) * 100.0
}

const fn default_min_spread_ticks() -> i64 {
    4
}

const fn default_max_spread_ticks() -> i64 {
    25
}

const fn default_min_tick_move() -> i64 {
    5
}

const fn default_order_expiry_ms() -> u64 {
    60_000
}

const fn default_quote_staleness_ms() -> u64 {
    2_000
}

const fn default_inventory_target_base_pct() -> f64 {
    50.0
}

const fn default_inventory_risk_aversion() -> f64 {
    0.0
}

const fn default_inventory_disable_side_on_hard_limit() -> bool {
    true
}

const fn default_ewma_lambda() -> f64 {
    0.95
}

const fn default_volatility_spread_multiplier() -> f64 {
    1.5
}

const fn default_soft_inventory_threshold() -> f64 {
    0.3
}

const fn default_hard_inventory_threshold() -> f64 {
    0.7
}

const fn default_mass_cancel_threshold() -> i64 {
    5
}

const fn default_avellaneda_gamma() -> f64 {
    0.3
}

const fn default_avellaneda_kappa() -> f64 {
    1.5
}

const fn default_avellaneda_time_horizon_secs() -> f64 {
    60.0
}

const fn default_order_levels() -> usize {
    1
}

const fn default_order_level_spacing_ticks() -> i64 {
    5
}

const fn default_order_level_size_multiplier() -> f64 {
    0.0
}
