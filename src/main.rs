use std::{
    collections::{HashMap, HashSet},
    env,
    path::PathBuf,
    str::FromStr,
    time::{Duration as StdDuration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow};
use futures_util::StreamExt;
use lighter_client::{
    lighter_client::{LighterClient, OrderSide},
    models,
    types::{AccountId, ApiKeyIndex, BaseQty, Expiry, MarketId, Price},
    ws_client::{OrderBookEvent, WsEvent},
};
use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, One, ToPrimitive};
use tokio::time::{self, Instant};
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    dotenvy::dotenv().ok();

    let config = Config::from_env()?;
    info!("loaded configuration: {:?}", config);

    let mut strategy = Strategy::new(config).await?;
    strategy.run().await
}

fn init_tracing() {
    let default = "lighter_mm=info";
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(default))
        .unwrap_or_else(|_| EnvFilter::new("info"));

    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_names(true)
        .try_init();
}

#[derive(Debug, Clone)]
struct Config {
    api_url: String,
    private_key: String,
    account_index: AccountId,
    api_key_index: ApiKeyIndex,
    signer_library: Option<PathBuf>,
    market_id: MarketId,
    base_order_size: Decimal,
    base_spread_bps: Decimal,
    level_spacing_bps: Decimal,
    level_size_multiplier: Decimal,
    levels: usize,
    price_tolerance_bps: Decimal,
    qty_tolerance_ticks: i64,
    order_expiry_secs: i64,
    stale_order_after: StdDuration,
    account_snapshot_interval: StdDuration,
    active_order_sync_interval: StdDuration,
    min_quote_interval: StdDuration,
    post_only: bool,
}

impl Config {
    fn from_env() -> Result<Self> {
        let api_url = env::var("LIGHTER_API_URL")
            .unwrap_or_else(|_| "https://mainnet.zklighter.elliot.ai".to_string());
        let signer_library = env::var("LIGHTER_SIGNER_PATH").ok().map(PathBuf::from);
        let private_key = required_env("LIGHTER_PRIVATE_KEY")?;
        let account_index: i64 = required_env("LIGHTER_ACCOUNT_INDEX")?.parse()?;
        let api_key_index: i32 = required_env("LIGHTER_API_KEY_INDEX")?.parse()?;

        let market_id = env::var("LIGHTER_MM_MARKET_ID")
            .or_else(|_| env::var("LIGHTER_MARKET_ID"))
            .ok()
            .and_then(|value| value.parse::<i32>().ok())
            .map(MarketId::new)
            .unwrap_or_else(|| MarketId::new(1));

        let base_order_size = decimal_env("LIGHTER_MM_BASE_SIZE", Decimal::new(1, 3))?;
        let base_spread_bps = decimal_env("LIGHTER_MM_SPREAD_BPS", Decimal::from(25))?;
        let level_spacing_bps = decimal_env("LIGHTER_MM_LEVEL_SPREAD_BPS", Decimal::from(15))?;
        let level_size_multiplier =
            decimal_env("LIGHTER_MM_LEVEL_SIZE_MULTIPLIER", Decimal::one())?;
        let levels = env::var("LIGHTER_MM_LEVELS")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(2);
        let price_tolerance_bps = decimal_env("LIGHTER_MM_TOLERANCE_BPS", Decimal::from(2))?;
        let qty_tolerance_ticks = env::var("LIGHTER_MM_QTY_TOLERANCE_TICKS")
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0);
        let order_expiry_secs = env::var("LIGHTER_MM_ORDER_EXPIRY_SECS")
            .ok()
            .and_then(|value| value.parse::<i64>().ok())
            .map(|value| value.max(300))
            .unwrap_or(360);
        let stale_order_after = env::var("LIGHTER_MM_STALE_CANCEL_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .map(StdDuration::from_secs)
            .unwrap_or_else(|| StdDuration::from_secs(45));
        let account_snapshot_interval = env::var("LIGHTER_MM_ACCOUNT_LOG_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .map(StdDuration::from_secs)
            .unwrap_or_else(|| StdDuration::from_secs(60));
        let active_order_sync_interval = env::var("LIGHTER_MM_ACTIVE_SYNC_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .map(StdDuration::from_secs)
            .unwrap_or_else(|| StdDuration::from_secs(10));
        let min_quote_interval = env::var("LIGHTER_MM_MIN_QUOTE_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .map(StdDuration::from_millis)
            .unwrap_or_else(|| StdDuration::from_millis(250));
        let post_only = env::var("LIGHTER_MM_POST_ONLY")
            .ok()
            .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
            .unwrap_or(true);

        Ok(Self {
            api_url,
            private_key,
            account_index: AccountId::new(account_index),
            api_key_index: ApiKeyIndex::new(api_key_index),
            signer_library,
            market_id,
            base_order_size,
            base_spread_bps,
            level_spacing_bps,
            level_size_multiplier,
            levels,
            price_tolerance_bps,
            qty_tolerance_ticks,
            order_expiry_secs,
            stale_order_after,
            account_snapshot_interval,
            active_order_sync_interval,
            min_quote_interval,
            post_only,
        })
    }
}

#[derive(Debug, Clone)]
struct MarketMetadata {
    symbol: String,
    price_decimals: u32,
    size_decimals: u32,
    min_base: Decimal,
    min_base_ticks: i64,
    min_quote: Decimal,
}

impl MarketMetadata {
    fn from_detail(detail: &models::OrderBookDetail) -> Result<Self> {
        let price_decimals = detail.price_decimals as u32;
        let size_decimals = detail.size_decimals as u32;
        let min_base = Decimal::from_str(&detail.min_base_amount).unwrap_or_else(|_| Decimal::ZERO);
        let min_quote =
            Decimal::from_str(&detail.min_quote_amount).unwrap_or_else(|_| Decimal::ZERO);
        let min_base_ticks = decimal_to_scaled_i64(min_base, size_decimals)?;

        Ok(Self {
            symbol: detail.symbol.clone(),
            price_decimals,
            size_decimals,
            min_base,
            min_base_ticks,
            min_quote,
        })
    }

    fn price_tick(&self) -> Decimal {
        Decimal::new(1, self.price_decimals)
    }
}

#[derive(Debug, Clone)]
struct OrderTarget {
    side: OrderSide,
    level: usize,
    price: Decimal,
    price_ticks: i64,
    quantity: Decimal,
    qty_ticks: i64,
}

#[derive(Debug, Clone)]
struct ActiveOrder {
    order_index: i64,
    client_order_index: i64,
    side: OrderSide,
    price: Decimal,
    price_ticks: i64,
    original_qty: Decimal,
    remaining_qty: Decimal,
    remaining_ticks: i64,
    status: String,
    tif: String,
    created_at: SystemTime,
    expires_at: SystemTime,
}

impl ActiveOrder {
    fn age(&self) -> StdDuration {
        SystemTime::now()
            .duration_since(self.created_at)
            .unwrap_or_else(|_| StdDuration::from_secs(0))
    }

    fn time_to_expiry(&self) -> Option<StdDuration> {
        self.expires_at.duration_since(SystemTime::now()).ok()
    }

    fn is_usable(
        &self,
        target: &OrderTarget,
        tolerance_ticks: i64,
        qty_tolerance: i64,
        stale_after: StdDuration,
    ) -> bool {
        if self.side != target.side {
            return false;
        }

        if self.age() >= stale_after {
            return false;
        }

        if (self.price_ticks - target.price_ticks).abs() > tolerance_ticks {
            return false;
        }

        if (self.remaining_ticks - target.qty_ticks).abs() > qty_tolerance {
            return false;
        }

        true
    }
}

#[derive(Debug, Clone)]
struct PendingOrder {
    client_order_index: i64,
    side: OrderSide,
    price_ticks: i64,
    qty_ticks: i64,
    level: usize,
    submitted_at: Instant,
}

struct Strategy {
    config: Config,
    metadata: MarketMetadata,
    client: LighterClient,
    state: StrategyState,
}

struct StrategyState {
    active_orders: HashMap<i64, ActiveOrder>,
    by_client: HashMap<i64, i64>,
    pending_orders: HashMap<i64, PendingOrder>,
    matched_orders: HashSet<i64>,
    last_mid_price: Option<Decimal>,
    last_quote_action: Instant,
    next_client_order_id: i64,
}

impl StrategyState {
    fn new(min_quote_interval: StdDuration) -> Self {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| StdDuration::from_secs(0))
            .as_micros();
        // Ensure the client order ID is within the valid range (0 to 2^48 - 1)
        const MAX_CLIENT_ORDER_ID: i64 = 281474976710655;
        let mut initial = (seed % MAX_CLIENT_ORDER_ID as u128) as i64;
        if initial == 0 {
            initial = 1;
        }

        Self {
            active_orders: HashMap::new(),
            by_client: HashMap::new(),
            pending_orders: HashMap::new(),
            matched_orders: HashSet::new(),
            last_mid_price: None,
            last_quote_action: Instant::now() - min_quote_interval,
            next_client_order_id: initial,
        }
    }

    fn next_client_order_id(&mut self) -> i64 {
        // Ensure the client order ID is within the valid range (0 to 2^48 - 1)
        const MAX_CLIENT_ORDER_ID: i64 = 281474976710655; // 2^48 - 1
        self.next_client_order_id = self.next_client_order_id.wrapping_add(1);
        if self.next_client_order_id == 0 || self.next_client_order_id > MAX_CLIENT_ORDER_ID {
            self.next_client_order_id = 1;
        }
        self.next_client_order_id
    }

    fn prune_stale_pending(&mut self, now: Instant, max_age: StdDuration) {
        self.pending_orders.retain(|_, pending| {
            match now.checked_duration_since(pending.submitted_at) {
                Some(age) => age <= max_age,
                None => true,
            }
        });
    }

    fn has_recent_pending(
        &self,
        target: &OrderTarget,
        price_tolerance: i64,
        qty_tolerance: i64,
        now: Instant,
        max_age: StdDuration,
    ) -> bool {
        self.pending_orders.values().any(|pending| {
            if pending.side != target.side || pending.level != target.level {
                return false;
            }

            let is_fresh = now
                .checked_duration_since(pending.submitted_at)
                .map(|age| age <= max_age)
                .unwrap_or(true);

            if !is_fresh {
                return false;
            }

            (pending.price_ticks - target.price_ticks).abs() <= price_tolerance
                && (pending.qty_ticks - target.qty_ticks).abs() <= qty_tolerance
        })
    }
}

impl Strategy {
    async fn new(config: Config) -> Result<Self> {
        let mut builder = LighterClient::builder()
            .api_url(config.api_url.clone())
            .private_key(config.private_key.clone())
            .account_index(config.account_index)
            .api_key_index(config.api_key_index);

        if let Some(path) = &config.signer_library {
            builder = builder.signer_library_path(path);
        }

        let client = builder.build().await?;

        let metadata = Self::load_market_metadata(&client, config.market_id).await?;
        info!(
            "loaded market metadata for {} (id {}): min_base={} ({} ticks) min_quote={}",
            metadata.symbol,
            config.market_id,
            metadata.min_base,
            metadata.min_base_ticks,
            metadata.min_quote
        );

        let mut strategy = Self {
            metadata,
            state: StrategyState::new(config.min_quote_interval),
            config,
            client,
        };

        strategy.sync_active_orders().await?;
        strategy.log_order_snapshot();

        Ok(strategy)
    }

    async fn run(&mut self) -> Result<()> {
        let mut ws = self
            .client
            .ws()
            .subscribe_order_book(self.config.market_id)
            .connect()
            .await?;

        let mut account_log_interval = time::interval(self.config.account_snapshot_interval);
        let mut active_sync_interval = time::interval(self.config.active_order_sync_interval);

        info!("strategy event loop started");

        loop {
            tokio::select! {
                maybe_event = ws.next() => {
                    match maybe_event {
                        Some(Ok(WsEvent::OrderBook(event))) => {
                            self.handle_order_book(event).await?;
                        }
                        Some(Ok(WsEvent::Connected)) => {
                            info!("websocket connected to market {}", self.config.market_id);
                        }
                        Some(Ok(WsEvent::Closed(frame))) => {
                            warn!(?frame, "websocket stream closed");
                            break;
                        }
                        Some(Ok(other)) => {
                            debug!(?other, "ignored websocket event");
                        }
                        Some(Err(err)) => {
                            error!(?err, "websocket stream error");
                            return Err(err.into());
                        }
                        None => {
                            warn!("websocket stream ended");
                            break;
                        }
                    }
                }
                _ = account_log_interval.tick() => {
                    self.log_account_snapshot().await?;
                }
                _ = active_sync_interval.tick() => {
                    self.sync_active_orders().await?;
                }
            }
        }

        Ok(())
    }

    async fn load_market_metadata(
        client: &LighterClient,
        market: MarketId,
    ) -> Result<MarketMetadata> {
        let details = client.orders().book_details(Some(market)).await?;
        let detail = details
            .order_book_details
            .iter()
            .find(|detail| detail.market_id == i32::from(market))
            .or_else(|| details.order_book_details.first())
            .context("market metadata not found")?;
        MarketMetadata::from_detail(detail)
    }

    async fn handle_order_book(&mut self, event: OrderBookEvent) -> Result<()> {
        let best_bid = event
            .state
            .bids
            .iter()
            .find_map(|level| Decimal::from_str(&level.price).ok());
        let best_ask = event
            .state
            .asks
            .iter()
            .find_map(|level| Decimal::from_str(&level.price).ok());

        let (bid, ask) = match (best_bid, best_ask) {
            (Some(bid), Some(ask)) if bid > Decimal::ZERO && ask > Decimal::ZERO => (bid, ask),
            _ => return Ok(()),
        };

        let mid_price = (bid + ask) / Decimal::from(2);
        self.state.last_mid_price = Some(mid_price);
        debug!(%bid, %ask, %mid_price, "order book tick");
        self.requote(mid_price).await
    }

    async fn requote(&mut self, mid_price: Decimal) -> Result<()> {
        let now = Instant::now();
        if now.duration_since(self.state.last_quote_action) < self.config.min_quote_interval {
            return Ok(());
        }

        let targets = self.build_targets(mid_price)?;
        if targets.is_empty() {
            return Ok(());
        }

        let tolerance_ticks = self.price_tolerance_ticks(mid_price)?;
        self.state.matched_orders.clear();
        let pending_grace = self.pending_cooldown();
        self.state.prune_stale_pending(now, pending_grace);

        let mut to_place = Vec::new();
        for target in &targets {
            let matched = self
                .state
                .active_orders
                .iter()
                .find(|(order_index, order)| {
                    !self.state.matched_orders.contains(order_index)
                        && order.is_usable(
                            target,
                            tolerance_ticks,
                            self.config.qty_tolerance_ticks,
                            self.config.stale_order_after,
                        )
                })
                .map(|(order_index, _)| *order_index);

            if let Some(order_index) = matched {
                self.state.matched_orders.insert(order_index);
            } else {
                let has_pending = self.state.has_recent_pending(
                    target,
                    tolerance_ticks,
                    self.config.qty_tolerance_ticks,
                    now,
                    pending_grace,
                );

                if !has_pending {
                    to_place.push(target.clone());
                }
            }
        }

        let mut to_cancel = Vec::new();
        for order_index in self.state.active_orders.keys() {
            if !self.state.matched_orders.contains(order_index) {
                to_cancel.push(*order_index);
            }
        }

        let mut mutated = false;
        if !to_cancel.is_empty() {
            self.cancel_orders(&to_cancel).await?;
            mutated = true;
        }

        if !to_place.is_empty() {
            self.place_orders(&to_place).await?;
            mutated = true;
        }

        if mutated {
            self.sync_active_orders().await?;
        }

        self.state.last_quote_action = now;
        Ok(())
    }

    async fn cancel_orders(&mut self, orders: &[i64]) -> Result<()> {
        for order_index in orders {
            if let Some(order) = self.state.active_orders.get(order_index) {
                info!(
                    order_index = *order_index,
                    client_order_index = order.client_order_index,
                    side = ?order.side,
                    price = %order.price,
                    remaining = %order.remaining_qty,
                    "canceling resting order"
                );
            }

            match self
                .client
                .cancel(self.config.market_id, *order_index)
                .submit()
                .await
            {
                Ok(submission) => {
                    info!(
                        order_index = *order_index,
                        tx = submission.response().tx_hash,
                        "cancelled order"
                    );
                    if let Some(order) = self.state.active_orders.remove(order_index) {
                        self.state.by_client.remove(&order.client_order_index);
                    }
                }
                Err(err) => {
                    warn!(order_index = *order_index, ?err, "failed to cancel order");
                }
            }
        }

        Ok(())
    }

    async fn place_orders(&mut self, targets: &[OrderTarget]) -> Result<()> {
        for target in targets {
            if target.qty_ticks <= 0 {
                continue;
            }

            if target.qty_ticks < self.metadata.min_base_ticks {
                warn!(
                    side = ?target.side,
                    level = target.level,
                    qty_ticks = target.qty_ticks,
                    min_ticks = self.metadata.min_base_ticks,
                    "skipping order below min base size"
                );
                continue;
            }

            let qty = BaseQty::try_from(target.qty_ticks)
                .map_err(|_| anyhow!("quantity must be positive"))?;
            let expiry = Expiry::from_now(::time::Duration::seconds(self.config.order_expiry_secs));
            let price = Price::ticks(target.price_ticks);
            let client_order_index = self.state.next_client_order_id();

            let builder = match target.side {
                OrderSide::Bid => self.client.order(self.config.market_id).buy(),
                OrderSide::Ask => self.client.order(self.config.market_id).sell(),
            };

            let mut builder = builder
                .with_client_order_id(client_order_index)
                .qty(qty)
                .limit(price)
                .expires_at(expiry);

            if self.config.post_only {
                builder = builder.post_only();
            }

            match builder.submit().await {
                Ok(submission) => {
                    info!(
                        side = ?target.side,
                        level = target.level,
                        client_order_index,
                        price = %target.price,
                        qty = %target.quantity,
                        qty_ticks = target.qty_ticks,
                        tx = submission.response().tx_hash,
                        "submitted maker order"
                    );
                    self.state.pending_orders.insert(
                        client_order_index,
                        PendingOrder {
                            client_order_index,
                            side: target.side,
                            price_ticks: target.price_ticks,
                            qty_ticks: target.qty_ticks,
                            level: target.level,
                            submitted_at: Instant::now(),
                        },
                    );
                }
                Err(err) => {
                    warn!(
                        side = ?target.side,
                        level = target.level,
                        price = %target.price,
                        qty = %target.quantity,
                        ?err,
                        "failed to submit order"
                    );
                }
            }
        }

        Ok(())
    }

    async fn sync_active_orders(&mut self) -> Result<()> {
        let response = self
            .client
            .account()
            .active_orders(self.config.market_id)
            .await?;

        let mut updated = HashMap::new();
        let mut by_client = HashMap::new();

        for order in response.orders {
            let active = Self::convert_order(&order, &self.metadata)?;
            if !self.state.active_orders.contains_key(&active.order_index) {
                info!(
                    order_index = active.order_index,
                    client_order_index = active.client_order_index,
                    side = ?active.side,
                    price = %active.price,
                    qty = %active.original_qty,
                    remaining = %active.remaining_qty,
                    "tracking new active order"
                );
            }
            by_client.insert(active.client_order_index, active.order_index);
            updated.insert(active.order_index, active);
        }

        for order_index in self
            .state
            .active_orders
            .keys()
            .filter(|idx| !updated.contains_key(idx))
            .copied()
            .collect::<Vec<_>>()
        {
            info!(
                order_index,
                "order left active set (likely filled or cancelled externally)"
            );
        }

        self.state.active_orders = updated;
        self.state.by_client = by_client;

        for client_id in self
            .state
            .pending_orders
            .keys()
            .copied()
            .collect::<Vec<_>>()
        {
            if self.state.by_client.contains_key(&client_id) {
                self.state.pending_orders.remove(&client_id);
            }
        }

        Ok(())
    }

    fn log_order_snapshot(&self) {
        let mut active: Vec<_> = self.state.active_orders.values().cloned().collect();
        active.sort_by(|a, b| {
            let side_rank = |side: OrderSide| match side {
                OrderSide::Bid => 0,
                OrderSide::Ask => 1,
            };
            let rank_a = (side_rank(a.side), a.price_ticks);
            let rank_b = (side_rank(b.side), b.price_ticks);
            rank_a.cmp(&rank_b)
        });

        if active.is_empty() {
            info!("no resting orders on book");
        } else {
            for order in &active {
                let age = order.age();
                let time_to_expiry = order
                    .time_to_expiry()
                    .unwrap_or_else(|| StdDuration::from_secs(0));
                info!(
                    order_index = order.order_index,
                    client_order_index = order.client_order_index,
                    side = ?order.side,
                    price = %order.price,
                    remaining = %order.remaining_qty,
                    status = %order.status,
                    tif = %order.tif,
                    age_secs = age.as_secs_f64(),
                    expires_in_secs = time_to_expiry.as_secs_f64(),
                    "active order snapshot"
                );
            }
        }

        if !self.state.pending_orders.is_empty() {
            for pending in self.state.pending_orders.values() {
                info!(
                    client_order_index = pending.client_order_index,
                    side = ?pending.side,
                    level = pending.level,
                    price_ticks = pending.price_ticks,
                    qty_ticks = pending.qty_ticks,
                    since_ms = pending.submitted_at.elapsed().as_millis() as u64,
                    "pending order awaiting confirmation"
                );
            }
        }
    }

    async fn log_account_snapshot(&self) -> Result<()> {
        let details = self.client.account().details().await?;
        for account in &details.accounts {
            info!(
                account_index = account.account_index,
                available_balance = %account.available_balance,
                collateral = %account.collateral,
                total_order_count = account.total_order_count,
                pending_order_count = account.pending_order_count,
                total_asset_value = %account.total_asset_value,
                "account snapshot"
            );

            for position in &account.positions {
                info!(
                    account_index = account.account_index,
                    market_id = position.market_id,
                    symbol = %position.symbol,
                    sign = position.sign,
                    position = %position.position,
                    avg_entry_price = %position.avg_entry_price,
                    position_value = %position.position_value,
                    unrealized_pnl = %position.unrealized_pnl,
                    realized_pnl = %position.realized_pnl,
                    liquidation_price = %position.liquidation_price,
                    allocated_margin = %position.allocated_margin,
                    "position snapshot"
                );
            }
        }

        self.log_order_snapshot();
        Ok(())
    }

    fn build_targets(&self, mid_price: Decimal) -> Result<Vec<OrderTarget>> {
        let mut targets = Vec::with_capacity(self.config.levels * 2);
        let bps_divisor = Decimal::from_i64(10_000).unwrap();

        for level in 0..self.config.levels {
            let level_decimal = Decimal::from_i64(level as i64).unwrap_or(Decimal::ZERO);
            let spread_bps =
                self.config.base_spread_bps + self.config.level_spacing_bps * level_decimal;
            let spread_fraction = spread_bps / bps_divisor;
            let mut size_multiplier = Decimal::ONE;
            if level > 0 {
                size_multiplier = (0..level).fold(Decimal::ONE, |acc, _| {
                    acc * self.config.level_size_multiplier
                });
            }

            let quantity = (self.config.base_order_size * size_multiplier)
                .round_dp(self.metadata.size_decimals);

            if quantity <= Decimal::ZERO {
                continue;
            }

            let bid_price = (mid_price * (Decimal::one() - spread_fraction))
                .round_dp(self.metadata.price_decimals);
            let ask_price = (mid_price * (Decimal::one() + spread_fraction))
                .round_dp(self.metadata.price_decimals);

            if bid_price > Decimal::ZERO {
                let price_ticks = decimal_to_scaled_i64(bid_price, self.metadata.price_decimals)?;
                let qty_ticks = decimal_to_scaled_i64(quantity, self.metadata.size_decimals)?;
                targets.push(OrderTarget {
                    side: OrderSide::Bid,
                    level,
                    price: bid_price,
                    price_ticks,
                    quantity,
                    qty_ticks,
                });
            }

            if ask_price > Decimal::ZERO {
                let price_ticks = decimal_to_scaled_i64(ask_price, self.metadata.price_decimals)?;
                let qty_ticks = decimal_to_scaled_i64(quantity, self.metadata.size_decimals)?;
                targets.push(OrderTarget {
                    side: OrderSide::Ask,
                    level,
                    price: ask_price,
                    price_ticks,
                    quantity,
                    qty_ticks,
                });
            }
        }

        Ok(targets)
    }

    fn price_tolerance_ticks(&self, mid_price: Decimal) -> Result<i64> {
        let fraction = self.config.price_tolerance_bps / Decimal::from_i64(10_000).unwrap();
        let tolerance_price = (mid_price * fraction).max(self.metadata.price_tick());
        let ticks = decimal_to_scaled_i64(tolerance_price, self.metadata.price_decimals)?;
        Ok(ticks.max(1))
    }

    fn pending_cooldown(&self) -> StdDuration {
        let extra = StdDuration::from_millis(500);
        self.config
            .min_quote_interval
            .checked_add(extra)
            .unwrap_or(self.config.min_quote_interval)
    }

    fn convert_order(order: &models::Order, metadata: &MarketMetadata) -> Result<ActiveOrder> {
        let price = Decimal::from_str(&order.price).unwrap_or_else(|_| Decimal::ZERO);
        let remaining =
            Decimal::from_str(&order.remaining_base_amount).unwrap_or_else(|_| Decimal::ZERO);
        let original =
            Decimal::from_str(&order.initial_base_amount).unwrap_or_else(|_| Decimal::ZERO);

        let price_ticks = decimal_to_scaled_i64(price, metadata.price_decimals)?;
        let remaining_ticks = decimal_to_scaled_i64(remaining, metadata.size_decimals)?;

        let created_at = epoch_to_system_time(order.timestamp);
        let expires_at = epoch_to_system_time(order.order_expiry);

        Ok(ActiveOrder {
            order_index: order.order_index,
            client_order_index: order.client_order_index,
            side: if order.is_ask {
                OrderSide::Ask
            } else {
                OrderSide::Bid
            },
            price,
            price_ticks,
            original_qty: original,
            remaining_qty: remaining,
            remaining_ticks,
            status: format!("{:?}", order.status),
            tif: format!("{:?}", order.time_in_force),
            created_at,
            expires_at,
        })
    }
}

fn required_env(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("set the {name} environment variable"))
}

fn decimal_env(name: &str, default: Decimal) -> Result<Decimal> {
    Ok(env::var(name)
        .ok()
        .and_then(|value| Decimal::from_str(&value).ok())
        .unwrap_or(default))
}

fn decimal_to_scaled_i64(value: Decimal, decimals: u32) -> Result<i64> {
    let factor = Decimal::from(10_i64.pow(decimals));
    let scaled = (value * factor).round();
    scaled
        .to_i64()
        .ok_or_else(|| anyhow!("value {value} exceeds i64 range when scaled"))
}

fn epoch_to_system_time(value: i64) -> SystemTime {
    if value > 1_000_000_000_000 {
        UNIX_EPOCH + StdDuration::from_millis(value.max(0) as u64)
    } else {
        UNIX_EPOCH + StdDuration::from_secs(value.max(0) as u64)
    }
}
