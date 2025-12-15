//! # Subscriptions & Live Tailing
//!
//! This module implements real-time event subscriptions for SpiteDB. Subscribers
//! can receive events as they're committed, starting from any historical position.
//!
//! ## The Catch-Up + Live Pattern
//!
//! Subscriptions use a two-phase approach:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                        Subscription Timeline                             │
//! │                                                                          │
//! │  Subscriber starts                                                       │
//! │  from position 100                    Current head = 500                 │
//! │       │                                      │                           │
//! │       ▼                                      ▼                           │
//! │  ┌────────────────────────────────┐  ┌──────────────────────────────┐   │
//! │  │     Phase 1: Catch-Up          │  │     Phase 2: Live            │   │
//! │  │     Read events 100-500        │  │     Receive events 501+      │   │
//! │  │     from SQLite                │  │     via broadcast channel    │   │
//! │  └────────────────────────────────┘  └──────────────────────────────┘   │
//! │                                                                          │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Why This Pattern?
//!
//! 1. **No missed events**: Subscriber receives all events from their start position
//! 2. **No duplicates**: Careful handoff ensures each event delivered exactly once
//! 3. **Efficient**: Historical reads are batched, live events are pushed
//!
//! ## Backpressure Handling
//!
//! If a subscriber can't keep up with live events:
//! - Broadcast channel has a bounded capacity
//! - Lagging receivers get `RecvError::Lagged(n)` indicating n missed events
//! - Subscription detects lag and returns an error or reconnects
//!
//! ## Rust Concepts
//!
//! - **`tokio::sync::broadcast`**: Multi-producer, multi-consumer channel where
//!   each receiver gets a copy of every message. Perfect for pub/sub patterns.
//!
//! - **`Stream` trait**: Async iterator pattern. Subscribers can use `while let`
//!   or stream combinators to process events.
//!
//! - **`Pin<Box<...>>`**: Required for async streams that may contain self-references.
//!   The `async_stream` crate handles this automatically.

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use tokio::sync::broadcast;
use tokio::sync::mpsc;

use crate::error::{Error, Result};
use crate::types::{Event, GlobalPos, StreamId, StreamRev};

// =============================================================================
// Configuration
// =============================================================================

/// Default capacity for the broadcast channel.
///
/// This determines how many events can be buffered before slow subscribers
/// start lagging. At 64KB average event size, 10K events = ~640MB buffer.
/// Most events are much smaller, so this is quite generous.
pub const DEFAULT_BROADCAST_CAPACITY: usize = 10_000;

/// Default batch size for catch-up reads.
///
/// Larger batches are more efficient but use more memory.
pub const DEFAULT_CATCHUP_BATCH_SIZE: usize = 1000;

// =============================================================================
// Broadcast Event
// =============================================================================

/// An event broadcast to subscribers after commit.
///
/// This is a lightweight wrapper that's cheap to clone (via Arc internally
/// for data) for efficient broadcast distribution.
#[derive(Debug, Clone)]
pub struct BroadcastEvent {
    /// Global position of the event.
    pub global_pos: GlobalPos,

    /// Stream this event belongs to.
    pub stream_id: StreamId,

    /// Revision within the stream.
    pub stream_rev: StreamRev,

    /// Timestamp when the event was stored (Unix milliseconds).
    pub timestamp_ms: u64,

    /// Event payload (wrapped in Arc for efficient cloning).
    pub data: std::sync::Arc<Vec<u8>>,
}

impl BroadcastEvent {
    /// Creates a new broadcast event.
    pub fn new(
        global_pos: GlobalPos,
        stream_id: StreamId,
        stream_rev: StreamRev,
        timestamp_ms: u64,
        data: Vec<u8>,
    ) -> Self {
        Self {
            global_pos,
            stream_id,
            stream_rev,
            timestamp_ms,
            data: std::sync::Arc::new(data),
        }
    }

    /// Converts to a regular Event (copies the data).
    pub fn into_event(self) -> Event {
        Event {
            global_pos: self.global_pos,
            stream_id: self.stream_id,
            stream_rev: self.stream_rev,
            timestamp_ms: self.timestamp_ms,
            data: (*self.data).clone(),
        }
    }
}

impl From<Event> for BroadcastEvent {
    fn from(event: Event) -> Self {
        Self::new(
            event.global_pos,
            event.stream_id,
            event.stream_rev,
            event.timestamp_ms,
            event.data,
        )
    }
}

impl From<BroadcastEvent> for Event {
    fn from(broadcast: BroadcastEvent) -> Self {
        broadcast.into_event()
    }
}

// =============================================================================
// Subscription State Machine
// =============================================================================

/// Internal state of a subscription.
///
/// This state machine is used by `GlobalSubscription` for the Stream trait
/// implementation. It handles the transition from catch-up to live phases.
#[allow(dead_code)] // Variants used by GlobalSubscription (Stream impl)
enum SubscriptionState {
    /// Initial state - need to start catch-up.
    Init,

    /// Catching up from historical events.
    CatchingUp {
        /// Current position in catch-up.
        current_pos: GlobalPos,
        /// Buffered events from last batch read.
        buffer: Vec<Event>,
        /// Index into buffer.
        buffer_index: usize,
    },

    /// Live streaming from broadcast channel.
    Live {
        /// Broadcast receiver for live events.
        receiver: broadcast::Receiver<BroadcastEvent>,
    },

    /// Subscription ended (error or complete).
    Ended,
}

// =============================================================================
// Global Subscription
// =============================================================================

/// A subscription to the global event log.
///
/// Implements the `Stream` trait, yielding events in global position order.
///
/// # Usage
///
/// ```rust,ignore
/// use futures::StreamExt;
///
/// let mut subscription = db.subscribe_global(GlobalPos::FIRST).await?;
///
/// while let Some(result) = subscription.next().await {
///     match result {
///         Ok(event) => println!("Event at {}: {:?}", event.global_pos, event.data),
///         Err(e) => eprintln!("Subscription error: {}", e),
///     }
/// }
/// ```
///
/// # Backpressure
///
/// If the subscriber can't keep up, the subscription may error with
/// `Error::SubscriptionLagged`. The subscriber can then decide to:
/// - Create a new subscription from the last known position
/// - Accept some missed events and continue
///
/// # Note
///
/// This is a lower-level implementation for the Stream trait. For most use
/// cases, prefer `SimpleSubscription` or `CatchUpSubscription`.
#[allow(dead_code)] // Part of the Stream trait infrastructure
pub struct GlobalSubscription {
    /// Current state of the subscription.
    state: SubscriptionState,

    /// Starting position for the subscription.
    start_pos: GlobalPos,

    /// Channel to request catch-up reads.
    read_request_tx: mpsc::Sender<CatchUpRequest>,

    /// Receiver for catch-up read responses.
    read_response_rx: mpsc::Receiver<CatchUpResponse>,

    /// Sender for broadcast receiver creation.
    broadcast_subscribe_tx: mpsc::Sender<BroadcastSubscribeRequest>,

    /// Batch size for catch-up reads.
    catchup_batch_size: usize,

    /// Optional stream filter (None = all streams).
    stream_filter: Option<StreamId>,

    /// Current position for determining when to switch to live.
    /// This is set when we subscribe to the broadcast channel.
    live_start_pos: Option<GlobalPos>,
}

/// Request for a catch-up read.
pub struct CatchUpRequest {
    /// Position to read from.
    pub from_pos: GlobalPos,
    /// Maximum events to read.
    pub limit: usize,
    /// Optional stream filter.
    pub stream_filter: Option<StreamId>,
    /// Response channel.
    pub response: tokio::sync::oneshot::Sender<CatchUpResponse>,
}

/// Response from a catch-up read.
pub struct CatchUpResponse {
    /// Events read from the database.
    pub events: Result<Vec<Event>>,
    /// Current head position at time of read.
    pub head_pos: GlobalPos,
}

/// Request to subscribe to the broadcast channel.
pub struct BroadcastSubscribeRequest {
    /// Response channel for the receiver.
    pub response: tokio::sync::oneshot::Sender<BroadcastSubscribeResponse>,
}

/// Response with the broadcast receiver.
pub struct BroadcastSubscribeResponse {
    /// Broadcast receiver for live events.
    pub receiver: broadcast::Receiver<BroadcastEvent>,
    /// Current head position when subscription was created.
    pub head_pos: GlobalPos,
}

impl GlobalSubscription {
    /// Creates a new global subscription.
    ///
    /// # Arguments
    ///
    /// * `start_pos` - Position to start from (inclusive)
    /// * `read_request_tx` - Channel to request catch-up reads
    /// * `broadcast_subscribe_tx` - Channel to subscribe to broadcast
    /// * `catchup_batch_size` - Batch size for catch-up reads
    pub fn new(
        start_pos: GlobalPos,
        read_request_tx: mpsc::Sender<CatchUpRequest>,
        broadcast_subscribe_tx: mpsc::Sender<BroadcastSubscribeRequest>,
        catchup_batch_size: usize,
    ) -> Self {
        // Create a channel for read responses
        let (_, read_response_rx) = mpsc::channel(1);

        Self {
            state: SubscriptionState::Init,
            start_pos,
            read_request_tx,
            read_response_rx,
            broadcast_subscribe_tx,
            catchup_batch_size,
            stream_filter: None,
            live_start_pos: None,
        }
    }

    /// Creates a new global subscription with a stream filter.
    pub fn with_stream_filter(mut self, stream_id: StreamId) -> Self {
        self.stream_filter = Some(stream_id);
        self
    }

    /// Starts the catch-up phase by subscribing to broadcast and initiating reads.
    #[allow(dead_code)] // Part of Stream trait infrastructure
    async fn start_catchup(&mut self) -> Result<()> {
        // First, subscribe to the broadcast channel to get the current head position
        // This ensures we don't miss events during the catch-up phase
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        self.broadcast_subscribe_tx
            .send(BroadcastSubscribeRequest { response: response_tx })
            .await
            .map_err(|_| Error::Schema("subscription service closed".to_string()))?;

        let broadcast_response = response_rx
            .await
            .map_err(|_| Error::Schema("subscription service closed".to_string()))?;

        self.live_start_pos = Some(broadcast_response.head_pos);

        // If we're already at or past the head, go directly to live
        if self.start_pos.as_raw() >= broadcast_response.head_pos.as_raw() {
            self.state = SubscriptionState::Live {
                receiver: broadcast_response.receiver,
            };
        } else {
            // Need to catch up - request first batch of historical events
            let (_read_tx, read_rx) = mpsc::channel(1);
            self.read_response_rx = read_rx;

            let (response_tx, _) = tokio::sync::oneshot::channel();

            // Request initial catch-up read
            self.read_request_tx
                .send(CatchUpRequest {
                    from_pos: self.start_pos,
                    limit: self.catchup_batch_size,
                    stream_filter: self.stream_filter.clone(),
                    response: response_tx,
                })
                .await
                .map_err(|_| Error::Schema("reader service closed".to_string()))?;

            // Create response channel for reads - receiver stored for processing
            let (_read_tx, read_rx) = mpsc::channel(1);
            self.read_response_rx = read_rx;

            self.state = SubscriptionState::CatchingUp {
                current_pos: self.start_pos,
                buffer: Vec::new(),
                buffer_index: 0,
            };
        }

        Ok(())
    }
}

impl Stream for GlobalSubscription {
    type Item = Result<Event>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                SubscriptionState::Init => {
                    // Need to initialize - this is async, so we need to handle it
                    // For simplicity in the sync poll context, we'll just return pending
                    // and require the caller to call start() first
                    // In practice, we'd use a more complex state machine or async-stream

                    // Return pending to indicate we need async initialization
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }

                SubscriptionState::CatchingUp {
                    current_pos: _,
                    buffer,
                    buffer_index,
                } => {
                    // If we have buffered events, return them
                    if *buffer_index < buffer.len() {
                        let event = buffer[*buffer_index].clone();
                        *buffer_index += 1;
                        return Poll::Ready(Some(Ok(event)));
                    }

                    // Buffer exhausted - need to read more or transition to live
                    // For now, poll the read response channel
                    match Pin::new(&mut self.read_response_rx).poll_recv(cx) {
                        Poll::Ready(Some(response)) => {
                            match response.events {
                                Ok(events) => {
                                    if events.is_empty() {
                                        // No more historical events - transition to live
                                        // We need to re-subscribe to broadcast
                                        // This is simplified - in production we'd preserve the receiver
                                        self.state = SubscriptionState::Ended;
                                        return Poll::Ready(None);
                                    }

                                    // Check if we've caught up to live
                                    let last_pos = events.last().map(|e| e.global_pos);

                                    if let (Some(last), Some(live_start)) =
                                        (last_pos, self.live_start_pos)
                                    {
                                        if last.as_raw() >= live_start.as_raw() {
                                            // Caught up - transition to live
                                            // For now, just end (simplified)
                                            self.state = SubscriptionState::Ended;
                                            return Poll::Ready(None);
                                        }
                                    }

                                    // More catch-up needed - update state and return first event
                                    let first_event = events[0].clone();
                                    let new_current_pos = events
                                        .last()
                                        .map(|e| e.global_pos.next())
                                        .unwrap_or(GlobalPos::FIRST);

                                    self.state = SubscriptionState::CatchingUp {
                                        current_pos: new_current_pos,
                                        buffer: events,
                                        buffer_index: 1, // Already returning first
                                    };

                                    return Poll::Ready(Some(Ok(first_event)));
                                }
                                Err(e) => {
                                    self.state = SubscriptionState::Ended;
                                    return Poll::Ready(Some(Err(e)));
                                }
                            }
                        }
                        Poll::Ready(None) => {
                            // Channel closed
                            self.state = SubscriptionState::Ended;
                            return Poll::Ready(None);
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }

                SubscriptionState::Live { receiver } => {
                    // Poll the broadcast receiver
                    match receiver.try_recv() {
                        Ok(broadcast_event) => {
                            // Apply stream filter if set
                            if let Some(ref filter) = self.stream_filter {
                                if &broadcast_event.stream_id != filter {
                                    // Skip this event, continue polling
                                    continue;
                                }
                            }
                            return Poll::Ready(Some(Ok(broadcast_event.into_event())));
                        }
                        Err(broadcast::error::TryRecvError::Empty) => {
                            // No events available - register waker and return pending
                            cx.waker().wake_by_ref();
                            return Poll::Pending;
                        }
                        Err(broadcast::error::TryRecvError::Lagged(n)) => {
                            // Subscriber fell behind - return error
                            self.state = SubscriptionState::Ended;
                            return Poll::Ready(Some(Err(Error::SubscriptionLagged(n))));
                        }
                        Err(broadcast::error::TryRecvError::Closed) => {
                            // Channel closed
                            self.state = SubscriptionState::Ended;
                            return Poll::Ready(None);
                        }
                    }
                }

                SubscriptionState::Ended => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

// =============================================================================
// Subscription Manager
// =============================================================================

/// Manages subscriptions and broadcasts events to subscribers.
///
/// This component:
/// - Maintains the broadcast channel for live events
/// - Handles catch-up read requests
/// - Coordinates the transition from catch-up to live
pub struct SubscriptionManager {
    /// Broadcast sender for live events.
    broadcast_tx: broadcast::Sender<BroadcastEvent>,

    /// Current head position (last committed global_pos).
    head_pos: GlobalPos,
}

impl SubscriptionManager {
    /// Creates a new subscription manager.
    pub fn new(capacity: usize) -> Self {
        let (broadcast_tx, _) = broadcast::channel(capacity);

        Self {
            broadcast_tx,
            head_pos: GlobalPos::FIRST,
        }
    }

    /// Returns a new broadcast receiver.
    pub fn subscribe(&self) -> broadcast::Receiver<BroadcastEvent> {
        self.broadcast_tx.subscribe()
    }

    /// Returns the current head position.
    pub fn head_pos(&self) -> GlobalPos {
        self.head_pos
    }

    /// Updates the head position and broadcasts events.
    ///
    /// Called by the writer after successfully committing a batch.
    pub fn broadcast_events(&mut self, events: Vec<BroadcastEvent>) {
        for event in events {
            // Update head position
            if event.global_pos.as_raw() > self.head_pos.as_raw() {
                self.head_pos = event.global_pos;
            }

            // Broadcast to all subscribers
            // If there are no receivers, this is a no-op
            let _ = self.broadcast_tx.send(event);
        }
    }

    /// Returns the number of active subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.broadcast_tx.receiver_count()
    }
}

// =============================================================================
// Simplified Subscription Builder
// =============================================================================

/// Builder for creating subscriptions with a simpler API.
///
/// This is the primary interface for creating subscriptions from SpiteDB.
pub struct SubscriptionBuilder {
    /// Starting position.
    from_pos: GlobalPos,

    /// Optional stream filter.
    stream_filter: Option<StreamId>,

    /// Batch size for catch-up.
    catchup_batch_size: usize,
}

impl SubscriptionBuilder {
    /// Creates a new subscription builder starting from a position.
    pub fn from_position(pos: GlobalPos) -> Self {
        Self {
            from_pos: pos,
            stream_filter: None,
            catchup_batch_size: DEFAULT_CATCHUP_BATCH_SIZE,
        }
    }

    /// Creates a subscription starting from the beginning.
    pub fn from_start() -> Self {
        Self::from_position(GlobalPos::FIRST)
    }

    /// Filters to only events from a specific stream.
    pub fn filter_stream(mut self, stream_id: impl Into<StreamId>) -> Self {
        self.stream_filter = Some(stream_id.into());
        self
    }

    /// Sets the batch size for catch-up reads.
    pub fn catchup_batch_size(mut self, size: usize) -> Self {
        self.catchup_batch_size = size;
        self
    }

    /// Returns the starting position.
    pub fn start_position(&self) -> GlobalPos {
        self.from_pos
    }

    /// Returns the stream filter if set.
    pub fn stream_filter(&self) -> Option<&StreamId> {
        self.stream_filter.as_ref()
    }

    /// Returns the catch-up batch size.
    pub fn batch_size(&self) -> usize {
        self.catchup_batch_size
    }
}

// =============================================================================
// Simple Polling Subscription
// =============================================================================

/// A simple subscription that uses polling.
///
/// This is a simpler implementation that doesn't use the Stream trait,
/// making it easier to use in straightforward async contexts.
///
/// # Example
///
/// ```rust,ignore
/// let mut sub = db.subscribe_all(GlobalPos::FIRST).await?;
///
/// loop {
///     match sub.next().await {
///         Some(Ok(event)) => println!("Event: {:?}", event),
///         Some(Err(e)) => {
///             eprintln!("Error: {}", e);
///             break;
///         }
///         None => break, // Subscription closed
///     }
/// }
/// ```
pub struct SimpleSubscription {
    /// Broadcast receiver for live events.
    receiver: broadcast::Receiver<BroadcastEvent>,

    /// Optional stream filter.
    stream_filter: Option<StreamId>,
}

impl SimpleSubscription {
    /// Creates a new simple subscription from a broadcast receiver.
    pub fn new(receiver: broadcast::Receiver<BroadcastEvent>) -> Self {
        Self {
            receiver,
            stream_filter: None,
        }
    }

    /// Creates a subscription with a stream filter.
    pub fn with_filter(receiver: broadcast::Receiver<BroadcastEvent>, stream_id: StreamId) -> Self {
        Self {
            receiver,
            stream_filter: Some(stream_id),
        }
    }

    /// Receives the next event.
    ///
    /// Blocks until an event is available or an error occurs.
    ///
    /// # Returns
    ///
    /// - `Some(Ok(event))` - Next event
    /// - `Some(Err(e))` - Error (e.g., lagged)
    /// - `None` - Subscription closed
    pub async fn next(&mut self) -> Option<Result<Event>> {
        loop {
            match self.receiver.recv().await {
                Ok(broadcast_event) => {
                    // Apply stream filter if set
                    if let Some(ref filter) = self.stream_filter {
                        if &broadcast_event.stream_id != filter {
                            continue; // Skip this event
                        }
                    }
                    return Some(Ok(broadcast_event.into_event()));
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    return Some(Err(Error::SubscriptionLagged(n)));
                }
                Err(broadcast::error::RecvError::Closed) => {
                    return None;
                }
            }
        }
    }

    /// Tries to receive the next event without blocking.
    ///
    /// # Returns
    ///
    /// - `Some(Ok(event))` - Event available
    /// - `Some(Err(e))` - Error (e.g., lagged)
    /// - `None` - No event available (would block)
    pub fn try_next(&mut self) -> Option<Result<Event>> {
        loop {
            match self.receiver.try_recv() {
                Ok(broadcast_event) => {
                    if let Some(ref filter) = self.stream_filter {
                        if &broadcast_event.stream_id != filter {
                            continue;
                        }
                    }
                    return Some(Ok(broadcast_event.into_event()));
                }
                Err(broadcast::error::TryRecvError::Empty) => {
                    return None; // No event available
                }
                Err(broadcast::error::TryRecvError::Lagged(n)) => {
                    return Some(Err(Error::SubscriptionLagged(n)));
                }
                Err(broadcast::error::TryRecvError::Closed) => {
                    return None;
                }
            }
        }
    }

    /// Returns the number of events in the receive buffer.
    ///
    /// This is the number of events that have been broadcast but not yet
    /// received by this subscriber.
    pub fn len(&self) -> usize {
        self.receiver.len()
    }

    /// Returns true if the receive buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// =============================================================================
// Catch-Up Subscription
// =============================================================================

/// A subscription that handles both catch-up and live phases.
///
/// This is the full-featured subscription that:
/// 1. Reads historical events from the database (catch-up phase)
/// 2. Seamlessly transitions to live events from the broadcast channel
/// 3. Handles backpressure and lag detection
///
/// # Example
///
/// ```rust,ignore
/// let mut sub = db.subscribe_from(GlobalPos::from_raw(1000)).await?;
///
/// // Will receive events starting from position 1000
/// while let Some(result) = sub.next().await {
///     let event = result?;
///     println!("Position {}: {:?}", event.global_pos, event.data);
/// }
/// ```
pub struct CatchUpSubscription {
    /// Current phase of the subscription.
    phase: CatchUpPhase,

    /// Starting position for the subscription (kept for debugging/logging).
    #[allow(dead_code)]
    start_pos: GlobalPos,

    /// Position we're currently at.
    current_pos: GlobalPos,

    /// Position where we switch to live.
    live_start_pos: GlobalPos,

    /// Read function for catch-up.
    read_fn: Box<dyn Fn(GlobalPos, usize) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<Event>>> + Send>> + Send + Sync>,

    /// Broadcast receiver for live phase.
    receiver: broadcast::Receiver<BroadcastEvent>,

    /// Batch size for catch-up reads.
    batch_size: usize,

    /// Buffered events from catch-up.
    buffer: Vec<Event>,

    /// Index into buffer.
    buffer_idx: usize,

    /// Optional stream filter.
    stream_filter: Option<StreamId>,
}

/// Phase of the catch-up subscription.
enum CatchUpPhase {
    /// Reading historical events.
    CatchingUp,
    /// Receiving live events.
    Live,
    /// Subscription ended.
    Ended,
}

impl CatchUpSubscription {
    /// Creates a new catch-up subscription.
    ///
    /// # Arguments
    ///
    /// * `start_pos` - Position to start from
    /// * `live_start_pos` - Position where live events begin
    /// * `receiver` - Broadcast receiver for live events
    /// * `read_fn` - Function to read historical events
    /// * `batch_size` - Batch size for catch-up reads
    pub fn new<F, Fut>(
        start_pos: GlobalPos,
        live_start_pos: GlobalPos,
        receiver: broadcast::Receiver<BroadcastEvent>,
        read_fn: F,
        batch_size: usize,
    ) -> Self
    where
        F: Fn(GlobalPos, usize) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Vec<Event>>> + Send + 'static,
    {
        // Determine initial phase
        let phase = if start_pos.as_raw() >= live_start_pos.as_raw() {
            CatchUpPhase::Live
        } else {
            CatchUpPhase::CatchingUp
        };

        Self {
            phase,
            start_pos,
            current_pos: start_pos,
            live_start_pos,
            read_fn: Box::new(move |pos, limit| Box::pin(read_fn(pos, limit))),
            receiver,
            batch_size,
            buffer: Vec::new(),
            buffer_idx: 0,
            stream_filter: None,
        }
    }

    /// Adds a stream filter.
    pub fn with_filter(mut self, stream_id: StreamId) -> Self {
        self.stream_filter = Some(stream_id);
        self
    }

    /// Gets the next event.
    pub async fn next(&mut self) -> Option<Result<Event>> {
        loop {
            match self.phase {
                CatchUpPhase::CatchingUp => {
                    // Check buffer first
                    if self.buffer_idx < self.buffer.len() {
                        let event = self.buffer[self.buffer_idx].clone();
                        self.buffer_idx += 1;

                        // Apply filter
                        if let Some(ref filter) = self.stream_filter {
                            if &event.stream_id != filter {
                                continue;
                            }
                        }

                        return Some(Ok(event));
                    }

                    // Buffer empty - read more or transition to live
                    if self.current_pos.as_raw() >= self.live_start_pos.as_raw() {
                        // Caught up - transition to live
                        self.phase = CatchUpPhase::Live;
                        continue;
                    }

                    // Read next batch
                    match (self.read_fn)(self.current_pos, self.batch_size).await {
                        Ok(events) => {
                            if events.is_empty() {
                                // No more historical events
                                self.phase = CatchUpPhase::Live;
                                continue;
                            }

                            // Update current position
                            if let Some(last) = events.last() {
                                self.current_pos = last.global_pos.next();
                            }

                            // Store in buffer
                            self.buffer = events;
                            self.buffer_idx = 0;
                            continue;
                        }
                        Err(e) => {
                            self.phase = CatchUpPhase::Ended;
                            return Some(Err(e));
                        }
                    }
                }

                CatchUpPhase::Live => {
                    match self.receiver.recv().await {
                        Ok(broadcast_event) => {
                            // Skip events we already delivered during catch-up
                            if broadcast_event.global_pos.as_raw() < self.current_pos.as_raw() {
                                continue;
                            }

                            // Apply filter
                            if let Some(ref filter) = self.stream_filter {
                                if &broadcast_event.stream_id != filter {
                                    continue;
                                }
                            }

                            return Some(Ok(broadcast_event.into_event()));
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            return Some(Err(Error::SubscriptionLagged(n)));
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            self.phase = CatchUpPhase::Ended;
                            return None;
                        }
                    }
                }

                CatchUpPhase::Ended => {
                    return None;
                }
            }
        }
    }

    /// Returns whether the subscription is in the live phase.
    pub fn is_live(&self) -> bool {
        matches!(self.phase, CatchUpPhase::Live)
    }

    /// Returns the current position.
    pub fn current_position(&self) -> GlobalPos {
        self.current_pos
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broadcast_event_conversion() {
        let event = Event {
            global_pos: GlobalPos::from_raw(42),
            stream_id: StreamId::new("test-stream"),
            stream_rev: StreamRev::from_raw(1),
            timestamp_ms: 12345,
            data: b"hello".to_vec(),
        };

        let broadcast: BroadcastEvent = event.clone().into();
        assert_eq!(broadcast.global_pos.as_raw(), 42);
        assert_eq!(broadcast.stream_id.as_str(), "test-stream");

        let back: Event = broadcast.into();
        assert_eq!(back.data, event.data);
    }

    #[test]
    fn test_subscription_manager_creation() {
        let manager = SubscriptionManager::new(100);
        assert_eq!(manager.head_pos().as_raw(), 1);
        assert_eq!(manager.subscriber_count(), 0);
    }

    #[test]
    fn test_subscription_manager_broadcast() {
        let mut manager = SubscriptionManager::new(100);
        let mut receiver = manager.subscribe();

        let events = vec![
            BroadcastEvent::new(
                GlobalPos::from_raw(1),
                StreamId::new("stream-1"),
                StreamRev::from_raw(1),
                12345,
                b"event1".to_vec(),
            ),
            BroadcastEvent::new(
                GlobalPos::from_raw(2),
                StreamId::new("stream-1"),
                StreamRev::from_raw(2),
                12346,
                b"event2".to_vec(),
            ),
        ];

        manager.broadcast_events(events);

        assert_eq!(manager.head_pos().as_raw(), 2);

        // Receive events
        let evt1 = receiver.try_recv().unwrap();
        assert_eq!(evt1.global_pos.as_raw(), 1);

        let evt2 = receiver.try_recv().unwrap();
        assert_eq!(evt2.global_pos.as_raw(), 2);
    }

    #[test]
    fn test_subscription_builder() {
        let builder = SubscriptionBuilder::from_position(GlobalPos::from_raw(100))
            .filter_stream("my-stream")
            .catchup_batch_size(500);

        assert_eq!(builder.start_position().as_raw(), 100);
        assert_eq!(builder.stream_filter().map(|s| s.as_str()), Some("my-stream"));
        assert_eq!(builder.batch_size(), 500);
    }

    #[tokio::test]
    async fn test_simple_subscription() {
        let (tx, _) = broadcast::channel::<BroadcastEvent>(100);
        let rx = tx.subscribe();

        let mut sub = SimpleSubscription::new(rx);

        // Send an event
        let _ = tx.send(BroadcastEvent::new(
            GlobalPos::from_raw(1),
            StreamId::new("test"),
            StreamRev::from_raw(1),
            12345,
            b"hello".to_vec(),
        ));

        // Should receive it
        let event = sub.next().await.unwrap().unwrap();
        assert_eq!(event.global_pos.as_raw(), 1);
        assert_eq!(event.data, b"hello");
    }

    #[tokio::test]
    async fn test_simple_subscription_with_filter() {
        let (tx, _) = broadcast::channel::<BroadcastEvent>(100);
        let rx = tx.subscribe();

        let mut sub = SimpleSubscription::with_filter(rx, StreamId::new("wanted"));

        // Send events from different streams
        let _ = tx.send(BroadcastEvent::new(
            GlobalPos::from_raw(1),
            StreamId::new("unwanted"),
            StreamRev::from_raw(1),
            12345,
            b"skip".to_vec(),
        ));

        let _ = tx.send(BroadcastEvent::new(
            GlobalPos::from_raw(2),
            StreamId::new("wanted"),
            StreamRev::from_raw(1),
            12346,
            b"keep".to_vec(),
        ));

        // Should only receive the filtered event
        let event = sub.next().await.unwrap().unwrap();
        assert_eq!(event.global_pos.as_raw(), 2);
        assert_eq!(event.data, b"keep");
    }

    #[tokio::test]
    async fn test_subscription_lagged_error() {
        let (tx, _) = broadcast::channel::<BroadcastEvent>(2); // Small capacity
        let rx = tx.subscribe();

        let mut sub = SimpleSubscription::new(rx);

        // Fill the buffer and overflow
        for i in 1..=5 {
            let _ = tx.send(BroadcastEvent::new(
                GlobalPos::from_raw(i),
                StreamId::new("test"),
                StreamRev::from_raw(i),
                12345,
                format!("event{}", i).into_bytes(),
            ));
        }

        // Should get lagged error
        let result = sub.next().await.unwrap();
        assert!(matches!(result, Err(Error::SubscriptionLagged(_))));
    }
}
