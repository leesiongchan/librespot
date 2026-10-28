use crate::{
    LoadContextOptions, LoadRequestOptions, PlayContext,
    context_resolver::{ContextAction, ContextResolver, ResolveContext},
    core::{
        Error, Session, SpotifyUri,
        authentication::Credentials,
        dealer::{
            manager::{BoxedStream, BoxedStreamResult, Reply, RequestReply},
            protocol::{Command, FallbackWrapper, Message, Request},
        },
        session::UserAttributes,
        spclient::TransferRequest,
    },
    model::{LoadRequest, PlayingTrack, SpircPlayStatus},
    playback::{
        mixer::Mixer,
        player::{Player, PlayerEvent, PlayerEventChannel, RemoteState, StartupOutcome},
    },
    protocol::{
        connect::{Cluster, ClusterUpdate, LogoutCommand, SetVolumeCommand},
        context::Context,
        explicit_content_pubsub::UserAttributesUpdate,
        player::ProvidedTrack,
        playlist4_external::PlaylistModificationInfo,
        social_connect_v2::SessionUpdate,
        transfer_state::TransferState,
        user_attributes::UserAttributesMutation,
    },
    startup::{
        StartupDecision, remote_changed, remote_playback, startup_decision, transfer_position,
    },
    state::{
        context::{ContextType, ResetContext},
        provider::IsProvider,
        {ConnectConfig, ConnectState},
    },
};
use futures_util::StreamExt;
use librespot_protocol::context_page::ContextPage;
use protobuf::MessageField;
use std::{
    future::Future,
    sync::Arc,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tokio::{
    sync::mpsc,
    time::{Instant, sleep, sleep_until},
};

// Leave time for native failure delivery before Swift's 20-second readiness deadline.
const RESTORATION_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, Error)]
enum SpircError {
    #[error("response payload empty")]
    NoData,
    #[error("{0} had no uri")]
    NoUri(&'static str),
    #[error("message pushed for another URI")]
    InvalidUri(String),
    #[error("failed to put connect state for new device")]
    FailedDealerSetup,
    #[error("unknown endpoint: {0:#?}")]
    UnknownEndpoint(serde_json::Value),
}

impl From<SpircError> for Error {
    fn from(err: SpircError) -> Self {
        use SpircError::*;
        match err {
            NoData | NoUri(_) => Error::unavailable(err),
            InvalidUri(_) | FailedDealerSetup => Error::aborted(err),
            UnknownEndpoint(_) => Error::unimplemented(err),
        }
    }
}

struct SpircTask {
    queue_tracker: crate::queue::QueueTracker,
    observed_repeat: Option<(bool, bool)>,
    player: Arc<Player>,
    mixer: Arc<dyn Mixer>,

    /// the state management object
    connect_state: ConnectState,
    connect_established: bool,

    play_request_id: Option<u64>,
    play_status: SpircPlayStatus,

    connection_id_update: BoxedStreamResult<String>,
    connect_state_update: BoxedStreamResult<ClusterUpdate>,
    connect_state_volume_update: BoxedStreamResult<SetVolumeCommand>,
    connect_state_logout_request: BoxedStreamResult<LogoutCommand>,
    playlist_update: BoxedStreamResult<PlaylistModificationInfo>,
    session_update: BoxedStreamResult<FallbackWrapper<SessionUpdate>>,
    connect_state_command: BoxedStream<RequestReply>,
    user_attributes_update: BoxedStreamResult<UserAttributesUpdate>,
    user_attributes_mutation: BoxedStreamResult<UserAttributesMutation>,

    commands: Option<mpsc::UnboundedReceiver<SpircCommand>>,
    player_events: Option<PlayerEventChannel>,

    context_resolver: ContextResolver,

    emit_set_queue_events: bool,
    startup_restore: bool,
    startup_mode: crate::StartupMode,
    latest_owner: String,
    last_cluster_timestamp: Option<i64>,
    recovery_native: Option<crate::protocol::player::PlayerState>,
    bootstrap_pending: bool,
    bootstrap_queue_ready: bool,
    bootstrap_playback_confirmed: bool,
    recovery_resume_requested: bool,
    restoration_deadline: Option<Instant>,
    restoration_aborted: bool,
    #[cfg(test)]
    stall_state_publication: bool,
    #[cfg(test)]
    stall_new_device_publication: bool,
    #[cfg(test)]
    stall_volume_publication: bool,
    #[cfg(test)]
    stall_context_load: bool,
    last_identity: Option<crate::RecoveryIdentity>,
    /// The startup decision runs on the first connection id only; later ones are dealer reconnects.
    startup_resolved: bool,

    /// The mirror last emitted, so cluster updates emit only changes.
    last_remote: Option<RemoteState>,

    /// The time of play-here's transfer request, so its reply plays even from a paused
    /// device, and a repeat press inside the window is not sent again.
    play_here_requested_at: Option<i64>,

    shutdown: bool,
    session: Session,

    /// is set when transferring, and used after resolving the contexts to finish the transfer
    pub transfer_state: Option<TransferState>,

    /// when set to true, it will update the volume after [VOLUME_UPDATE_DELAY],
    /// when no other future resolves, otherwise resets the delay
    update_volume: bool,

    /// when set to true, it will update the volume after [UPDATE_STATE_DELAY],
    /// when no other future resolves, otherwise resets the delay
    update_state: bool,

    spirc_id: usize,
}

static SPIRC_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
enum SpircCommand {
    Play,
    PlayHere,
    PlayPause,
    Pause,
    Prev,
    Next,
    SelectQueue(
        QueueSelection,
        tokio::sync::oneshot::Sender<Result<(), QueueSelectionError>>,
    ),
    VolumeUp,
    VolumeDown,
    Shutdown,
    Shuffle(bool),
    Repeat(bool),
    RepeatTrack(bool),
    Disconnect {
        pause: bool,
    },
    SetPosition(u32),
    SetVolume(u16),
    Activate,
    Transfer(Option<TransferRequest>),
    Load(LoadRequest),
    AddToQueue(SpotifyUri),
    ClearQueue,
}

/// An occurrence in the last published queue, including hidden raw entries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueSelection {
    /// Structural snapshot version.
    pub generation: u64,
    /// Position before delimiter filtering.
    pub raw_next_index: u32,
}

/// A rejected selection leaves playback unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueueSelectionError {
    /// The queue changed after it was displayed.
    Stale,
    /// The entry no longer exists.
    OutOfRange,
    /// The device or entry cannot play.
    Unavailable,
}

pub(crate) fn validate_selection(
    generation: u64,
    selection: QueueSelection,
    len: usize,
) -> Result<usize, QueueSelectionError> {
    if generation != selection.generation {
        return Err(QueueSelectionError::Stale);
    }
    let index = selection.raw_next_index as usize;
    if index >= len {
        return Err(QueueSelectionError::OutOfRange);
    }
    Ok(index)
}

pub(crate) fn dispatch_selection(
    state: &mut ConnectState,
    generation: u64,
    selection: QueueSelection,
    load: impl FnOnce(SpotifyUri),
) -> Result<(), QueueSelectionError> {
    // Parse before promotion as well as before invoking the audio boundary.
    let index = validate_selection(generation, selection, state.player().next_tracks.len())?;
    let uri = SpotifyUri::from_uri(&state.player().next_tracks[index].uri)
        .map_err(|_| QueueSelectionError::Unavailable)?;
    state.select_queue_entry(generation, selection)?;
    load(uri);
    Ok(())
}

const CONTEXT_FETCH_THRESHOLD: usize = 2;

// delay to update volume after a certain amount of time, instead on each update request
const VOLUME_UPDATE_DELAY: Duration = Duration::from_millis(500);
// to reduce updates to remote, we group some request by waiting for a set amount of time
const UPDATE_STATE_DELAY: Duration = Duration::from_millis(200);
/// How long a play-here request stays pending, so a second Play press inside the
/// window does not send another transfer.
const PLAY_HERE_WINDOW_MS: i64 = 10_000;

/// The spotify connect handle
pub struct Spirc {
    commands: mpsc::UnboundedSender<SpircCommand>,
}

impl Spirc {
    /// Initializes a new spotify connect device
    ///
    /// The returned tuple consists out of a handle to the [`Spirc`] that
    /// can control the local connect device when active. And a [`Future`]
    /// which represents the [`Spirc`] event loop that processes the whole
    /// connect device logic.
    pub async fn new(
        config: ConnectConfig,
        session: Session,
        credentials: Credentials,
        player: Arc<Player>,
        mixer: Arc<dyn Mixer>,
    ) -> Result<(Spirc, impl Future<Output = ()>), Error> {
        fn extract_connection_id(msg: Message) -> Result<String, Error> {
            let connection_id = msg
                .headers
                .get("Spotify-Connection-Id")
                .ok_or_else(|| SpircError::InvalidUri(msg.uri.clone()))?;
            Ok(connection_id.to_owned())
        }

        let spirc_id = SPIRC_COUNTER.fetch_add(1, Ordering::AcqRel);
        debug!("new Spirc[{spirc_id}]");

        let emit_set_queue_events = config.emit_set_queue_events;
        let startup_restore = config.startup_restore;
        let startup_mode = config.startup_mode.clone();
        let connect_state = ConnectState::new(config, &session);

        let connection_id_update = session
            .dealer()
            .listen_for("hm://pusher/v1/connections/", extract_connection_id)?;

        let connect_state_update = session
            .dealer()
            .listen_for("hm://connect-state/v1/cluster", Message::from_raw)?;

        let connect_state_volume_update = session
            .dealer()
            .listen_for("hm://connect-state/v1/connect/volume", Message::from_raw)?;

        let connect_state_logout_request = session
            .dealer()
            .listen_for("hm://connect-state/v1/connect/logout", Message::from_raw)?;

        let playlist_update = session
            .dealer()
            .listen_for("hm://playlist/v2/playlist/", Message::from_raw)?;

        let session_update = session
            .dealer()
            .listen_for("social-connect/v2/session_update", Message::try_from_json)?;

        let user_attributes_update = session
            .dealer()
            .listen_for("spotify:user:attributes:update", Message::from_raw)?;

        // can be trigger by toggling autoplay in a desktop client
        let user_attributes_mutation = session
            .dealer()
            .listen_for("spotify:user:attributes:mutated", Message::from_raw)?;

        let connect_state_command = session
            .dealer()
            .handle_for("hm://connect-state/v1/player/command")?;

        // pre-acquire client_token, preventing multiple request while running
        let _ = session.spclient().client_token().await?;

        // Connect *after* all message listeners are registered
        session.connect(credentials, true).await?;

        // pre-acquire access_token (we need to be authenticated to retrieve a token)
        let _ = session.login5().auth_token().await?;

        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

        let player_events = player.get_player_event_channel();

        let mut task = SpircTask {
            queue_tracker: Default::default(),
            observed_repeat: None,
            player,
            mixer,

            connect_state,
            connect_established: false,

            play_request_id: None,
            play_status: SpircPlayStatus::Stopped,

            connection_id_update,
            connect_state_update,
            connect_state_volume_update,
            connect_state_logout_request,
            playlist_update,
            session_update,
            connect_state_command,
            user_attributes_update,
            user_attributes_mutation,
            commands: Some(cmd_rx),
            player_events: Some(player_events),

            context_resolver: ContextResolver::new(session.clone()),

            emit_set_queue_events,
            startup_restore,
            startup_mode,
            latest_owner: String::new(),
            last_cluster_timestamp: None,
            recovery_native: None,
            bootstrap_pending: false,
            bootstrap_queue_ready: false,
            bootstrap_playback_confirmed: false,
            recovery_resume_requested: false,
            restoration_deadline: None,
            restoration_aborted: false,
            #[cfg(test)]
            stall_state_publication: false,
            #[cfg(test)]
            stall_new_device_publication: false,
            #[cfg(test)]
            stall_volume_publication: false,
            #[cfg(test)]
            stall_context_load: false,
            last_identity: None,
            startup_resolved: false,
            last_remote: None,
            play_here_requested_at: None,

            shutdown: false,
            session,

            transfer_state: None,
            update_volume: false,
            update_state: false,

            spirc_id,
        };

        let spirc = Spirc { commands: cmd_tx };

        let initial_volume = task.connect_state.device_info().volume;
        task.connect_state.set_volume(0);

        match initial_volume.try_into() {
            Ok(volume) => {
                task.set_volume(volume);
                // we don't want to update the volume initially,
                // we just want to set the mixer to the correct volume
                task.update_volume = false;
            }
            Err(why) => error!("failed to update initial volume: {why}"),
        };

        Ok((spirc, task.run()))
    }

    /// Safely shutdowns the spirc.
    ///
    /// This pauses the playback, disconnects the connect device and
    /// bring the future initially returned to an end.
    pub fn shutdown(&self) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::Shutdown)?)
    }

    /// Resumes the playback
    ///
    /// Does nothing if we are not the active device, or it isn't paused.
    pub fn play(&self) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::Play)?)
    }

    /// Resumes or pauses the playback
    ///
    /// Does nothing if we are not the active device.
    pub fn play_pause(&self) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::PlayPause)?)
    }

    /// Pauses the playback
    ///
    /// Does nothing if we are not the active device, or if it isn't playing.
    pub fn pause(&self) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::Pause)?)
    }

    /// Seeks to the beginning or skips to the previous track.
    ///
    /// Seeks to the beginning when the current track position
    /// is greater than 3 seconds.
    ///
    /// Does nothing if we are not the active device.
    pub fn prev(&self) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::Prev)?)
    }

    /// Skips to the next track.
    ///
    /// Does nothing if we are not the active device.
    pub fn next(&self) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::Next)?)
    }

    /// Select an exact occurrence without reloading its context or intermediate audio.
    pub fn select_queue_entry(
        &self,
        selection: QueueSelection,
    ) -> Result<tokio::sync::oneshot::Receiver<Result<(), QueueSelectionError>>, Error> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.commands
            .send(SpircCommand::SelectQueue(selection, sender))?;
        Ok(receiver)
    }

    /// Increases the volume by configured steps of [ConnectConfig].
    ///
    /// Does nothing if we are not the active device.
    pub fn volume_up(&self) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::VolumeUp)?)
    }

    /// Decreases the volume by configured steps of [ConnectConfig].
    ///
    /// Does nothing if we are not the active device.
    pub fn volume_down(&self) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::VolumeDown)?)
    }

    /// Shuffles the playback according to the value.
    ///
    /// If true shuffles/reshuffles the playback. Otherwise, does
    /// nothing (if not shuffled) or unshuffles the playback while
    /// resuming at the position of the current track.
    ///
    /// Does nothing if we are not the active device.
    pub fn shuffle(&self, shuffle: bool) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::Shuffle(shuffle))?)
    }

    /// Repeats the playback context according to the value.
    ///
    /// Does nothing if we are not the active device.
    pub fn repeat(&self, repeat: bool) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::Repeat(repeat))?)
    }

    /// Repeats the current track if true.
    ///
    /// Does nothing if we are not the active device.
    ///
    /// Skipping to the next track disables the repeating.
    pub fn repeat_track(&self, repeat: bool) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::RepeatTrack(repeat))?)
    }

    /// Update the volume to the given value.
    ///
    /// Does nothing if we are not the active device.
    pub fn set_volume(&self, volume: u16) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::SetVolume(volume))?)
    }

    /// Updates the position to the given value.
    ///
    /// Does nothing if we are not the active device.
    ///
    /// If value is greater than the track duration,
    /// the update is ignored.
    pub fn set_position_ms(&self, position_ms: u32) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::SetPosition(position_ms))?)
    }

    /// Load a new context and replace the current.
    ///
    /// Does nothing if we are not the active device.
    ///
    /// Does not overwrite the queue.
    pub fn load(&self, command: LoadRequest) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::Load(command))?)
    }

    /// Adds a track, episode, album or playlist to the queue.
    ///
    /// Does nothing if we are not the active device.
    ///
    /// For albums and playlists, all tracks/episodes are resolved and added to the queue.
    pub fn add_to_queue(&self, uri: SpotifyUri) -> Result<(), Error> {
        if !matches!(
            uri,
            SpotifyUri::Track { .. }
                | SpotifyUri::Episode { .. }
                | SpotifyUri::Album { .. }
                | SpotifyUri::Playlist { .. }
        ) {
            return Err(Error::invalid_argument("uri"));
        }
        Ok(self.commands.send(SpircCommand::AddToQueue(uri))?)
    }

    /// Removes all queued tracks from the next tracks.
    ///
    /// Does nothing if we are not the active device.
    ///
    /// Only tracks added to the queue (via [Spirc::add_to_queue] or a connect client)
    /// are removed. The current track, even if it was queued, and the tracks of the
    /// context are kept; the next tracks are filled up from the context again.
    pub fn clear_queue(&self) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::ClearQueue)?)
    }

    /// Disconnects the current device and pauses the playback according the value.
    ///
    /// Does nothing if we are not the active device.
    pub fn disconnect(&self, pause: bool) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::Disconnect { pause })?)
    }

    /// Acquires the control as active connect device.
    ///
    /// Does not [Spirc::transfer] the playback. Does nothing if we are not the active device.
    pub fn activate(&self) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::Activate)?)
    }

    /// Acquires the control as active connect device over the transfer flow.
    ///
    /// Does nothing if we are not the active device.
    pub fn transfer(&self, transfer_request: Option<TransferRequest>) -> Result<(), Error> {
        Ok(self
            .commands
            .send(SpircCommand::Transfer(transfer_request))?)
    }

    /// Bopify: plays here from wherever the account's session is, taking over
    /// another device, resuming the last session, or playing Liked Songs.
    ///
    /// Needs `ConnectConfig::startup_restore`; while this device is active it is [Spirc::play].
    pub fn play_here(&self) -> Result<(), Error> {
        Ok(self.commands.send(SpircCommand::PlayHere)?)
    }
}

impl SpircTask {
    async fn run(mut self) {
        if let Err(why) = self.session.dealer().start().await {
            error!("starting dealer failed: {why}");
            return;
        }

        self.run_event_loop().await;

        if !self.shutdown && self.connect_state.is_active() {
            warn!("unexpected shutdown");
            if let Err(why) = self.handle_disconnect(true, true).await {
                error!("error during disconnecting: {why}")
            }
        }

        // this should clear the active session id, leaving an empty state
        if let Err(why) = self.session.spclient().delete_connect_state_request().await {
            error!("error during connect state deletion: {why}")
        };

        self.session.dealer().close().await;
    }

    async fn run_event_loop(&mut self) {
        // simplify unwrapping of received item or parsed result
        macro_rules! unwrap {
            ( $next:expr, |$some:ident| $use_some:expr ) => {
                match $next {
                    Some($some) => $use_some,
                    None => {
                        warn!(target: "bopify_core::session", "connect: stream_closed spirc={} stream={}", self.spirc_id, stringify!($next));
                        error!("{} selected, but none received", stringify!($next));
                        break;
                    }
                }
            };
            ( $next:expr, match |$ok:ident| $use_ok:expr ) => {
                unwrap!($next, |$ok| match $ok {
                    Ok($ok) => $use_ok,
                    Err(why) => error!("could not parse {}: {}", stringify!($ok), why),
                })
            };
        }

        let session = self.session.clone();
        while !self.session.is_invalid() && !self.shutdown {
            if self.expire_restoration() {
                if let Err(why) = self.notify().await {
                    error!("restoration cleanup failed: {why}");
                }
            }
            let restoration_pending =
                self.restoration_deadline.is_some() && self.restoration_pending();
            let commands = self.commands.as_mut();
            let player_events = self.player_events.as_mut();

            // when state and volume update have a higher priority than context resolving
            // because of that the context resolving has to wait, so that the other tasks can finish
            let allow_context_resolving = !self.update_state && !self.update_volume;

            tokio::select! {
                _ = session.invalidated() => break,
                _ = async {
                    let deadline = self.restoration_deadline?;
                    sleep_until(deadline).await;
                    Some(())
                }, if restoration_pending => {
                    self.expire_restoration();
                    if let Err(why) = self.notify().await { error!("restoration cleanup failed: {why}"); }
                },
                // startup of the dealer requires a connection_id, which is retrieved at the very beginning
                connection_id_update = self.connection_id_update.next() => unwrap! {
                    connection_id_update,
                    match |connection_id| if let Err(why) = self.handle_connection_id_update(connection_id).await {
                        warn!(target: "bopify_core::session", "connect: connection_id_failed spirc={} error_kind={:?}", self.spirc_id, why.kind);
                        error!("failed handling connection id update: {why}");
                        break;
                    }
                },
                // main dealer update of any remote device updates
                cluster_update = self.connect_state_update.next() => unwrap! {
                    cluster_update,
                    match |cluster_update| if let Err(e) = self.handle_cluster_update(cluster_update).await {
                        error!("could not dispatch connect state update: {e}");
                    }
                },
                // main dealer request handling (dealer expects an answer)
                request = self.connect_state_command.next() => unwrap! {
                    request,
                    |request| if let Err(e) = self.handle_connect_state_request(request).await {
                        error!("couldn't handle connect state command: {e}");
                    }
                },
                // volume request handling is send separately (it's more like a fire forget)
                volume_update = self.connect_state_volume_update.next() => unwrap! {
                    volume_update,
                    match |volume_update| match volume_update.volume.try_into() {
                        Ok(volume) => self.set_volume(volume),
                        Err(why) => error!("can't update volume, failed to parse i32 to u16: {why}")
                    }
                },
                logout_request = self.connect_state_logout_request.next() => unwrap! {
                    logout_request,
                    |logout_request| {
                        error!("received logout request, currently not supported: {logout_request:#?}");
                        // todo: call logout handling
                    }
                },
                playlist_update = self.playlist_update.next() => unwrap! {
                    playlist_update,
                    match |playlist_update| if let Err(why) = self.handle_playlist_modification(playlist_update) {
                        error!("failed to handle playlist modification: {why}")
                    }
                },
                user_attributes_update = self.user_attributes_update.next() => unwrap! {
                    user_attributes_update,
                    match |attributes| self.handle_user_attributes_update(attributes)
                },
                user_attributes_mutation = self.user_attributes_mutation.next() => unwrap! {
                    user_attributes_mutation,
                    match |attributes| self.handle_user_attributes_mutation(attributes)
                },
                session_update = self.session_update.next() => unwrap! {
                    session_update,
                    match |session_update| self.handle_session_update(session_update)
                },
                cmd = async { commands?.recv().await }, if commands.is_some() && self.connect_established => if let Some(cmd) = cmd {
                    if let Err(e) = self.handle_command(cmd).await {
                        debug!("could not dispatch command: {e}");
                    }
                },
                event = async { player_events?.recv().await }, if player_events.is_some() => if let Some(event) = event {
                    if let Err(e) = self.handle_player_event(event) {
                        error!("could not dispatch player event: {e}");
                    }
                },
                _ = async { sleep(UPDATE_STATE_DELAY).await }, if self.update_state => {
                    self.update_state = false;

                    if let Err(why) = self.notify().await {
                        error!("state update: {why}")
                    }
                },
                _ = async { sleep(VOLUME_UPDATE_DELAY).await }, if self.update_volume => {
                    self.update_volume = false;

                    info!("delayed volume update for all devices: volume is now {}", self.connect_state.device_info().volume);
                    let deadline = self.restoration_request_deadline();
                    #[cfg(test)]
                    let stall_publication = std::mem::take(&mut self.stall_volume_publication);
                    let publication = async {
                        #[cfg(test)]
                        if stall_publication { std::future::pending::<()>().await; }
                        self.connect_state.notify_volume_changed(&self.session).await
                    };
                    if let Err(why) = Self::await_restoration_request(deadline, publication).await {
                        error!("error updating connect state for volume update: {why}")
                    }

                    // for some reason the web-player does need two separate updates, so that the
                    // position of the current track is retained, other clients also send a state
                    // update before they send the volume update
                    if let Err(why) = self.notify().await {
                        error!("error updating connect state for volume update: {why}")
                    }
                },
                // context resolver handling, the idea/reason behind it the following:
                //
                // when we request a context that has multiple pages (for example an artist)
                // resolving all pages at once can take around ~1-30sec, when we resolve
                // everything at once that would block our main loop for that time
                //
                // to circumvent this behavior, we request each context separately here and
                // finish after we received our last item of a type
                next_context = async {
                    self.context_resolver.get_next_context(|| {
                        // Sending local file URIs to this endpoint results in a Bad Request status.
                        // It's likely appropriate to filter them out anyway; Spotify's backend
                        // has no knowledge about these tracks and so can't do anything with them.
                        self.connect_state.recent_track_uris()
                            .into_iter()
                            .filter(|t| !t.starts_with("spotify:local"))
                            .collect::<Vec<_>>()
                    }).await
                }, if allow_context_resolving && self.context_resolver.has_next() => {
                    let update_state = self.handle_next_context(next_context);
                    if update_state {
                        if let Err(why) = self.notify().await {
                            error!("update after context resolving failed: {why}")
                        }
                    }
                },
                else => break
            }
            self.emit_set_queue_event();
        }
        debug!(target: "bopify_core::session", "connect: loop_exit spirc={} session_invalid={} shutdown_requested={}", self.spirc_id, self.session.is_invalid(), self.shutdown);
    }

    fn handle_next_context(&mut self, next_context: Result<Context, Error>) -> bool {
        if self.restoration_aborted {
            return false;
        }
        let next_context = match next_context {
            Err(why) => return self.handle_context_failure(why),
            Ok(ctx) => ctx,
        };

        debug!("handling next context {:?}", next_context.uri);
        match self
            .context_resolver
            .apply_next_context(&mut self.connect_state, next_context)
        {
            Ok(Some(remaining)) => self.context_resolver.add_list(remaining),
            Ok(None) => {}
            Err(why) => return self.handle_context_failure(why),
        }

        let update_state = match self
            .context_resolver
            .try_finish(&mut self.connect_state, &mut self.transfer_state)
        {
            Ok(completed) => completed,
            Err(why) => return self.handle_context_failure(why),
        };
        if update_state {
            self.add_autoplay_resolving_when_required();
        }

        if let Some(native) = self.recovery_native.as_ref() {
            if self.foreign_owner() {
                return false;
            }
            if self
                .connect_state
                .get_context(self.connect_state.active_context)
                .is_ok()
            {
                let mut staged = self.connect_state.clone();
                if let Err(why) = staged.restore_recovery_player_state(native.clone()) {
                    return self.handle_context_failure(why);
                }
                self.connect_state = staged;
                if let Err(why) = self.start_recovered_track() {
                    return self.handle_context_failure(why);
                }
                self.recovery_native = None;
                self.bootstrap_queue_ready = true;
            }
        } else if update_state {
            self.bootstrap_queue_ready = true;
        }
        self.maybe_finish_bootstrap();

        if update_state {
            self.emit_set_queue_event();
        }
        self.context_resolver.remove_used_and_invalid();
        update_state
    }

    fn restoration_pending(&self) -> bool {
        !self.restoration_aborted
            && (self.bootstrap_pending
                || self.transfer_state.is_some()
                || self.recovery_native.is_some())
    }

    fn restoration_request_deadline(&self) -> Option<Instant> {
        if self.restoration_aborted {
            Some(Instant::now() + Duration::from_secs(2))
        } else {
            self.restoration_deadline
                .filter(|_| self.restoration_pending())
        }
    }

    async fn await_restoration_request<T>(
        deadline: Option<Instant>,
        request: impl Future<Output = Result<T, Error>>,
    ) -> Result<T, Error> {
        match deadline {
            Some(deadline) => tokio::time::timeout_at(deadline, request)
                .await
                .map_err(Error::deadline_exceeded)?,
            None => request.await,
        }
    }

    fn expire_restoration(&mut self) -> bool {
        if !self.restoration_pending()
            || !self
                .restoration_deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return false;
        }
        warn!(target: "bopify_core::restoration", "restoration: deadline expired queue_ready={} playback_confirmed={}", self.bootstrap_queue_ready, self.bootstrap_playback_confirmed);
        self.end_restoration(crate::playback::player::ConnectionOutcome::Unavailable);
        true
    }

    fn handle_context_failure(&mut self, why: Error) -> bool {
        use crate::core::{error::ErrorKind, spclient::SpClientError};
        let required = (self.bootstrap_pending
            || self.transfer_state.is_some()
            || self.recovery_native.is_some())
            && self
                .context_resolver
                .next_is_required(self.connect_state.active_context);
        let transient = matches!(
            why.kind,
            ErrorKind::Unavailable
                | ErrorKind::DeadlineExceeded
                | ErrorKind::Aborted
                | ErrorKind::ResourceExhausted
        ) || matches!(
            why.error.downcast_ref::<SpClientError>(),
            Some(SpClientError::NoData)
        ) || matches!(
            why.error.downcast_ref::<crate::state::StateError>(),
            Some(crate::state::StateError::ContextHasNoTracks)
        );
        let within_deadline = self
            .restoration_deadline
            .is_none_or(|deadline| Instant::now() < deadline);
        if required && transient && within_deadline && self.context_resolver.retry_next() {
            warn!(target: "bopify_core::restoration", "restoration: context retry context={:?} kind={:?} attempt=2", self.connect_state.active_context, why.kind);
            return false;
        }
        warn!(target: "bopify_core::restoration", "restoration: context failed context={:?} kind={:?} required={required}", self.connect_state.active_context, why.kind);
        debug!("context resolution failed: {why}");
        self.context_resolver.mark_next_unavailable();
        self.context_resolver.remove_used_and_invalid();
        if required {
            self.end_restoration(crate::playback::player::ConnectionOutcome::Unavailable);
            return true;
        }
        false
    }

    fn end_restoration(&mut self, outcome: crate::playback::player::ConnectionOutcome) {
        if self.restoration_aborted {
            return;
        }
        self.player.stop();
        self.context_resolver.clear();
        self.bootstrap_pending = false;
        self.bootstrap_queue_ready = false;
        self.bootstrap_playback_confirmed = false;
        self.recovery_resume_requested = false;
        self.restoration_deadline = None;
        self.restoration_aborted = true;
        self.play_request_id = None;
        self.play_status = SpircPlayStatus::Stopped;
        self.connect_state.set_active(false);
        let state = self.connect_state.player_mut();
        state.is_playing = false;
        state.is_paused = true;
        state.is_buffering = false;
        self.player.emit_session_disconnected_event(
            self.session.connection_id(),
            self.session.username(),
            false,
        );
        self.player.emit_connection_ready(outcome);
        self.update_state = true;
    }

    /// Observe implicit repeat changes (including skips) after every event-loop mutation.
    fn emit_set_queue_event(&mut self) {
        if let Some(identity) = self.local_identity() {
            if self.last_identity.as_ref() != Some(&identity) {
                self.last_identity = Some(identity.clone());
                self.player.emit_recovery_identity(identity);
            }
        }
        self.connect_state
            .publish_repeat(&mut self.observed_repeat, |context, track| {
                self.player.emit_repeat_changed_event(context, track);
            });
        if !self.emit_set_queue_events {
            return;
        }
        if let Some(snapshot) = self.connect_state.queue_snapshot(&mut self.queue_tracker) {
            self.player.emit_set_queue_event(
                snapshot.generation,
                self.connect_state.context_uri().clone(),
                snapshot.current,
                snapshot.next,
                snapshot.previous,
            );
        }
    }

    // todo: is the time_delta still necessary?
    fn now_ms(&self) -> i64 {
        let dur = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|err| err.duration());

        dur.as_millis() as i64 + 1000 * self.session.time_delta()
    }

    async fn handle_command(&mut self, cmd: SpircCommand) -> Result<(), Error> {
        trace!("Received SpircCommand::{cmd:?}");
        match cmd {
            SpircCommand::SelectQueue(selection, reply) => {
                self.emit_set_queue_event();
                let playing = self.connect_state.is_playing();
                let result = if !self.connect_state.is_active() {
                    Err(QueueSelectionError::Unavailable)
                } else {
                    dispatch_selection(
                        &mut self.connect_state,
                        self.queue_tracker.generation,
                        selection,
                        |id| self.player.load(id, playing, 0),
                    )
                };
                if result.is_ok() {
                    self.add_autoplay_resolving_when_required();
                    self.record_loading(playing, 0);
                    self.emit_set_queue_event();
                }
                let _ = reply.send(result);
            }

            SpircCommand::Shutdown => {
                trace!("Received SpircCommand::Shutdown");
                self.handle_pause();
                self.handle_disconnect(false, true).await?;
                self.shutdown = true;
                if let Some(rx) = self.commands.as_mut() {
                    rx.close()
                }
            }
            SpircCommand::Transfer(request) if !self.connect_state.is_active() => {
                let device_id = self.session.device_id();
                Self::await_restoration_request(
                    self.restoration_request_deadline(),
                    self.session
                        .spclient()
                        .transfer(device_id, device_id, request.as_ref()),
                )
                .await?;
                return Ok(());
            }
            SpircCommand::Activate if !self.connect_state.is_active() => {
                trace!("Received SpircCommand::{cmd:?}");
                self.handle_activate();
                return self.notify().await;
            }
            SpircCommand::PlayHere if self.startup_restore && !self.connect_state.is_active() => {
                self.handle_play_here().await;
                return Ok(());
            }
            SpircCommand::Load(command) => {
                self.handle_load(command, None, None).await?;
                if !self.connect_state.is_active() {
                    self.handle_activate();
                }
            }
            SpircCommand::Transfer(..) | SpircCommand::Activate => {
                warn!("SpircCommand::{cmd:?} will be ignored while already active")
            }
            _ if !self.connect_state.is_active() => {
                warn!("SpircCommand::{cmd:?} will be ignored while Not Active")
            }
            SpircCommand::Disconnect { pause } => {
                if pause {
                    self.handle_pause()
                }
                return self.handle_disconnect(false, false).await;
            }
            SpircCommand::Play | SpircCommand::PlayHere => self.handle_play(),
            SpircCommand::PlayPause => self.handle_play_pause(),
            SpircCommand::Pause => self.handle_pause(),
            SpircCommand::Prev => self.handle_prev()?,
            SpircCommand::Next => self.handle_next(None)?,
            SpircCommand::VolumeUp => self.handle_volume_up(),
            SpircCommand::VolumeDown => self.handle_volume_down(),
            SpircCommand::Shuffle(shuffle) => self.handle_shuffle(shuffle)?,
            SpircCommand::Repeat(repeat) => self.handle_repeat_context(repeat)?,
            SpircCommand::RepeatTrack(repeat) => self.handle_repeat_track(repeat),
            SpircCommand::SetPosition(position) => self.handle_seek(position),
            SpircCommand::SetVolume(volume) => self.set_volume(volume),

            SpircCommand::AddToQueue(uri) => self.handle_add_to_queue(uri).await,
            SpircCommand::ClearQueue => {
                self.connect_state.clear_queue()?;
                self.emit_set_queue_event();
            }
        };

        self.notify().await
    }

    fn handle_player_event(&mut self, event: PlayerEvent) -> Result<(), Error> {
        if self.restoration_aborted
            || (!self.connect_state.is_active()
                && matches!(self.play_status, SpircPlayStatus::Stopped))
        {
            return Ok(());
        }
        if let PlayerEvent::TrackChanged { audio_item } = event {
            self.connect_state.update_duration(audio_item.duration_ms);
            self.update_state = true;
            return Ok(());
        }

        // update play_request_id
        if let PlayerEvent::PlayRequestIdChanged { play_request_id } = event {
            self.play_request_id = Some(play_request_id);
            return Ok(());
        }

        let is_current_track = matches! {
            (event.get_play_request_id(), self.play_request_id),
            (Some(event_id), Some(current_id)) if event_id == current_id
        };

        // we only process events if the play_request_id matches. If it doesn't, it is
        // an event that belongs to a previous track and only arrives now due to a race
        // condition. In this case we have updated the state already and don't want to
        // mess with it.
        if !is_current_track {
            return Ok(());
        }

        match event {
            PlayerEvent::EndOfTrack { .. } => {
                let next_track = self
                    .connect_state
                    .repeat_track()
                    .then(|| self.connect_state.current_track(|t| t.uri.clone()));

                self.handle_next(next_track)?
            }
            PlayerEvent::Loading { .. } => match self.play_status {
                SpircPlayStatus::LoadingPlay { position_ms } => {
                    self.connect_state
                        .update_position(position_ms, self.now_ms());
                    trace!("==> LoadingPlay");
                }
                SpircPlayStatus::LoadingPause { position_ms } => {
                    self.connect_state
                        .update_position(position_ms, self.now_ms());
                    trace!("==> LoadingPause");
                }
                _ => {
                    self.connect_state.update_position(0, self.now_ms());
                    trace!("==> Loading");
                }
            },
            PlayerEvent::Seeked { position_ms, .. } => {
                trace!("==> Seeked");
                self.connect_state
                    .update_position(position_ms, self.now_ms())
            }
            PlayerEvent::Playing { position_ms, .. }
            | PlayerEvent::PositionCorrection { position_ms, .. } => {
                self.bootstrap_playback_confirmed = true;
                trace!("==> Playing");
                let new_nominal_start_time = self.now_ms() - position_ms as i64;
                match self.play_status {
                    SpircPlayStatus::Playing {
                        ref mut nominal_start_time,
                        ..
                    } => {
                        if (*nominal_start_time - new_nominal_start_time).abs() > 100 {
                            *nominal_start_time = new_nominal_start_time;
                            self.connect_state
                                .update_position(position_ms, self.now_ms());
                        } else {
                            self.maybe_finish_bootstrap();
                            return Ok(());
                        }
                    }
                    SpircPlayStatus::LoadingPlay { .. } | SpircPlayStatus::LoadingPause { .. } => {
                        self.connect_state
                            .update_position(position_ms, self.now_ms());
                        self.play_status = SpircPlayStatus::Playing {
                            nominal_start_time: new_nominal_start_time,
                            preloading_of_next_track_triggered: false,
                        };
                    }
                    _ => return Ok(()),
                }
            }
            PlayerEvent::Paused {
                position_ms: new_position_ms,
                ..
            } => {
                trace!("==> Paused");
                match self.play_status {
                    SpircPlayStatus::Paused { .. } | SpircPlayStatus::Playing { .. } => {
                        self.connect_state
                            .update_position(new_position_ms, self.now_ms());
                        self.play_status = SpircPlayStatus::Paused {
                            position_ms: new_position_ms,
                            preloading_of_next_track_triggered: false,
                        };
                    }
                    SpircPlayStatus::LoadingPlay { .. } | SpircPlayStatus::LoadingPause { .. } => {
                        self.connect_state
                            .update_position(new_position_ms, self.now_ms());
                        self.play_status = SpircPlayStatus::Paused {
                            position_ms: new_position_ms,
                            preloading_of_next_track_triggered: false,
                        };
                    }
                    _ => return Ok(()),
                }
            }
            PlayerEvent::Stopped { .. } => {
                trace!("==> Stopped");
                match self.play_status {
                    SpircPlayStatus::Stopped => return Ok(()),
                    _ => self.play_status = SpircPlayStatus::Stopped,
                }
            }
            PlayerEvent::TimeToPreloadNextTrack { .. } => {
                self.handle_preload_next_track();
                return Ok(());
            }
            PlayerEvent::Unavailable { track_id, .. } => {
                self.handle_unavailable(&track_id)?;
                if self.connect_state.current_track(|t| &t.uri) == &track_id.to_uri() {
                    self.handle_next(None)?
                }
            }
            _ => return Ok(()),
        }

        self.update_state = true;
        if matches!(
            self.play_status,
            SpircPlayStatus::Paused { .. } | SpircPlayStatus::Playing { .. }
        ) {
            self.bootstrap_playback_confirmed = true;
            self.maybe_finish_bootstrap();
        }
        Ok(())
    }

    async fn handle_connection_id_update(&mut self, connection_id: String) -> Result<(), Error> {
        trace!("Received connection ID update: {connection_id:?}");
        self.session.set_connection_id(&connection_id);

        let deadline = self.restoration_request_deadline();
        #[cfg(test)]
        let stall_publication = std::mem::take(&mut self.stall_new_device_publication);
        let publication = async {
            #[cfg(test)]
            if stall_publication {
                std::future::pending::<()>().await;
            }
            self.connect_state
                .notify_new_device_appeared(&self.session)
                .await
        };
        let cluster = match Self::await_restoration_request(deadline, publication).await {
            Ok(res) => Cluster::parse_from_bytes(&res).ok(),
            Err(why) => {
                if self.expire_restoration() {
                    return Ok(());
                }
                error!("{why:?}");
                None
            }
        }
        .ok_or(SpircError::FailedDealerSetup)?;

        debug!(
            "successfully put connect state for {} with connection-id {connection_id}",
            self.session.device_id()
        );

        self.connect_established = true;
        self.latest_owner.clone_from(&cluster.active_device_id);

        if self.startup_restore && !self.startup_resolved {
            self.startup_resolved = true;
            if matches!(self.startup_mode, crate::StartupMode::Recovery(_)) {
                self.resolve_recovery(&cluster)?;
                return Ok(());
            }
            let outcome = self.resolve_startup(&cluster).await;
            match &outcome {
                StartupOutcome::Remote { .. } => self
                    .player
                    .emit_connection_ready(crate::playback::player::ConnectionOutcome::Remote),
                StartupOutcome::Nothing => self
                    .player
                    .emit_connection_ready(crate::playback::player::ConnectionOutcome::Unavailable),
                _ => {
                    self.bootstrap_pending = true;
                    self.restoration_deadline = Some(Instant::now() + RESTORATION_TIMEOUT);
                    self.bootstrap_queue_ready = self.transfer_state.is_none()
                        && self
                            .connect_state
                            .get_context(self.connect_state.active_context)
                            .is_ok();
                }
            }
            self.player.emit_startup_resolved_event(outcome);
            return Ok(());
        }

        let same_session = cluster.player_state.session_id == self.session.session_id()
            || cluster.player_state.session_id.is_empty();
        if !cluster.active_device_id.is_empty() || !same_session {
            info!(
                "active device is <{}> with session <{}>",
                cluster.active_device_id, cluster.player_state.session_id
            );
            return Ok(());
        } else if cluster.transfer_data.is_empty() {
            debug!("got empty transfer state, do nothing");
            return Ok(());
        } else {
            info!(
                "trying to take over control automatically, session_id: {}",
                cluster.player_state.session_id
            )
        }

        use protobuf::Message;

        match TransferState::parse_from_bytes(&cluster.transfer_data) {
            Ok(transfer_state) => self.handle_transfer(transfer_state)?,
            Err(why) => error!("failed to take over control: {why}"),
        }

        Ok(())
    }

    /// Bopify: never returns an error, because an `Err` from the connection-id
    /// handler ends the Spirc task, and Swift would see a lost session.
    async fn resolve_startup(&mut self, cluster: &Cluster) -> StartupOutcome {
        match startup_decision(cluster, self.session.device_id()) {
            StartupDecision::Remote(remote) => {
                let device_name = remote.device_name.clone();
                // Emitted with the outcome, so the mirror appears at launch.
                self.publish_remote(Some(remote));
                return StartupOutcome::Remote { device_name };
            }
            StartupDecision::Restore(transfer) => {
                if self.restore_session(transfer) {
                    return StartupOutcome::Restored;
                }
            }
            StartupDecision::Fallback => {}
        }
        self.load_liked_songs(false).await
    }

    fn foreign_owner(&self) -> bool {
        !self.latest_owner.is_empty() && self.latest_owner != self.session.device_id()
    }

    fn local_identity(&self) -> Option<crate::RecoveryIdentity> {
        let state = self.connect_state.player();
        let track = state.track.as_ref()?;
        if track.uri.is_empty() {
            return None;
        }
        Some(crate::RecoveryIdentity {
            track_uri: track.uri.clone(),
            track_uid: (!track.uid.is_empty()).then(|| track.uid.clone()),
            context_uri: (!state.context_uri.is_empty() && state.context_uri != "-")
                .then(|| state.context_uri.clone()),
        })
    }

    fn resolve_recovery(&mut self, cluster: &Cluster) -> Result<(), Error> {
        use crate::playback::player::ConnectionOutcome;
        use crate::recovery::{RecoveryDecision, recovery_decision};
        self.bootstrap_pending = true;
        self.restoration_deadline = Some(Instant::now() + RESTORATION_TIMEOUT);
        match recovery_decision(cluster, self.session.device_id()) {
            RecoveryDecision::Foreign => {
                info!("recovery: foreign owner, remain inactive");
                self.bootstrap_pending = false;
                self.publish_remote(remote_playback(cluster, self.session.device_id()));
                self.player.emit_connection_ready(ConnectionOutcome::Remote);
            }
            RecoveryDecision::NoSession => {
                info!("recovery: no usable session");
                self.bootstrap_pending = false;
                self.player
                    .emit_connection_ready(ConnectionOutcome::Unavailable);
            }
            RecoveryDecision::Transfer(transfer) => {
                info!("recovery: restore paused transfer");
                let supplied_options = transfer.options.is_some();
                if !self.restore_session(transfer) {
                    self.bootstrap_pending = false;
                    self.player
                        .emit_connection_ready(ConnectionOutcome::Unavailable);
                } else {
                    self.apply_recovery_options(supplied_options);
                    self.apply_recovery_position();
                    self.bootstrap_queue_ready = self.transfer_state.is_none()
                        && self
                            .connect_state
                            .get_context(self.connect_state.active_context)
                            .is_ok();
                }
            }
            RecoveryDecision::OwnState(mut native) => {
                info!("recovery: restore own native session");
                let supplied_options = native.options.is_some();
                native.is_playing = false;
                native.is_paused = true;
                native.is_buffering = false;
                self.connect_state.restore_recovery_player_state(*native)?;
                self.apply_recovery_options(supplied_options);
                self.recovery_native = Some(self.connect_state.player().clone());
                let context = if self.connect_state.active_context == ContextType::Autoplay {
                    self.connect_state.context_uri().replace("station:", "")
                } else {
                    self.connect_state.context_uri().clone()
                };
                let track = self.connect_state.current_track(|t| t.uri.clone());
                let resolve = ResolveContext::from_uri(
                    context.clone(),
                    track.clone(),
                    ContextType::Default,
                    ContextAction::Replace,
                );
                if self.connect_state.active_context == ContextType::Default {
                    self.context_resolver.add_required(resolve);
                } else {
                    self.context_resolver.add(resolve);
                }
                if self.connect_state.active_context == ContextType::Autoplay {
                    self.context_resolver.add_required(ResolveContext::from_uri(
                        context,
                        track,
                        ContextType::Autoplay,
                        ContextAction::Replace,
                    ));
                }
            }
        }
        Ok(())
    }

    fn apply_recovery_options(&mut self, supplied: bool) {
        if supplied {
            return;
        }
        if let (crate::StartupMode::Recovery(Some(checkpoint)), Some(identity)) =
            (&self.startup_mode, self.local_identity())
        {
            if checkpoint.matches(&identity) {
                let options = self
                    .connect_state
                    .player_mut()
                    .options
                    .mut_or_insert_default();
                options.shuffling_context = checkpoint.shuffle_enabled;
                options.repeating_context = checkpoint.repeat_mode == "context";
                options.repeating_track = checkpoint.repeat_mode == "track";
            }
        }
    }

    fn recovery_position(&self) -> u32 {
        let state = self.connect_state.player();
        let mut position = state.position_as_of_timestamp.max(0);
        if let (crate::StartupMode::Recovery(Some(checkpoint)), Some(identity)) =
            (&self.startup_mode, self.local_identity())
        {
            if checkpoint.matches(&identity) {
                position = checkpoint.position_ms;
            }
        }
        if state.duration > 0 {
            position = position.min(state.duration);
        }
        u32::try_from(position).unwrap_or(u32::MAX)
    }

    fn apply_recovery_position(&mut self) {
        let position = self.recovery_position();
        self.player.seek(position);
        self.connect_state.update_position(position, self.now_ms());
    }

    fn start_recovered_track(&mut self) -> Result<(), Error> {
        if self.foreign_owner() {
            return Ok(());
        }
        let position = self.recovery_position();
        self.load_track(false, position)?;
        self.handle_activate();
        self.emit_set_queue_event();
        Ok(())
    }

    fn maybe_finish_bootstrap(&mut self) {
        if self.expire_restoration() {
            return;
        }
        if !self.bootstrap_pending
            || !self.bootstrap_queue_ready
            || !self.bootstrap_playback_confirmed
            || (self.foreign_owner()
                && matches!(self.startup_mode, crate::StartupMode::Recovery(_)))
        {
            return;
        }
        let Some(identity) = self.local_identity() else {
            return;
        };
        if !self.recovery_resume_requested {
            if let crate::StartupMode::Recovery(Some(checkpoint)) = &self.startup_mode {
                if checkpoint.should_resume && checkpoint.matches(&identity) {
                    self.recovery_resume_requested = true;
                    self.bootstrap_playback_confirmed = false;
                    self.handle_play();
                    return;
                }
            }
        }
        self.emit_set_queue_event();
        self.bootstrap_pending = false;
        self.restoration_deadline = None;
        self.player
            .emit_connection_ready(crate::playback::player::ConnectionOutcome::Local(identity));
        info!("recovery: local playback confirmed");
    }

    /// Takes the last session over paused, as `startup_decision` prepared it.
    /// Returns false when no track was loaded, after dropping the pending
    /// transfer so it is not finished on top of Liked Songs.
    fn restore_session(&mut self, transfer: TransferState) -> bool {
        match self.handle_transfer(transfer) {
            // handle_transfer returns Ok without loading when the session names no
            // playable track, so only a paused load counts as restored.
            Ok(()) if matches!(self.play_status, SpircPlayStatus::LoadingPause { .. }) => {
                return true;
            }
            Ok(()) => warn!("restore: the last session had no track to restore"),
            Err(why) => warn!("restore: restoring the last session failed: {why}"),
        }
        // A half-applied transfer must not be finished later on top of Liked Songs.
        self.transfer_state = None;
        false
    }

    /// Bopify: logs its own failures, because the command loop only debug-logs
    /// an error. The cluster comes from the same PUT whose reply the startup
    /// decision reads, since cluster updates are not known to carry the full last session.
    async fn handle_play_here(&mut self) {
        use protobuf::Message as _;
        if self
            .play_here_requested_at
            .is_some_and(|at| self.now_ms() - at < PLAY_HERE_WINDOW_MS)
        {
            debug!("play here: a transfer request is already pending");
            return;
        }
        let cluster = match Self::await_restoration_request(
            self.restoration_request_deadline(),
            self.connect_state.notify_new_device_appeared(&self.session),
        )
        .await
        {
            Ok(bytes) => Cluster::parse_from_bytes(&bytes).ok(),
            Err(why) => {
                warn!("play here: fetching the cluster failed: {why}");
                return;
            }
        };
        let Some(cluster) = cluster else {
            warn!("play here: the state update returned no cluster");
            return;
        };
        let decision = startup_decision(&cluster, self.session.device_id());
        let kind = match &decision {
            StartupDecision::Remote(_) => "remote",
            StartupDecision::Restore(_) => "restore",
            StartupDecision::Fallback => "fallback",
        };
        // Visible in a diagnostic build: tells a Liked Songs fallback from a thin reply.
        debug!(
            "play here: decision={kind} transfer_data={} bytes",
            cluster.transfer_data.len()
        );
        match decision {
            StartupDecision::Remote(_) => {
                // Set before the request goes out, so the transfer the server sends back
                // plays even if the mirrored device was paused.
                self.play_here_requested_at = Some(self.now_ms());
                // Spawned, so this loop can answer the transfer request the server sends
                // before it replies; awaiting here dropped the dealer connection (run 5).
                let spclient = self.session.spclient().clone();
                let device_id = self.session.device_id().to_owned();
                tokio::spawn(async move {
                    // Run 5 moved playback despite a 410, so the reply is only logged.
                    if let Err(why) = spclient.transfer(&device_id, &device_id, None).await {
                        warn!("play here: transfer replied {why}");
                    }
                });
                return;
            }
            StartupDecision::Restore(transfer) => {
                if self.restore_session(transfer) {
                    self.handle_play();
                    if let Err(why) = self.notify().await {
                        warn!("play here: publishing the restored session failed: {why}");
                    }
                    return;
                }
            }
            StartupDecision::Fallback => {}
        }
        self.load_liked_songs(true).await;
    }

    /// Mirrors the Activate and Load command paths, but keeps the load's
    /// result, which the command path only debug-logs.
    async fn load_liked_songs(&mut self, start_playing: bool) -> StartupOutcome {
        self.handle_activate();
        let uri = format!("spotify:user:{}:collection", self.session.username());
        let request = LoadRequest::from_context_uri(
            uri,
            LoadRequestOptions {
                start_playing,
                ..Default::default()
            },
        );
        let outcome = match self.handle_load(request, None, None).await {
            Ok(()) => StartupOutcome::LikedSongs,
            Err(why) => {
                warn!("liked songs: loading failed: {why}");
                StartupOutcome::Nothing
            }
        };
        if let Err(why) = self.notify().await {
            warn!("liked songs: state update failed: {why}");
        }
        outcome
    }

    fn publish_remote(&mut self, remote: Option<RemoteState>) {
        if remote_changed(&mut self.last_remote, &remote) {
            self.player.emit_remote_playback_event(remote);
        }
    }

    fn handle_user_attributes_update(&mut self, update: UserAttributesUpdate) {
        trace!("Received attributes update: {update:#?}");
        let attributes: UserAttributes = update
            .pairs
            .iter()
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
            .collect();
        self.session.set_user_attributes(attributes)
    }

    fn handle_user_attributes_mutation(&mut self, mutation: UserAttributesMutation) {
        for attribute in mutation.fields.iter() {
            let key = &attribute.name;

            if key == "autoplay" && self.session.config().autoplay.is_some() {
                trace!("Autoplay override active. Ignoring mutation.");
                continue;
            }

            if let Some(old_value) = self.session.user_data().attributes.get(key) {
                let new_value = match old_value.as_ref() {
                    "0" => "1",
                    "1" => "0",
                    _ => old_value,
                };
                self.session.set_user_attribute(key, new_value);

                trace!("Received attribute mutation, {key} was {old_value} is now {new_value}");

                if key == "filter-explicit-content" && new_value == "1" {
                    self.player
                        .emit_filter_explicit_content_changed_event(matches!(new_value, "1"));
                }

                if key == "autoplay" && old_value != new_value {
                    self.player
                        .emit_auto_play_changed_event(matches!(new_value, "1"));

                    self.add_autoplay_resolving_when_required()
                }
            } else {
                trace!("Received attribute mutation for {key} but key was not found!");
            }
        }
    }

    async fn handle_cluster_update(
        &mut self,
        mut cluster_update: ClusterUpdate,
    ) -> Result<(), Error> {
        let reason = cluster_update.update_reason.enum_value();

        let device_ids = cluster_update.devices_that_changed.join(", ");
        debug!(
            "cluster update: {reason:?} from {device_ids}, active device: {}",
            cluster_update.cluster.active_device_id
        );

        if let Some(cluster) = cluster_update.cluster.take() {
            let owner = if cluster.active_device_id.is_empty() {
                "empty"
            } else if cluster.active_device_id == self.session.device_id() {
                "self"
            } else {
                "other"
            };
            debug!(target: "bopify_core::ownership", "ownership: cluster spirc={} owner={owner} changed_ms={} server_ms={} reason={reason:?} active={} restoring={}", self.spirc_id, cluster.changed_timestamp_ms, cluster.server_timestamp_ms, self.connect_state.is_active(), self.restoration_pending());
            // A publication reply can arrive before an older queued dealer update.
            // Compare server snapshot versions, never the device's wall clock.
            if cluster.changed_timestamp_ms > 0 {
                if self
                    .last_cluster_timestamp
                    .is_some_and(|last| cluster.changed_timestamp_ms < last)
                {
                    debug!(target: "bopify_core::ownership", "ownership: stale_cluster spirc={} changed_ms={} confirmed_ms={:?}", self.spirc_id, cluster.changed_timestamp_ms, self.last_cluster_timestamp);
                    return Ok(());
                }
                self.last_cluster_timestamp = Some(cluster.changed_timestamp_ms);
            }
            self.latest_owner.clone_from(&cluster.active_device_id);
            if self.bootstrap_pending
                && self.foreign_owner()
                && matches!(self.startup_mode, crate::StartupMode::Recovery(_))
            {
                self.recovery_native = None;
                self.transfer_state = None;
                self.end_restoration(crate::playback::player::ConnectionOutcome::Remote);
            }
            let became_inactive = self.connect_state.is_active()
                && cluster.active_device_id != self.session.device_id();
            if became_inactive {
                info!("device became inactive");
                self.handle_disconnect(cluster.active_device_id.is_empty(), true)
                    .await?;
                self.handle_stop();
            } else if self.connect_state.is_active() {
                // fixme: workaround fix, because of missing information why it behaves like it does
                //  background: when another device sends a connect-state update, some player's position de-syncs
                //  tried: providing session_id, playback_id, track-metadata "track_player"
                self.update_state = true;
            }

            // Bopify: this includes the update that just deactivated this device.
            if self.startup_restore && !self.connect_state.is_active() {
                let remote = remote_playback(&cluster, self.session.device_id());
                self.publish_remote(remote);
            }
        } else if self.connect_state.is_active() {
            Self::await_restoration_request(
                self.restoration_request_deadline(),
                self.connect_state.became_inactive(&self.session),
            )
            .await?;
        }

        Ok(())
    }

    async fn handle_connect_state_request(
        &mut self,
        (request, sender): RequestReply,
    ) -> Result<(), Error> {
        self.connect_state.set_last_command(request.clone());

        debug!(
            "handling: '{}' from {}",
            request.command, request.sent_by_device_id
        );

        let response = match self.handle_request(request).await {
            Ok(_) => Reply::Success,
            Err(why) => {
                error!("failed to handle request: {why}");
                Reply::Failure
            }
        };

        sender.send(response).map_err(Into::into)
    }

    async fn handle_request(&mut self, request: Request) -> Result<(), Error> {
        use Command::*;

        match request.command {
            // errors and unknown commands
            Transfer(transfer) if transfer.data.is_none() => {
                warn!("transfer endpoint didn't contain any data to transfer");
                Err(SpircError::NoData)?
            }
            Unknown(unknown) => Err(SpircError::UnknownEndpoint(unknown))?,
            // implicit update of the connect_state
            UpdateContext(update_context) => {
                if matches!(update_context.context.uri, Some(ref uri) if uri != self.connect_state.context_uri())
                {
                    debug!(
                        "ignoring context update for <{:?}>, because it isn't the current context <{}>",
                        update_context.context.uri,
                        self.connect_state.context_uri()
                    )
                } else {
                    self.context_resolver.add(ResolveContext::from_context(
                        update_context.context,
                        ContextType::Default,
                        ContextAction::Replace,
                    ))
                }
                return Ok(());
            }
            // modification and update of the connect_state
            Transfer(transfer) => {
                // Taken before handle_transfer, so an error can't leave it set for a later transfer.
                let play_here = self
                    .play_here_requested_at
                    .take()
                    .is_some_and(|at| self.now_ms() - at < PLAY_HERE_WINDOW_MS);
                self.handle_transfer(transfer.data.expect("by condition checked"))?;
                if play_here {
                    // Bopify: play-here asked to play, even if the mirrored device was paused.
                    debug!("play here: playing the transferred session");
                    self.handle_play();
                }
                return self.notify().await;
            }
            Play(mut play) => {
                if !self.connect_state.is_active() {
                    self.handle_activate()
                }

                let context = match play.context.uri {
                    Some(s) => PlayContext::Uri(s),
                    None if !play.context.pages.is_empty() => PlayContext::Tracks(
                        play.context
                            .pages
                            .iter()
                            .cloned()
                            .flat_map(|p| p.tracks)
                            .flat_map(|t| t.uri)
                            .collect(),
                    ),
                    None => Err(SpircError::NoUri("context"))?,
                };

                let context_options = play
                    .options
                    .player_options_override
                    .map(Into::into)
                    .map(LoadContextOptions::Options);

                let fallback_index = play
                    .options
                    .skip_to
                    .as_ref()
                    .and_then(|s| s.track_index)
                    .map(|i| i as usize);

                self.handle_load(
                    LoadRequest {
                        context,
                        options: LoadRequestOptions {
                            start_playing: true,
                            seek_to: play.options.seek_to.unwrap_or_default(),
                            playing_track: play.options.skip_to.and_then(|s| s.try_into().ok()),
                            context_options,
                        },
                    },
                    play.context.pages.pop(),
                    fallback_index,
                )
                .await?;

                self.connect_state.set_origin(play.play_origin)
            }
            Pause(_) => self.handle_pause(),
            SeekTo(seek_to) => {
                // for some reason the position is stored in value, not in position
                trace!("seek to {seek_to:?}");
                self.handle_seek(seek_to.value)
            }
            SetShufflingContext(shuffle) => self.handle_shuffle(shuffle.value)?,
            SetRepeatingContext(repeat_context) => {
                self.handle_repeat_context(repeat_context.value)?
            }
            SetRepeatingTrack(repeat_track) => self.handle_repeat_track(repeat_track.value),
            AddToQueue(add_to_queue) => {
                self.connect_state.add_to_queue(add_to_queue.track, true);
                self.emit_set_queue_event();
            }
            SetQueue(set_queue) => {
                self.connect_state.handle_set_queue(set_queue);
                self.emit_set_queue_event();
            }
            SetOptions(set_options) => {
                if let Some(repeat_context) = set_options.repeating_context {
                    self.handle_repeat_context(repeat_context)?
                }

                if let Some(repeat_track) = set_options.repeating_track {
                    self.handle_repeat_track(repeat_track)
                }

                let shuffle = set_options.shuffling_context;
                if let Some(shuffle) = shuffle {
                    self.handle_shuffle(shuffle)?;
                }
            }
            SkipNext(skip_next) => self.handle_next(skip_next.track.map(|t| t.uri))?,
            SkipPrev(_) => self.handle_prev()?,
            Resume(_) if matches!(self.play_status, SpircPlayStatus::Stopped) => {
                self.load_track(true, 0)?
            }
            Resume(_) => self.handle_play(),
        }

        self.update_state = true;
        Ok(())
    }

    fn handle_transfer(&mut self, mut transfer: TransferState) -> Result<(), Error> {
        let track = self.connect_state.current_track_from_transfer(&transfer)?;
        let mut ctx_uri = match transfer.current_session.context.uri {
            None => Err(SpircError::NoUri("transfer context"))?,
            // can apparently happen when a state is transferred and was started with "uris" via the api
            Some(ref uri) if uri == "-" || uri.is_empty() => None,
            Some(ref uri) => Some(uri.clone()),
        };

        self.context_resolver.clear();
        self.transfer_state = None;
        self.recovery_native = None;
        self.restoration_aborted = false;
        self.restoration_deadline = Some(Instant::now() + RESTORATION_TIMEOUT);
        self.bootstrap_queue_ready = false;
        self.bootstrap_playback_confirmed = false;
        self.recovery_resume_requested = false;
        self.connect_state.queue_replaced = true;

        self.connect_state.reset_context(
            ctx_uri
                .as_deref()
                .map(ResetContext::WhenDifferent)
                .unwrap_or(ResetContext::Completely),
        );

        self.connect_state.set_track(track);

        let autoplay = self
            .connect_state
            .current_track(|track| ContextType::for_track(track))
            == ContextType::Autoplay;
        if autoplay {
            ctx_uri = ctx_uri.map(|c| c.replace("station:", ""));
        }

        let fallback = self.connect_state.current_track(|t| &t.uri).clone();
        let load_from_context_uri = ctx_uri.is_some();

        match ctx_uri {
            Some(ref uri) => {
                let resolve = ResolveContext::from_uri(
                    uri.clone(),
                    &fallback,
                    ContextType::Default,
                    ContextAction::Replace,
                );
                if autoplay {
                    self.context_resolver.add(resolve);
                } else {
                    self.context_resolver.add_required(resolve);
                }
            }
            None => {
                let all_tracks = transfer
                    .current_session
                    .context
                    .pages
                    .iter()
                    .cloned()
                    .flat_map(|p| p.tracks)
                    .collect::<Vec<_>>();

                if !all_tracks.is_empty() {
                    if autoplay {
                        self.connect_state.update_context(
                            Context {
                                uri: Some("spotify:web-api".into()),
                                pages: vec![all_tracks.into()],
                                ..Default::default()
                            },
                            ContextType::Autoplay,
                        )?;
                    } else {
                        self.load_context_from_tracks(all_tracks)?;
                    }
                } else {
                    warn!(
                        "tried to transfer with an invalid state, using fallback as ctx_uri ({fallback})"
                    );
                    ctx_uri = Some(fallback.clone())
                }
            }
        };

        self.handle_activate();

        let timestamp = self.now_ms();
        let state = &mut self.connect_state;
        state.handle_initial_transfer(&mut transfer, ctx_uri.clone());

        // adjust active context, so resolve knows for which context it should set up the state
        state.active_context = if autoplay {
            ContextType::Autoplay
        } else {
            ContextType::Default
        };

        let position = transfer_position(&transfer.playback, timestamp);

        let is_playing = !transfer.playback.is_paused();

        if self.connect_state.current_track(|t| t.is_autoplay()) || autoplay {
            if let Some(ctx_uri) = ctx_uri {
                debug!("currently in autoplay context, async resolving autoplay for {ctx_uri}");
                self.context_resolver.add_required(ResolveContext::from_uri(
                    ctx_uri,
                    fallback,
                    ContextType::Autoplay,
                    ContextAction::Replace,
                ))
            } else if self
                .connect_state
                .get_context(ContextType::Autoplay)
                .is_err()
            {
                warn!("couldn't resolve autoplay context without a context uri");
            }
        }

        if load_from_context_uri {
            self.transfer_state = Some(transfer);
        } else {
            match self
                .connect_state
                .get_context(self.connect_state.active_context)
            {
                Err(why) => {
                    warn!("continuing transfer in an unknown state. {why}");
                    self.transfer_state = Some(transfer);
                    self.context_resolver.add_required(ResolveContext::from_uri(
                        self.connect_state.context_uri().clone(),
                        self.connect_state.current_track(|track| track.uri.clone()),
                        self.connect_state.active_context,
                        ContextAction::Replace,
                    ));
                }
                Ok(_) => {
                    let mut staged = self.connect_state.clone();
                    staged.finish_transfer(transfer)?;
                    self.connect_state = staged;
                    self.bootstrap_queue_ready = true;
                }
            }
        }

        self.load_track(is_playing, position.try_into()?)
    }

    async fn handle_disconnect(
        &mut self,
        connection_lost: bool,
        stop_local: bool,
    ) -> Result<(), Error> {
        debug!(target: "bopify_core::ownership", "ownership: disconnect spirc={} active={} connection_lost={connection_lost}", self.spirc_id, self.connect_state.is_active());
        if connection_lost {
            // Report loss before cleanup can publish a stopped or inactive state.
            self.player.emit_session_disconnected_event(
                self.session.connection_id(),
                self.session.username(),
                true,
            );
        }
        self.context_resolver.clear();

        self.play_status = SpircPlayStatus::Stopped {};
        self.connect_state
            .update_position_in_relation(self.now_ms());
        // Local ownership and audio must be released even if publication fails.
        self.connect_state.set_active(false);
        if stop_local {
            self.play_request_id = None;
            self.player.stop();
        }
        if !connection_lost {
            self.player.emit_session_disconnected_event(
                self.session.connection_id(),
                self.session.username(),
                false,
            );
        }
        self.notify().await?;

        Self::await_restoration_request(
            self.restoration_request_deadline(),
            self.connect_state.became_inactive(&self.session),
        )
        .await?;
        debug!(target: "bopify_core::ownership", "ownership: inactive spirc={} active={}", self.spirc_id, self.connect_state.is_active());

        Ok(())
    }

    fn handle_stop(&mut self) {
        self.player.stop();
        self.connect_state.update_position(0, self.now_ms());
        self.connect_state.clear_next_tracks();

        if let Err(why) = self.connect_state.reset_playback_to_position(None) {
            warn!("failed filling up next_track during stopping: {why}")
        }
    }

    fn handle_activate(&mut self) {
        // Bopify: Swift drops the mirror on becoming active, so the next takeover must emit
        // again, and a transfer that never arrived must not force-play a later one.
        self.last_remote = None;
        self.play_here_requested_at = None;

        self.connect_state.set_active(true);
        self.player
            .emit_session_connected_event(self.session.connection_id(), self.session.username());
        self.player.emit_session_client_changed_event(
            self.session.client_id(),
            self.session.client_name(),
            self.session.client_brand_name(),
            self.session.client_model_name(),
        );

        self.player
            .emit_volume_changed_event(self.connect_state.device_info().volume as u16);

        self.player
            .emit_auto_play_changed_event(self.session.autoplay());

        self.player
            .emit_filter_explicit_content_changed_event(self.session.filter_explicit_content());

        self.player
            .emit_shuffle_changed_event(self.connect_state.shuffling_context());

        self.player.emit_repeat_changed_event(
            self.connect_state.repeat_context(),
            self.connect_state.repeat_track(),
        );
    }

    async fn handle_load(
        &mut self,
        cmd: LoadRequest,
        page: Option<ContextPage>,
        fallback_index: Option<usize>,
    ) -> Result<(), Error> {
        self.transfer_state = None;
        self.recovery_native = None;
        self.context_resolver.clear();
        self.restoration_aborted = false;
        self.bootstrap_queue_ready = false;
        self.bootstrap_playback_confirmed = false;
        self.recovery_resume_requested = false;
        if matches!(self.startup_mode, crate::StartupMode::Recovery(_)) {
            self.startup_mode = crate::StartupMode::Recovery(None);
        }
        self.restoration_deadline = self
            .bootstrap_pending
            .then(|| Instant::now() + RESTORATION_TIMEOUT);
        self.connect_state.queue_replaced = true;
        self.connect_state
            .reset_context(if let PlayContext::Uri(ref uri) = cmd.context {
                ResetContext::WhenDifferent(uri)
            } else {
                ResetContext::Completely
            });

        self.connect_state.reset_options();

        let autoplay = matches!(cmd.context_options, Some(LoadContextOptions::Autoplay));
        match cmd.context {
            PlayContext::Uri(uri) => {
                self.load_context_from_uri(uri, page.as_ref(), autoplay)
                    .await?
            }
            PlayContext::Tracks(tracks) => self.load_context_from_tracks(tracks)?,
        }

        let cmd_options = cmd.options;

        self.connect_state.set_active_context(ContextType::Default);

        // for play commands with skip by uid, the context of the command contains
        // tracks with uri and uid, so we merge the new context with the resolved/existing context
        self.connect_state.merge_context(page);

        // load here, so that we clear the queue only after we definitely retrieved a new context
        self.connect_state.clear_next_tracks();
        self.connect_state.clear_restrictions();

        debug!("play track <{:?}>", cmd_options.playing_track);

        let index = match cmd_options.playing_track {
            None => None,
            Some(ref playing_track) => Some(match playing_track {
                PlayingTrack::Index(i) => Ok(*i as usize),
                PlayingTrack::Uri(uri) => {
                    let ctx = self.connect_state.get_context(ContextType::Default)?;
                    ConnectState::find_index_in_context(ctx, |t| &t.uri == uri)
                }
                PlayingTrack::Uid(uid) => {
                    let ctx = self.connect_state.get_context(ContextType::Default)?;
                    ConnectState::find_index_in_context(ctx, |t| &t.uid == uid)
                }
            }),
        }
        .map(|i| {
            i.unwrap_or_else(|why| {
                warn!(
                    "Failed to resolve index by {:?}, using fallback index: {:?} (Error: {why})",
                    cmd_options.playing_track, fallback_index
                );
                fallback_index.unwrap_or_default()
            })
        });

        if let Some(LoadContextOptions::Options(ref options)) = cmd_options.context_options {
            debug!(
                "loading with shuffle: <{}>, repeat track: <{}> context: <{}>",
                options.shuffle, options.repeat, options.repeat_track
            );

            self.connect_state.set_shuffle(options.shuffle);
            self.connect_state.set_repeat_context(options.repeat);
            self.connect_state.set_repeat_track(options.repeat_track);
        }

        if matches!(cmd_options.context_options, Some(LoadContextOptions::Options(ref o)) if o.shuffle)
        {
            if let Some(index) = index {
                self.connect_state.set_current_track(index)?;
            } else {
                self.connect_state.set_current_track_random()?;
            }

            if self.context_resolver.has_next() {
                self.connect_state.update_queue_revision()
            } else {
                self.connect_state.shuffle_new()?;
                self.add_autoplay_resolving_when_required();
            }
        } else {
            self.connect_state
                .set_current_track(index.unwrap_or_default())?;
            self.connect_state.reset_playback_to_position(index)?;
            self.add_autoplay_resolving_when_required();
        }

        if self.connect_state.current_track(MessageField::is_some) {
            self.load_track(cmd_options.start_playing, cmd_options.seek_to)?;
            self.bootstrap_queue_ready = true;
        } else {
            info!("No active track, stopping");
            self.handle_stop()
        }

        Ok(())
    }

    async fn load_context_from_uri(
        &mut self,
        context_uri: String,
        page: Option<&ContextPage>,
        autoplay: bool,
    ) -> Result<(), Error> {
        let update_context = if autoplay {
            ContextType::Autoplay
        } else {
            ContextType::Default
        };

        self.connect_state.set_active_context(update_context);

        let fallback = match page {
            // check that the uri is valid or the page has a valid uri that can be used
            Some(page) => match ConnectState::find_valid_uri(Some(&context_uri), Some(page)) {
                Some(ctx_uri) => ctx_uri,
                None => return Err(SpircError::InvalidUri(context_uri).into()),
            },
            // when there is no page, the uri should be valid
            None => &context_uri,
        };

        let current_context_uri = self.connect_state.context_uri();

        if current_context_uri == &context_uri && fallback == context_uri {
            debug!("context <{current_context_uri}> didn't change, no resolving required")
        } else {
            debug!("resolving context for load command");
            self.context_resolver.clear();
            self.context_resolver.add(ResolveContext::from_uri(
                &context_uri,
                fallback,
                update_context,
                ContextAction::Replace,
            ));
            let deadline = self.restoration_request_deadline();
            #[cfg(test)]
            let stall_load = std::mem::take(&mut self.stall_context_load);
            let resolution = async {
                #[cfg(test)]
                if stall_load {
                    std::future::pending::<()>().await;
                }
                Ok(self.context_resolver.get_next_context(Vec::new).await)
            };
            let context = Self::await_restoration_request(deadline, resolution).await?;
            self.handle_next_context(context);
        }

        Ok(())
    }

    fn load_context_from_tracks(&mut self, tracks: impl Into<ContextPage>) -> Result<(), Error> {
        const WEB_API_URI: &str = "spotify:web-api";
        let ctx = Context {
            // by providing values for uri/url the player in the official client's isn't frozen
            uri: Some(WEB_API_URI.into()),
            url: Some(format!("context://{WEB_API_URI}")),
            pages: vec![tracks.into()],
            ..Default::default()
        };

        let _ = self
            .connect_state
            .update_context(ctx, ContextType::Default)?;

        self.emit_set_queue_event();

        Ok(())
    }

    fn handle_play(&mut self) {
        match self.play_status {
            SpircPlayStatus::Paused {
                position_ms,
                preloading_of_next_track_triggered,
            } => {
                self.player.play();
                self.connect_state
                    .update_position(position_ms, self.now_ms());
                self.play_status = SpircPlayStatus::Playing {
                    nominal_start_time: self.now_ms() - position_ms as i64,
                    preloading_of_next_track_triggered,
                };
            }
            SpircPlayStatus::LoadingPause { position_ms } => {
                self.player.play();
                self.play_status = SpircPlayStatus::LoadingPlay { position_ms };
            }
            _ => return,
        }

        // Synchronize the volume from the mixer. This is useful on
        // systems that can switch sources from and back to librespot.
        let current_volume = self.mixer.volume();
        self.set_volume(current_volume);
    }

    fn handle_play_pause(&mut self) {
        match self.play_status {
            SpircPlayStatus::Paused { .. } | SpircPlayStatus::LoadingPause { .. } => {
                self.handle_play()
            }
            SpircPlayStatus::Playing { .. } | SpircPlayStatus::LoadingPlay { .. } => {
                self.handle_pause()
            }
            _ => (),
        }
    }

    fn handle_pause(&mut self) {
        match self.play_status {
            SpircPlayStatus::Playing {
                nominal_start_time,
                preloading_of_next_track_triggered,
            } => {
                self.player.pause();
                let position_ms = (self.now_ms() - nominal_start_time) as u32;
                self.connect_state
                    .update_position(position_ms, self.now_ms());
                self.play_status = SpircPlayStatus::Paused {
                    position_ms,
                    preloading_of_next_track_triggered,
                };
            }
            SpircPlayStatus::LoadingPlay { position_ms } => {
                self.player.pause();
                self.play_status = SpircPlayStatus::LoadingPause { position_ms };
            }
            _ => (),
        }
    }

    fn handle_seek(&mut self, position_ms: u32) {
        let duration = self.connect_state.player().duration;
        if i64::from(position_ms) > duration {
            warn!("tried to seek to {position_ms}ms of {duration}ms");
            return;
        }

        self.connect_state
            .update_position(position_ms, self.now_ms());
        self.player.seek(position_ms);
        let now = self.now_ms();
        match self.play_status {
            SpircPlayStatus::Stopped => (),
            SpircPlayStatus::LoadingPause {
                position_ms: ref mut position,
            }
            | SpircPlayStatus::LoadingPlay {
                position_ms: ref mut position,
            }
            | SpircPlayStatus::Paused {
                position_ms: ref mut position,
                ..
            } => *position = position_ms,
            SpircPlayStatus::Playing {
                ref mut nominal_start_time,
                ..
            } => *nominal_start_time = now - position_ms as i64,
        };
    }

    fn handle_shuffle(&mut self, shuffle: bool) -> Result<(), Error> {
        self.connect_state.queue_replaced = true;
        self.player.emit_shuffle_changed_event(shuffle);
        self.connect_state.handle_shuffle(shuffle)
    }

    fn handle_repeat_context(&mut self, repeat: bool) -> Result<(), Error> {
        self.player
            .emit_repeat_changed_event(repeat, self.connect_state.repeat_track());
        self.connect_state.handle_set_repeat_context(repeat)
    }

    fn handle_repeat_track(&mut self, repeat: bool) {
        self.player
            .emit_repeat_changed_event(self.connect_state.repeat_context(), repeat);
        self.connect_state.set_repeat_track(repeat);
    }

    async fn handle_add_to_queue(&mut self, uri: SpotifyUri) {
        let track_uris: Vec<String> = match uri {
            SpotifyUri::Track { .. } | SpotifyUri::Episode { .. } => vec![uri.to_uri()],
            SpotifyUri::Album { .. } | SpotifyUri::Playlist { .. } => {
                match Self::await_restoration_request(
                    self.restoration_request_deadline(),
                    self.session.spclient().get_context(&uri.to_uri()),
                )
                .await
                {
                    Ok(context) => context
                        .pages
                        .iter()
                        .flat_map(|page| page.tracks.iter())
                        .filter_map(|track| track.uri.clone())
                        .collect(),
                    Err(e) => {
                        error!("failed to resolve context for {}: {e}", uri.item_type());
                        return;
                    }
                }
            }
            _ => return,
        };

        for track_uri in track_uris {
            let track = ProvidedTrack {
                uri: track_uri.clone(),
                ..Default::default()
            };
            self.connect_state.add_to_queue(track, true);
        }
        self.emit_set_queue_event();
    }

    fn handle_preload_next_track(&mut self) {
        // Requests the player thread to preload the next track
        match self.play_status {
            SpircPlayStatus::Paused {
                ref mut preloading_of_next_track_triggered,
                ..
            }
            | SpircPlayStatus::Playing {
                ref mut preloading_of_next_track_triggered,
                ..
            } => {
                *preloading_of_next_track_triggered = true;
            }
            _ => (),
        }

        if let Some(track_id) = self.connect_state.preview_next_track() {
            self.player.preload(track_id);
        }
    }

    // Mark unavailable tracks so we can skip them later
    fn handle_unavailable(&mut self, track_id: &SpotifyUri) -> Result<(), Error> {
        self.connect_state.mark_unavailable(track_id)?;
        self.handle_preload_next_track();

        Ok(())
    }

    fn add_autoplay_resolving_when_required(&mut self) {
        let require_load_new = !self
            .connect_state
            .has_next_tracks(Some(CONTEXT_FETCH_THRESHOLD))
            && self.session.autoplay()
            && !self.connect_state.context_uri().is_empty();

        if !require_load_new {
            return;
        }

        let current_context = self.connect_state.context_uri();
        let fallback = self.connect_state.current_track(|t| &t.uri);

        let has_tracks = self
            .connect_state
            .get_context(ContextType::Autoplay)
            .map(|c| !c.tracks.is_empty())
            .unwrap_or_default();

        let resolve = ResolveContext::from_uri(
            current_context,
            fallback,
            ContextType::Autoplay,
            if has_tracks {
                ContextAction::Append
            } else {
                ContextAction::Replace
            },
        );

        self.context_resolver.add(resolve);
    }

    fn handle_next(&mut self, track_uri: Option<String>) -> Result<(), Error> {
        self.connect_state.preserve_recovery_queue = false;
        let continue_playing = self.connect_state.is_playing();

        let current_uri = self.connect_state.current_track(|t| &t.uri);
        let mut has_next_track =
            matches!(track_uri, Some(ref track_uri) if current_uri == track_uri);

        if !has_next_track {
            has_next_track = loop {
                let index = self.connect_state.next_track()?;

                let current_uri = self.connect_state.current_track(|t| &t.uri);
                if matches!(track_uri, Some(ref track_uri) if current_uri != track_uri) {
                    continue;
                } else {
                    break index.is_some();
                }
            };
        };

        if has_next_track {
            self.add_autoplay_resolving_when_required();
            self.load_track(continue_playing, 0)
        } else {
            info!("Not playing next track because there are no more tracks left in queue.");
            self.handle_stop();
            Ok(())
        }
    }

    fn handle_prev(&mut self) -> Result<(), Error> {
        self.connect_state.preserve_recovery_queue = false;
        // Previous behaves differently based on the position
        // Under 3s it goes to the previous song (starts playing)
        // Over 3s it seeks to zero (retains previous play status)
        if self.position() < 3000 {
            let repeat_context = self.connect_state.repeat_context();
            match self.connect_state.prev_track()? {
                None if repeat_context => self.connect_state.reset_playback_to_position(None)?,
                None => {
                    self.connect_state.reset_playback_to_position(None)?;
                    self.handle_stop()
                }
                Some(_) => self.load_track(self.connect_state.is_playing(), 0)?,
            }
        } else {
            self.handle_seek(0);
        }

        Ok(())
    }

    fn handle_volume_up(&mut self) {
        let volume = (self.connect_state.device_info().volume as u16)
            .saturating_add(self.connect_state.volume_step_size);

        self.set_volume(volume);
    }

    fn handle_volume_down(&mut self) {
        let volume = (self.connect_state.device_info().volume as u16)
            .saturating_sub(self.connect_state.volume_step_size);

        self.set_volume(volume);
    }

    fn handle_playlist_modification(
        &mut self,
        playlist_modification_info: PlaylistModificationInfo,
    ) -> Result<(), Error> {
        let uri = playlist_modification_info
            .uri
            .ok_or(SpircError::NoUri("playlist modification"))?;
        let uri = String::from_utf8(uri)?;

        if self.connect_state.context_uri() != &uri {
            debug!(
                "ignoring playlist modification update for playlist <{uri}>, because it isn't the current context"
            );
            return Ok(());
        }

        debug!("playlist modification for current context: {uri}");
        self.context_resolver.add(ResolveContext::from_uri(
            uri,
            self.connect_state.current_track(|t| &t.uri),
            ContextType::Default,
            ContextAction::Replace,
        ));

        Ok(())
    }

    fn handle_session_update(&mut self, session_update: FallbackWrapper<SessionUpdate>) {
        // we know that this enum value isn't present in our current proto definitions, by that
        // the json parsing fails because the enum isn't known as proto representation
        const WBC: &str = "WIFI_BROADCAST_CHANGED";

        let mut session_update = match session_update {
            FallbackWrapper::Inner(update) => update,
            FallbackWrapper::Fallback(value) => {
                let fallback_inner = value.to_string();
                if fallback_inner.contains(WBC) {
                    log::debug!("Received SessionUpdate::{WBC}");
                } else {
                    log::warn!("SessionUpdate couldn't be parse correctly: {value:?}");
                }
                return;
            }
        };

        let reason = session_update.reason.enum_value();

        let mut session = match session_update.session.take() {
            None => return,
            Some(session) => session,
        };

        let active_device = session.host_active_device_id.take();
        if matches!(active_device, Some(ref device) if device == self.session.device_id()) {
            info!(
                "session update: <{:?}> for self, current session_id {}, new session_id {}",
                reason,
                self.session.session_id(),
                session.session_id
            );

            if self.session.session_id() != session.session_id {
                self.session.set_session_id(&session.session_id);
                self.connect_state.set_session_id(session.session_id);
            }
        } else {
            debug!("session update: <{reason:?}> from active session host: <{active_device:?}>");
        }

        // this seems to be used for jams or handling the current session_id
        //
        // handling this event was intended to keep the playback when other clients (primarily
        // mobile) connects, otherwise they would steel the current playback when there was no
        // session_id provided on the initial PutStateReason::NEW_DEVICE state update
        //
        // by generating an initial session_id from the get-go we prevent that behavior and
        // currently don't need to handle this event, might still be useful for later "jam" support
    }

    fn position(&mut self) -> u32 {
        match self.play_status {
            SpircPlayStatus::Stopped => 0,
            SpircPlayStatus::LoadingPlay { position_ms }
            | SpircPlayStatus::LoadingPause { position_ms }
            | SpircPlayStatus::Paused { position_ms, .. } => position_ms,
            SpircPlayStatus::Playing {
                nominal_start_time, ..
            } => (self.now_ms() - nominal_start_time) as u32,
        }
    }

    fn load_track(&mut self, start_playing: bool, position_ms: u32) -> Result<(), Error> {
        if self.connect_state.current_track(MessageField::is_none) {
            debug!("current track is none, stopping playback");
            self.handle_stop();
            return Ok(());
        }

        let current_uri = self.connect_state.current_track(|t| &t.uri);
        let id = SpotifyUri::from_uri(current_uri)?;
        self.restoration_aborted = false;
        self.player.load(id, start_playing, position_ms);
        self.record_loading(start_playing, position_ms);
        Ok(())
    }

    fn record_loading(&mut self, start_playing: bool, position_ms: u32) {
        self.connect_state
            .update_position(position_ms, self.now_ms());
        if start_playing {
            self.play_status = SpircPlayStatus::LoadingPlay { position_ms };
        } else {
            self.play_status = SpircPlayStatus::LoadingPause { position_ms };
        }
        self.connect_state.set_status(&self.play_status);
    }

    fn observe_publication_reply(&mut self, bytes: &[u8]) {
        use protobuf::Message as _;
        let Ok(cluster) = Cluster::parse_from_bytes(bytes) else {
            return;
        };
        let owner = if cluster.active_device_id.is_empty() {
            "empty"
        } else if cluster.active_device_id == self.session.device_id() {
            "self"
        } else {
            "other"
        };
        debug!(target: "bopify_core::ownership", "ownership: publication_reply spirc={} owner={owner} changed_ms={} server_ms={}", self.spirc_id, cluster.changed_timestamp_ms, cluster.server_timestamp_ms);
        // A foreign reply must not hide a subsequent update that relinquishes ownership.
        if owner == "self" && cluster.changed_timestamp_ms > 0 {
            self.last_cluster_timestamp = self
                .last_cluster_timestamp
                .max(Some(cluster.changed_timestamp_ms));
        }
    }

    async fn notify(&mut self) -> Result<(), Error> {
        if self.restoration_aborted {
            return tokio::time::timeout(
                Duration::from_secs(2),
                self.session.spclient().put_connect_state_inactive(false),
            )
            .await
            .map_err(Error::deadline_exceeded)?
            .map(|_| ());
        }
        self.emit_set_queue_event();
        self.connect_state.set_status(&self.play_status);

        if self.connect_state.is_playing() {
            self.connect_state
                .update_position_in_relation(self.now_ms());
        }

        self.connect_state.set_now(self.now_ms() as u64);

        let deadline = self.restoration_request_deadline();
        debug!(target: "bopify_core::ownership", "ownership: publish spirc={} active={} restoring={}", self.spirc_id, self.connect_state.is_active(), self.restoration_pending());
        #[cfg(test)]
        let stall_publication = std::mem::take(&mut self.stall_state_publication);
        let publication = async {
            #[cfg(test)]
            if stall_publication {
                std::future::pending::<()>().await;
            }
            self.connect_state.send_state(&self.session).await
        };
        let result = Self::await_restoration_request(deadline, publication).await;
        debug!(target: "bopify_core::ownership", "ownership: published spirc={} success={} error_kind={:?}", self.spirc_id, result.is_ok(), result.as_ref().err().map(|error| error.kind));
        if let Ok(bytes) = &result {
            self.observe_publication_reply(bytes);
        }
        result.map(|_| ())
    }

    fn set_volume(&mut self, volume: u16) {
        debug!("SpircTask::set_volume({volume})");

        let old_volume = self.connect_state.device_info().volume;
        let new_volume = volume as u32;
        if old_volume != new_volume || self.mixer.volume() != volume {
            self.update_volume = true;

            self.connect_state.set_volume(new_volume);
            self.mixer.set_volume(volume);
            if let Some(cache) = self.session.cache() {
                cache.save_volume(volume)
            }
            if self.connect_state.is_active() {
                self.player.emit_volume_changed_event(volume);
            }
        }
    }
}

impl Drop for SpircTask {
    fn drop(&mut self) {
        debug!("drop Spirc[{}]", self.spirc_id);
    }
}

#[cfg(test)]
mod recovery_tests {
    use super::*;
    use crate::playback::{
        audio_backend::{Sink, SinkResult},
        config::PlayerConfig,
        convert::Converter,
        decoder::AudioPacket,
        mixer::{MixerConfig, softmixer::SoftMixer},
        player::ConnectionOutcome,
    };
    struct SilentSink;
    impl Sink for SilentSink {
        fn write(&mut self, _: AudioPacket, _: &mut Converter) -> SinkResult<()> {
            Ok(())
        }
    }
    fn fixture(should_resume: bool) -> (SpircTask, PlayerEventChannel) {
        let session = Session::new(crate::core::SessionConfig::default(), None);
        let mixer: Arc<dyn Mixer> = Arc::new(SoftMixer::open(MixerConfig::default()).unwrap());
        let player = Player::new(
            PlayerConfig::default(),
            session.clone(),
            mixer.get_soft_volume(),
            || Box::new(SilentSink),
        );
        let output = player.get_player_event_channel();
        let mut state = ConnectState::new(ConnectConfig::default(), &session);
        state.player_mut().track = MessageField::some(ProvidedTrack {
            uri: "spotify:track:6rqhFgbbKwnb9MLmUQDhG6".into(),
            uid: "occurrence".into(),
            ..Default::default()
        });
        state.player_mut().context_uri = "spotify:playlist:37i9dQZF1E8NDRt5R5oIbG".into();
        let identity = crate::RecoveryIdentity {
            track_uri: state.player().track.uri.clone(),
            track_uid: Some("occurrence".into()),
            context_uri: Some(state.context_uri().clone()),
        };
        let checkpoint = crate::RecoveryCheckpoint {
            was_active: true,
            should_resume,
            identity: Some(identity),
            position_ms: 3200,
            volume: 0.,
            ..Default::default()
        };
        (
            SpircTask {
                queue_tracker: Default::default(),
                observed_repeat: None,
                player,
                mixer,
                connect_state: state,
                connect_established: true,
                play_request_id: Some(1),
                play_status: SpircPlayStatus::LoadingPause { position_ms: 3200 },
                connection_id_update: futures_util::stream::pending().boxed(),
                connect_state_update: futures_util::stream::pending().boxed(),
                connect_state_volume_update: futures_util::stream::pending().boxed(),
                connect_state_logout_request: futures_util::stream::pending().boxed(),
                playlist_update: futures_util::stream::pending().boxed(),
                session_update: futures_util::stream::pending().boxed(),
                connect_state_command: futures_util::stream::pending().boxed(),
                user_attributes_update: futures_util::stream::pending().boxed(),
                user_attributes_mutation: futures_util::stream::pending().boxed(),
                commands: None,
                player_events: None,
                context_resolver: ContextResolver::new(session.clone()),
                emit_set_queue_events: true,
                startup_restore: true,
                startup_mode: crate::StartupMode::Recovery(Some(checkpoint)),
                latest_owner: session.device_id().into(),
                last_cluster_timestamp: None,
                recovery_native: None,
                bootstrap_pending: true,
                bootstrap_queue_ready: false,
                bootstrap_playback_confirmed: false,
                recovery_resume_requested: false,
                restoration_deadline: None,
                restoration_aborted: false,
                stall_state_publication: false,
                stall_new_device_publication: false,
                stall_volume_publication: false,
                stall_context_load: false,
                last_identity: None,
                startup_resolved: true,
                last_remote: None,
                play_here_requested_at: None,
                shutdown: false,
                session,
                transfer_state: None,
                update_volume: false,
                update_state: false,
                spirc_id: 0,
            },
            output,
        )
    }
    fn paused(task: &mut SpircTask) {
        task.handle_player_event(PlayerEvent::Paused {
            play_request_id: 1,
            track_id: SpotifyUri::from_uri("spotify:track:6rqhFgbbKwnb9MLmUQDhG6").unwrap(),
            position_ms: 3200,
        })
        .unwrap();
    }

    fn resolving(task: &mut SpircTask, ty: ContextType) {
        task.connect_state.set_active(true);
        task.context_resolver.add(ResolveContext::from_uri(
            task.connect_state.context_uri().clone(),
            task.connect_state.current_track(|track| track.uri.clone()),
            ty,
            ContextAction::Replace,
        ));
    }

    #[tokio::test]
    async fn stalled_reconnection_publication_emits_unavailable_without_setup_failure() {
        let (mut task, mut output) = fixture(true);
        task.stall_new_device_publication = true;
        task.restoration_deadline = Some(Instant::now() + Duration::from_millis(10));
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            task.handle_connection_id_update("replacement-connection".into()),
        )
        .await
        .unwrap();
        assert!(!task.stall_new_device_publication);
        assert!(
            result.is_ok(),
            "expired restoration must not terminate dealer setup"
        );
        assert!(task.restoration_aborted);
        let outcome = tokio::time::timeout(Duration::from_secs(1), async {
            while let Some(event) = output.recv().await {
                if let PlayerEvent::ConnectionReady { outcome } = event {
                    return outcome;
                }
            }
            panic!("missing terminal restoration outcome");
        })
        .await
        .unwrap();
        assert!(matches!(outcome, ConnectionOutcome::Unavailable));
    }

    #[tokio::test]
    async fn stalled_state_publication_cannot_outlive_restoration_deadline() {
        let (mut task, mut output) = fixture(true);
        task.connect_state.set_active(true);
        task.bootstrap_queue_ready = true;
        task.stall_state_publication = true;
        task.update_state = true;
        task.restoration_deadline = Some(Instant::now() + UPDATE_STATE_DELAY * 2);
        let before = task.connect_state.player().clone();
        let outcome = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::select! {
                _ = task.run_event_loop() => panic!("event loop ended before restoration deadline"),
                outcome = async {
                    while let Some(event) = output.recv().await {
                        if let PlayerEvent::ConnectionReady { outcome } = event { return outcome; }
                    }
                    panic!("missing terminal restoration outcome")
                } => outcome,
            }
        })
        .await;
        assert!(
            !task.stall_state_publication,
            "fixture must enter the state-publication branch"
        );
        assert!(matches!(outcome, Ok(ConnectionOutcome::Unavailable)));
        assert!(task.restoration_aborted);
        assert!(!task.connect_state.is_active());
        assert_eq!(task.connect_state.player().track, before.track);
        assert_eq!(task.connect_state.player().next_tracks, before.next_tracks);
    }

    #[tokio::test]
    async fn stalled_volume_publication_cannot_outlive_restoration_deadline() {
        let (mut task, mut output) = fixture(true);
        task.connect_state.set_active(true);
        task.bootstrap_queue_ready = true;
        task.stall_volume_publication = true;
        task.update_volume = true;
        task.restoration_deadline = Some(Instant::now() + VOLUME_UPDATE_DELAY * 2);
        let before = task.connect_state.player().clone();
        let outcome = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::select! {
                _ = task.run_event_loop() => panic!("event loop ended before restoration deadline"),
                outcome = async {
                    while let Some(event) = output.recv().await {
                        if let PlayerEvent::ConnectionReady { outcome } = event { return outcome; }
                    }
                    panic!("missing terminal restoration outcome")
                } => outcome,
            }
        })
        .await;
        assert!(!task.stall_volume_publication);
        assert!(matches!(outcome, Ok(ConnectionOutcome::Unavailable)));
        assert!(task.restoration_aborted);
        assert!(!task.connect_state.is_active());
        assert_eq!(task.connect_state.player().track, before.track);
        assert_eq!(task.connect_state.player().next_tracks, before.next_tracks);
    }

    #[tokio::test]
    async fn stalled_load_context_cannot_outlive_restoration_deadline() {
        let (mut task, _) = fixture(true);
        task.stall_context_load = true;
        task.restoration_deadline = Some(Instant::now() + Duration::from_millis(10));
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            task.load_context_from_uri("spotify:playlist:new".into(), None, false),
        )
        .await;
        assert!(!task.stall_context_load);
        assert!(
            matches!(result, Ok(Err(error)) if error.kind == crate::core::error::ErrorKind::DeadlineExceeded)
        );
        assert!(!task.bootstrap_queue_ready);
    }

    #[tokio::test]
    async fn playback_confirmation_after_deadline_cannot_finish_bootstrap() {
        let (mut task, mut output) = fixture(true);
        task.bootstrap_queue_ready = true;
        task.bootstrap_playback_confirmed = true;
        task.restoration_deadline = Some(Instant::now() - Duration::from_millis(1));
        task.maybe_finish_bootstrap();
        assert!(task.restoration_aborted);
        let outcome = tokio::time::timeout(Duration::from_secs(1), async {
            while let Some(event) = output.recv().await {
                if let PlayerEvent::ConnectionReady { outcome } = event {
                    return outcome;
                }
            }
            panic!("missing terminal restoration outcome");
        })
        .await
        .unwrap();
        assert!(matches!(outcome, ConnectionOutcome::Unavailable));
    }

    #[tokio::test]
    async fn deadline_covers_queue_ready_and_pending_resume_confirmation() {
        for resume_requested in [false, true] {
            let (mut task, mut output) = fixture(true);
            task.connect_state.set_active(true);
            task.bootstrap_queue_ready = true;
            task.bootstrap_playback_confirmed = resume_requested;
            task.recovery_resume_requested = resume_requested;
            task.restoration_deadline = Some(Instant::now() + Duration::from_millis(10));
            let before = task.connect_state.player().clone();
            let outcome = tokio::time::timeout(Duration::from_secs(1), async {
                tokio::select! {
                    _ = task.run_event_loop() => panic!("event loop ended before restoration deadline"),
                    outcome = async {
                        while let Some(event) = output.recv().await {
                            if let PlayerEvent::ConnectionReady { outcome } = event { return outcome; }
                        }
                        panic!("missing terminal restoration outcome")
                    } => outcome,
                }
            }).await.unwrap();
            assert!(matches!(outcome, ConnectionOutcome::Unavailable));
            assert!(task.restoration_aborted);
            assert!(!task.connect_state.is_active());
            assert!(!task.bootstrap_pending);
            assert!(matches!(task.play_status, SpircPlayStatus::Stopped));
            assert_eq!(task.connect_state.player().track, before.track);
            assert_eq!(task.connect_state.player().next_tracks, before.next_tracks);
            assert_eq!(
                task.connect_state.player().position_as_of_timestamp,
                before.position_as_of_timestamp
            );
        }
    }

    #[tokio::test]
    async fn failed_explicit_uri_load_leaves_device_inactive() {
        let (mut task, _) = fixture(true);
        task.connect_state.set_active(false);
        let result = task
            .handle_command(SpircCommand::Load(LoadRequest {
                context: PlayContext::Uri(String::new()),
                options: LoadRequestOptions::default(),
            }))
            .await;
        assert!(result.is_err());
        assert!(!task.connect_state.is_active());
    }

    #[tokio::test]
    async fn explicit_load_dispatch_reactivates_an_inactive_device() {
        for after_abort in [false, true] {
            let (mut task, _) = fixture(true);
            if after_abort {
                task.end_restoration(ConnectionOutcome::Unavailable);
            }
            task.connect_state.set_active(false);
            // The unconnected fixture cannot publish; inspect the dispatch before that boundary.
            let _ = task
                .handle_command(SpircCommand::Load(LoadRequest {
                    context: PlayContext::Tracks(vec![
                        "spotify:track:3n3Ppam7vgaVa1iaRUc9Lp".into(),
                    ]),
                    options: LoadRequestOptions::default(),
                }))
                .await;
            assert!(!task.restoration_aborted);
            assert!(task.connect_state.is_active());
            assert_eq!(
                task.connect_state.current_track(|track| &track.uri),
                "spotify:track:3n3Ppam7vgaVa1iaRUc9Lp"
            );
            assert!(task.bootstrap_queue_ready);
            assert!(matches!(
                task.play_status,
                SpircPlayStatus::LoadingPause { .. }
            ));
        }
    }

    #[tokio::test]
    async fn new_load_accepts_context_after_failed_restoration() {
        let (mut task, _) = fixture(true);
        task.end_restoration(ConnectionOutcome::Unavailable);
        // An empty load fails before load_track, exposing whether its boundary resets the guard.
        assert!(
            task.handle_load(
                LoadRequest {
                    context: PlayContext::Tracks(vec![]),
                    options: LoadRequestOptions::default(),
                },
                None,
                None
            )
            .await
            .is_err()
        );
        assert!(!task.restoration_aborted);
        task.context_resolver.add(ResolveContext::from_uri(
            "spotify:playlist:new",
            "spotify:track:6rqhFgbbKwnb9MLmUQDhG6",
            ContextType::Default,
            ContextAction::Replace,
        ));
        assert!(task.handle_next_context(Ok(Context {
            uri: Some("spotify:playlist:new".into()),
            pages: vec![ContextPage {
                tracks: vec![crate::protocol::context_track::ContextTrack {
                    uri: Some("spotify:track:6rqhFgbbKwnb9MLmUQDhG6".into()),
                    uid: Some("new-occurrence".into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })));
        assert_eq!(task.connect_state.context_uri(), "spotify:playlist:new");
    }

    #[tokio::test]
    async fn new_load_clears_old_confirmation_and_resume_intent() {
        let (mut task, _) = fixture(true);
        task.bootstrap_queue_ready = true;
        task.bootstrap_playback_confirmed = true;
        task.recovery_resume_requested = true;
        assert!(
            task.handle_load(
                LoadRequest {
                    context: PlayContext::Tracks(vec![]),
                    options: LoadRequestOptions::default(),
                },
                None,
                None
            )
            .await
            .is_err()
        );
        assert!(!task.bootstrap_queue_ready);
        assert!(!task.bootstrap_playback_confirmed);
        assert!(!task.recovery_resume_requested);
        assert!(matches!(
            task.startup_mode,
            crate::StartupMode::Recovery(None)
        ));
    }

    #[tokio::test]
    async fn explicit_paused_load_does_not_reuse_old_resume_checkpoint() {
        let (mut task, _) = fixture(true);
        task.handle_load(
            LoadRequest {
                context: PlayContext::Tracks(vec!["spotify:track:6rqhFgbbKwnb9MLmUQDhG6".into()]),
                options: LoadRequestOptions::default(),
            },
            None,
            None,
        )
        .await
        .unwrap();
        assert!(task.bootstrap_queue_ready);
        task.play_request_id = Some(1);
        paused(&mut task);
        assert!(!task.bootstrap_pending);
        assert!(!task.recovery_resume_requested);
        assert!(matches!(task.play_status, SpircPlayStatus::Paused { .. }));
    }

    #[tokio::test]
    async fn required_empty_context_retries_once_then_preserves_state_on_failure() {
        let (mut task, mut output) = fixture(true);
        resolving(&mut task, ContextType::Default);
        task.connect_state
            .player_mut()
            .next_tracks
            .push(ProvidedTrack {
                uri: "spotify:track:6rqhFgbbKwnb9MLmUQDhG6".into(),
                uid: "q7".into(),
                provider: "queue".into(),
                ..Default::default()
            });
        let before = task.connect_state.player().clone();
        let no_data = || Error::from(crate::core::spclient::SpClientError::NoData);
        assert!(!task.handle_next_context(Err(no_data())));
        assert!(task.context_resolver.has_next());
        assert!(task.bootstrap_pending);
        assert!(task.handle_next_context(Err(no_data())));
        assert!(!task.bootstrap_pending);
        assert!(!task.connect_state.is_active());
        assert_eq!(task.connect_state.player().track, before.track);
        assert_eq!(task.connect_state.player().next_tracks, before.next_tracks);
        assert_eq!(task.connect_state.player().options, before.options);
        assert_eq!(
            task.connect_state.player().position_as_of_timestamp,
            before.position_as_of_timestamp
        );
        let outcome = tokio::time::timeout(Duration::from_secs(1), async {
            while let Some(event) = output.recv().await {
                if let PlayerEvent::ConnectionReady { outcome } = event {
                    return outcome;
                }
            }
            panic!("missing terminal restoration outcome")
        })
        .await
        .unwrap();
        assert!(matches!(outcome, ConnectionOutcome::Unavailable));
    }

    #[tokio::test]
    async fn invalid_required_context_aborts_and_late_playback_cannot_resume() {
        let (mut task, _) = fixture(true);
        resolving(&mut task, ContextType::Default);
        task.handle_next_context(Err(Error::invalid_argument("invalid context")));
        assert!(!task.bootstrap_pending);
        task.handle_player_event(PlayerEvent::PlayRequestIdChanged { play_request_id: 2 })
            .unwrap();
        task.handle_player_event(PlayerEvent::Playing {
            play_request_id: 2,
            track_id: SpotifyUri::from_uri("spotify:track:6rqhFgbbKwnb9MLmUQDhG6").unwrap(),
            position_ms: 8000,
        })
        .unwrap();
        assert!(!task.connect_state.is_active());
        assert!(!task.bootstrap_playback_confirmed);
        assert!(!task.recovery_resume_requested);
        assert!(matches!(task.play_status, SpircPlayStatus::Stopped));
        assert!(!task.connect_state.player().is_playing);
        assert!(!task.connect_state.player().is_buffering);
    }

    #[tokio::test]
    async fn publication_watermark_requires_a_versioned_local_reply_and_never_regresses() {
        use protobuf::Message as _;
        let (mut task, _) = fixture(true);
        task.observe_publication_reply(&[0xff]);
        task.observe_publication_reply(&[]);
        assert_eq!(task.last_cluster_timestamp, None);
        for (owner, timestamp, expected) in [
            (task.session.device_id().to_owned(), 0, None),
            ("foreign".into(), 200, None),
            (task.session.device_id().to_owned(), 100, Some(100)),
            (task.session.device_id().to_owned(), 99, Some(100)),
            (task.session.device_id().to_owned(), 101, Some(101)),
            ("foreign".into(), 300, Some(101)),
            (String::new(), 400, Some(101)),
        ] {
            let bytes = Cluster {
                active_device_id: owner,
                changed_timestamp_ms: timestamp,
                ..Default::default()
            }
            .write_to_bytes()
            .unwrap();
            task.observe_publication_reply(&bytes);
            assert_eq!(task.last_cluster_timestamp, expected);
        }
        task.session.set_connection_id("\n");
        assert!(task.notify().await.is_err());
        assert_eq!(task.last_cluster_timestamp, Some(101));
    }

    #[tokio::test]
    async fn genuine_takeover_stops_local_playback_even_when_publication_fails() {
        let (mut task, _) = fixture(false);
        task.bootstrap_pending = false;
        resolving(&mut task, ContextType::Default);
        task.session.set_connection_id("\n");
        task.play_status = SpircPlayStatus::Playing {
            nominal_start_time: task.now_ms() - 3200,
            preloading_of_next_track_triggered: false,
        };
        let result = task
            .handle_cluster_update(ClusterUpdate {
                cluster: MessageField::some(Cluster {
                    active_device_id: "foreign".into(),
                    changed_timestamp_ms: 101,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await;
        assert!(result.is_err());
        assert!(!task.connect_state.is_active());
        assert!(matches!(task.play_status, SpircPlayStatus::Stopped));
        assert!(!task.context_resolver.has_next());
        let before = task.connect_state.player().clone();
        task.handle_player_event(PlayerEvent::PlayRequestIdChanged { play_request_id: 2 })
            .unwrap();
        assert_eq!(task.play_request_id, None);
        task.handle_player_event(PlayerEvent::EndOfTrack {
            play_request_id: 2,
            track_id: SpotifyUri::from_uri(&before.track.uri).unwrap(),
        })
        .unwrap();
        assert_eq!(task.connect_state.player(), &before);
        assert!(matches!(task.play_status, SpircPlayStatus::Stopped));
    }

    #[tokio::test]
    async fn accepted_dealer_snapshot_advances_the_watermark() {
        let (mut task, _) = fixture(false);
        for timestamp in [100, 0, 101, 99] {
            task.handle_cluster_update(ClusterUpdate {
                cluster: MessageField::some(Cluster {
                    active_device_id: task.session.device_id().into(),
                    changed_timestamp_ms: timestamp,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await
            .unwrap();
        }
        assert_eq!(task.last_cluster_timestamp, Some(101));
    }

    #[tokio::test]
    async fn older_peer_snapshot_cannot_abort_confirmed_local_transfer() {
        let (mut task, _) = fixture(true);
        resolving(&mut task, ContextType::Default);
        task.session.set_connection_id("\n");
        task.last_cluster_timestamp = Some(100);
        task.handle_cluster_update(ClusterUpdate {
            cluster: MessageField::some(Cluster {
                active_device_id: "foreign".into(),
                changed_timestamp_ms: 99,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();
        assert!(task.connect_state.is_active());
        assert!(task.bootstrap_pending);
        assert!(task.context_resolver.has_next());
        assert_eq!(task.latest_owner, task.session.device_id());
    }

    #[tokio::test]
    async fn stale_snapshot_preserves_the_absolute_restoration_deadline() {
        let (mut task, _) = fixture(true);
        resolving(&mut task, ContextType::Default);
        let deadline = Instant::now() + Duration::from_secs(1);
        task.restoration_deadline = Some(deadline);
        task.last_cluster_timestamp = Some(100);
        task.handle_cluster_update(ClusterUpdate {
            cluster: MessageField::some(Cluster {
                active_device_id: "foreign".into(),
                changed_timestamp_ms: 99,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .unwrap();
        assert_eq!(task.restoration_deadline, Some(deadline));
        tokio::time::sleep_until(deadline).await;
        assert!(task.expire_restoration());
        assert!(!task.bootstrap_pending);
        assert!(!task.connect_state.is_active());
    }

    #[tokio::test]
    async fn empty_owner_snapshots_obey_freshness_without_hiding_connection_loss() {
        for timestamp in [99, 100, 101, 0, -1] {
            let (mut task, mut output) = fixture(true);
            resolving(&mut task, ContextType::Default);
            task.session.set_connection_id("\n");
            task.last_cluster_timestamp = Some(100);
            let result = task
                .handle_cluster_update(ClusterUpdate {
                    cluster: MessageField::some(Cluster {
                        changed_timestamp_ms: timestamp,
                        ..Default::default()
                    }),
                    ..Default::default()
                })
                .await;
            if timestamp == 99 {
                result.unwrap();
                assert_eq!(task.latest_owner, task.session.device_id());
                assert!(task.connect_state.is_active());
                assert!(task.context_resolver.has_next());
            } else {
                assert!(result.is_err());
                assert!(task.latest_owner.is_empty());
                assert!(!task.context_resolver.has_next());
                tokio::time::timeout(Duration::from_secs(1), async {
                    while let Some(event) = output.recv().await {
                        if let PlayerEvent::SessionDisconnected {
                            connection_lost, ..
                        } = event
                        {
                            assert!(connection_lost);
                            return;
                        }
                    }
                    panic!("missing connection-loss event")
                })
                .await
                .unwrap();
            }
        }
    }

    #[tokio::test]
    async fn equal_newer_or_unversioned_peer_snapshot_still_ends_restoration() {
        for timestamp in [100, 101, 0, -1] {
            let (mut task, mut output) = fixture(true);
            resolving(&mut task, ContextType::Default);
            task.session.set_connection_id("\n");
            task.last_cluster_timestamp = Some(100);
            let _ = task
                .handle_cluster_update(ClusterUpdate {
                    cluster: MessageField::some(Cluster {
                        active_device_id: "foreign".into(),
                        changed_timestamp_ms: timestamp,
                        ..Default::default()
                    }),
                    ..Default::default()
                })
                .await;
            assert!(!task.connect_state.is_active());
            assert!(!task.bootstrap_pending);
            assert!(!task.context_resolver.has_next());
            assert_eq!(task.latest_owner, "foreign");
            tokio::time::timeout(Duration::from_secs(1), async {
                while let Some(event) = output.recv().await {
                    if let PlayerEvent::ConnectionReady { outcome } = event {
                        assert!(matches!(outcome, ConnectionOutcome::Remote));
                        return;
                    }
                }
                panic!("missing remote restoration outcome")
            })
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn foreign_owner_during_retry_stops_before_remote_ready() {
        let (mut task, mut output) = fixture(true);
        resolving(&mut task, ContextType::Default);
        task.handle_next_context(Err(Error::from(
            crate::core::spclient::SpClientError::NoData,
        )));
        task.session.set_connection_id("\n");
        let cluster = Cluster {
            active_device_id: "foreign".into(),
            ..Default::default()
        };
        let _ = task
            .handle_cluster_update(ClusterUpdate {
                cluster: MessageField::some(cluster),
                ..Default::default()
            })
            .await;
        assert!(!task.context_resolver.has_next());
        assert!(!task.connect_state.is_active());
        assert!(matches!(task.play_status, SpircPlayStatus::Stopped));
        task.handle_player_event(PlayerEvent::PlayRequestIdChanged { play_request_id: 2 })
            .unwrap();
        assert_eq!(task.play_request_id, None);
        tokio::time::timeout(Duration::from_secs(1), async {
            while let Some(event) = output.recv().await {
                if let PlayerEvent::ConnectionReady { outcome } = event {
                    assert!(matches!(outcome, ConnectionOutcome::Remote));
                    return;
                }
            }
            panic!("missing remote restoration outcome")
        })
        .await
        .unwrap();
        drop(task);
        while let Some(event) = output.recv().await {
            assert!(!matches!(event, PlayerEvent::ConnectionReady { .. }));
        }
    }

    #[tokio::test]
    async fn required_retry_success_finishes_without_waiting_for_optional_page() {
        use crate::protocol::context_track::ContextTrack;
        let (mut task, _) = fixture(false);
        resolving(&mut task, ContextType::Default);
        let mut transfer = TransferState::default();
        transfer.playback.mut_or_insert_default().current_track =
            MessageField::some(ContextTrack {
                uri: Some(task.connect_state.player().track.uri.clone()),
                uid: Some("occurrence".into()),
                ..Default::default()
            });
        transfer
            .current_session
            .mut_or_insert_default()
            .context
            .mut_or_insert_default()
            .uri = Some(task.connect_state.context_uri().clone());
        task.transfer_state = Some(transfer);
        task.handle_next_context(Err(Error::from(
            crate::core::spclient::SpClientError::NoData,
        )));
        let context = Context {
            uri: Some(task.connect_state.context_uri().clone()),
            pages: vec![
                ContextPage {
                    tracks: vec![ContextTrack {
                        uri: Some(task.connect_state.player().track.uri.clone()),
                        uid: Some("occurrence".into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                ContextPage {
                    page_url: Some(
                        "hm://artistplaycontext/v1/page/spotify/album/fixture/page".into(),
                    ),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        assert!(task.handle_next_context(Ok(context)));
        assert!(task.transfer_state.is_none());
        assert!(task.bootstrap_queue_ready);
        paused(&mut task);
        assert!(!task.bootstrap_pending);
        let before = task.connect_state.player().clone();
        task.handle_next_context(Err(Error::from(
            crate::core::spclient::SpClientError::NoData,
        )));
        assert_eq!(task.connect_state.player(), &before);
        assert!(task.connect_state.is_active());
    }

    #[tokio::test]
    async fn empty_context_application_cannot_report_queue_ready() {
        let (mut task, _) = fixture(false);
        resolving(&mut task, ContextType::Default);
        task.handle_next_context(Ok(Context::default()));
        assert!(task.context_resolver.has_next());
        assert!(!task.bootstrap_queue_ready);
        task.handle_next_context(Ok(Context::default()));
        assert!(!task.bootstrap_pending);
        assert!(!task.bootstrap_queue_ready);
    }

    #[tokio::test]
    async fn optional_autoplay_failure_preserves_confirmed_queue() {
        let (mut task, _) = fixture(false);
        task.bootstrap_pending = false;
        resolving(&mut task, ContextType::Autoplay);
        let before = task.connect_state.player().clone();
        task.handle_next_context(Err(Error::from(
            crate::core::spclient::SpClientError::NoData,
        )));
        assert!(task.connect_state.is_active());
        assert_eq!(task.connect_state.player(), &before);
        assert!(!task.context_resolver.has_next());
    }
    #[tokio::test]
    async fn connection_loss_disconnect_notifies_before_failed_cleanup() {
        let (mut task, mut output) = fixture(true);
        // An invalid cleanup header fails before HTTP, keeping this boundary fixture offline.
        task.session.set_connection_id("\n");
        assert!(task.handle_disconnect(true, true).await.is_err());
        let lost = tokio::time::timeout(Duration::from_secs(1), async {
            while let Some(event) = output.recv().await {
                if let PlayerEvent::SessionDisconnected {
                    connection_lost, ..
                } = event
                {
                    return connection_lost;
                }
            }
            false
        })
        .await;
        assert_eq!(lost, Ok(true));
    }

    #[tokio::test]
    async fn ordinary_disconnect_does_not_report_terminal_loss() {
        let (mut task, mut output) = fixture(false);
        task.session.set_connection_id("\n");
        assert!(task.handle_disconnect(false, false).await.is_err());
        assert!(!task.connect_state.is_active());
        tokio::time::timeout(Duration::from_secs(1), async {
            while let Some(event) = output.recv().await {
                if let PlayerEvent::SessionDisconnected {
                    connection_lost, ..
                } = event
                {
                    assert!(!connection_lost);
                    return;
                }
            }
            panic!("missing ordinary disconnection before failed cleanup")
        })
        .await
        .unwrap();
        drop(task);
        while let Some(event) = output.recv().await {
            assert!(!matches!(
                event,
                PlayerEvent::SessionDisconnected {
                    connection_lost: true,
                    ..
                }
            ));
        }
    }

    #[tokio::test]
    async fn recovery_waits_for_queue_and_confirms_resumed_playback_once() {
        let (mut task, mut output) = fixture(true);
        paused(&mut task);
        assert!(task.bootstrap_pending);
        assert!(!task.recovery_resume_requested);
        task.bootstrap_queue_ready = true;
        task.maybe_finish_bootstrap();
        assert!(task.recovery_resume_requested);
        assert!(task.bootstrap_pending);
        let position = task.position();
        task.handle_player_event(PlayerEvent::Playing {
            play_request_id: 1,
            track_id: SpotifyUri::from_uri("spotify:track:6rqhFgbbKwnb9MLmUQDhG6").unwrap(),
            position_ms: position,
        })
        .unwrap();
        assert!(!task.bootstrap_pending);
        let mut queue_seen = false;
        while let Some(event) = output.recv().await {
            match event {
                PlayerEvent::SetQueue { .. } => queue_seen = true,
                PlayerEvent::ConnectionReady {
                    outcome: ConnectionOutcome::Local(_),
                } => {
                    assert!(queue_seen);
                    break;
                }
                _ => {}
            }
        }
        task.maybe_finish_bootstrap();
        assert!(!task.bootstrap_pending);
    }
    #[tokio::test]
    async fn recovery_paused_or_changed_occurrence_never_resumes() {
        for (resume, changed) in [(false, false), (true, true)] {
            let (mut task, _) = fixture(resume);
            if changed {
                task.connect_state
                    .player_mut()
                    .track
                    .mut_or_insert_default()
                    .uid = "other".into();
            }
            task.bootstrap_queue_ready = true;
            paused(&mut task);
            assert!(!task.recovery_resume_requested);
            assert!(!task.bootstrap_pending);
        }
    }
    #[tokio::test]
    async fn recovery_foreign_ownership_abandons_before_activation() {
        let (mut task, _) = fixture(true);
        task.latest_owner = "foreign".into();
        task.bootstrap_queue_ready = true;
        paused(&mut task);
        assert!(!task.recovery_resume_requested);
        assert!(!task.connect_state.is_active());
    }
    #[tokio::test]
    async fn session_shutdown_ends_waiting_loop_without_other_input() {
        let (mut task, _output) = fixture(false);
        task.bootstrap_pending = false;
        task.context_resolver.clear();
        task.connection_id_update = task.session.dealer()
            .listen_for("hm://pusher/v1/connections/", |_| Ok("fixture".to_string())).unwrap();
        let session = task.session.clone();
        let mut event_loop = Box::pin(task.run_event_loop());
        let mut cx = std::task::Context::from_waker(futures_util::task::noop_waker_ref());
        assert!(event_loop.as_mut().poll(&mut cx).is_pending());
        session.shutdown();
        assert!(event_loop.as_mut().poll(&mut cx).is_ready(),
            "Session::shutdown should end a waiting Spirc loop without needing unrelated input");
    }

}
