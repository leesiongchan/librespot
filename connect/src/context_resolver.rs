use crate::{
    core::{Error, Session},
    protocol::{
        autoplay_context_request::AutoplayContextRequest, context::Context,
        transfer_state::TransferState,
    },
    state::{ConnectState, StateError, context::ContextType},
};
use std::{
    cmp::PartialEq,
    collections::{HashMap, VecDeque},
    fmt::{Display, Formatter},
    hash::Hash,
    time::Duration,
};
use thiserror::Error as ThisError;
use tokio::time::Instant;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
enum Resolve {
    Uri(String),
    Context(Context),
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(super) enum ContextAction {
    Append,
    Replace,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(super) struct ResolveContext {
    resolve: Resolve,
    fallback: Option<String>,
    update: ContextType,
    action: ContextAction,
}

impl ResolveContext {
    fn append_context(uri: impl Into<String>) -> Self {
        Self {
            resolve: Resolve::Uri(uri.into()),
            fallback: None,
            update: ContextType::Default,
            action: ContextAction::Append,
        }
    }

    pub fn from_uri(
        uri: impl Into<String>,
        fallback: impl Into<String>,
        update: ContextType,
        action: ContextAction,
    ) -> Self {
        let fallback_uri = fallback.into();
        Self {
            resolve: Resolve::Uri(uri.into()),
            fallback: (!fallback_uri.is_empty()).then_some(fallback_uri),
            update,
            action,
        }
    }

    pub fn from_context(context: Context, update: ContextType, action: ContextAction) -> Self {
        Self {
            resolve: Resolve::Context(context),
            fallback: None,
            update,
            action,
        }
    }

    /// the uri which should be used to resolve the context, might not be the context uri
    fn resolve_uri(&self) -> Option<&str> {
        // it's important to call this always, or at least for every ResolveContext
        // otherwise we might not even check if we need to fallback and just use the fallback uri
        match self.resolve {
            Resolve::Uri(ref uri) => ConnectState::valid_resolve_uri(uri),
            Resolve::Context(ref ctx) => {
                ConnectState::find_valid_uri(ctx.uri.as_deref(), ctx.pages.first())
            }
        }
        .or(self.fallback.as_deref())
    }

    /// the actual context uri
    fn context_uri(&self) -> &str {
        match self.resolve {
            Resolve::Uri(ref uri) => uri,
            Resolve::Context(ref ctx) => ctx.uri.as_deref().unwrap_or_default(),
        }
    }
}

impl Display for ResolveContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "resolve_uri: <{:?}>, context_uri: <{}>, update: <{:?}>",
            self.resolve_uri(),
            self.context_uri(),
            self.update,
        )
    }
}

#[derive(Debug, ThisError)]
enum ContextResolverError {
    #[error("no next context to resolve")]
    NoNext,
    #[error("tried appending context with {0} pages")]
    UnexpectedPagesSize(usize),
    #[error("tried resolving not allowed context: {0:?}")]
    NotAllowedContext(String),
}

impl From<ContextResolverError> for Error {
    fn from(value: ContextResolverError) -> Self {
        Error::failed_precondition(value)
    }
}

pub struct ContextResolver {
    session: Session,
    queue: VecDeque<ResolveContext>,
    unavailable_contexts: HashMap<ResolveContext, Instant>,
    retry_at: Option<Instant>,
    retry_used: bool,
}

// time after which an unavailable context is retried
const RETRY_UNAVAILABLE: Duration = Duration::from_secs(3600);

impl ContextResolver {
    pub fn new(session: Session) -> Self {
        Self {
            session,
            queue: VecDeque::new(),
            unavailable_contexts: HashMap::new(),
            retry_at: None,
            retry_used: false,
        }
    }

    pub fn add(&mut self, resolve: ResolveContext) {
        let last_try = self
            .unavailable_contexts
            .get(&resolve)
            .map(Instant::elapsed);

        let last_try = if matches!(last_try, Some(last_try) if last_try > RETRY_UNAVAILABLE) {
            let _ = self.unavailable_contexts.remove(&resolve);
            debug!(
                "context was requested {}s ago, trying again to resolve the requested context",
                last_try.expect("checked by condition").as_secs()
            );
            None
        } else {
            last_try
        };

        if last_try.is_some() {
            debug!("tried loading unavailable context: {resolve}");
            return;
        } else if self.queue.contains(&resolve) {
            debug!("update for {resolve} is already added");
            return;
        } else {
            trace!(
                "added {} to resolver queue",
                resolve.resolve_uri().unwrap_or(resolve.context_uri())
            )
        }

        self.queue.push_back(resolve)
    }

    pub fn add_list(&mut self, resolve: Vec<ResolveContext>) {
        for resolve in resolve {
            self.add(resolve)
        }
    }

    pub fn add_required(&mut self, resolve: ResolveContext) {
        self.unavailable_contexts.remove(&resolve);
        self.add(resolve);
    }

    pub fn next_is_required(&self, active: ContextType) -> bool {
        matches!(self.find_next(), Some((next, _, _)) if next.update == active && next.action == ContextAction::Replace)
    }

    pub fn retry_next(&mut self) -> bool {
        if self.retry_used || !self.has_next() {
            return false;
        }
        self.retry_used = true;
        self.retry_at = Some(Instant::now() + Duration::from_millis(250));
        true
    }

    pub fn remove_used_and_invalid(&mut self) {
        self.retry_at = None;
        self.retry_used = false;
        if let Some((_, _, remove)) = self.find_next() {
            let _ = self.queue.drain(0..remove); // remove invalid
        }
        self.queue.pop_front(); // remove used
    }

    pub fn clear(&mut self) {
        self.queue = VecDeque::new();
        self.retry_at = None;
        self.retry_used = false;
    }

    fn find_next(&self) -> Option<(&ResolveContext, &str, usize)> {
        for idx in 0..self.queue.len() {
            let next = self.queue.get(idx)?;
            match next.resolve_uri() {
                None => {
                    warn!("skipped {idx} because of invalid resolve_uri: {next}");
                    continue;
                }
                Some(uri) => return Some((next, uri, idx)),
            }
        }
        None
    }

    pub fn has_next(&self) -> bool {
        self.find_next().is_some()
    }

    pub async fn get_next_context(
        &self,
        recent_track_uri: impl Fn() -> Vec<String>,
    ) -> Result<Context, Error> {
        if let Some(retry_at) = self.retry_at {
            tokio::time::sleep_until(retry_at).await;
        }
        let (next, resolve_uri, _) = self.find_next().ok_or(ContextResolverError::NoNext)?;

        match next.update {
            ContextType::Default => {
                let mut ctx = self.session.spclient().get_context(resolve_uri).await;
                if let Ok(ctx) = ctx.as_mut() {
                    ctx.uri = Some(next.context_uri().to_string());
                    ctx.url = ctx.uri.as_ref().map(|s| format!("context://{s}"));
                }

                ctx
            }
            ContextType::Autoplay => {
                if resolve_uri.contains("spotify:show:") || resolve_uri.contains("spotify:episode:")
                {
                    // autoplay is not supported for podcasts
                    Err(ContextResolverError::NotAllowedContext(
                        resolve_uri.to_string(),
                    ))?
                }

                let request = AutoplayContextRequest {
                    context_uri: Some(resolve_uri.to_string()),
                    recent_track_uri: recent_track_uri(),
                    ..Default::default()
                };
                self.session.spclient().get_autoplay_context(&request).await
            }
        }
    }

    pub fn mark_next_unavailable(&mut self) {
        if let Some((next, _, _)) = self.find_next() {
            self.unavailable_contexts
                .insert(next.clone(), Instant::now());
        }
    }

    pub fn apply_next_context(
        &self,
        state: &mut ConnectState,
        mut context: Context,
    ) -> Result<Option<Vec<ResolveContext>>, Error> {
        let (next, _, _) = self.find_next().ok_or(ContextResolverError::NoNext)?;

        let mut staged = state.clone();
        let native_queue = staged
            .preserve_recovery_queue
            .then(|| staged.player().clone());
        let remaining = match next.action {
            ContextAction::Append if context.pages.len() == 1 => staged
                .fill_context_from_page(context.pages.remove(0))
                .map(|_| None),
            ContextAction::Replace => {
                let remaining = staged.update_context(context, next.update);
                if let Resolve::Context(ref ctx) = next.resolve {
                    staged.merge_context(ctx.pages.clone().pop());
                }

                remaining
            }
            ContextAction::Append => {
                warn!("unexpected page size: {context:#?}");
                Err(ContextResolverError::UnexpectedPagesSize(context.pages.len()).into())
            }
        }?;

        if staged.get_context(next.update)?.tracks.is_empty() {
            Err(StateError::ContextHasNoTracks)?;
        }

        if let Some(native) = native_queue {
            staged.restore_recovery_player_state(native)?;
        }
        *state = staged;

        Ok(remaining.map(|remaining| {
            remaining
                .into_iter()
                .map(ResolveContext::append_context)
                .collect::<Vec<_>>()
        }))
    }

    pub fn try_finish(
        &self,
        state: &mut ConnectState,
        transfer_state: &mut Option<TransferState>,
    ) -> Result<bool, Error> {
        let (next, _, _) = match self.find_next() {
            None => return Ok(false),
            Some(next) => next,
        };

        if let Some(transfer) = transfer_state.as_ref() {
            let track = match state.player().track.as_ref() {
                Some(track) => track.clone(),
                None => state.current_track_from_transfer(transfer)?,
            };
            let required = ContextType::for_track(&track);
            if next.update != required || state.get_context(required).is_err() {
                return Ok(false);
            }
            // Transfer completion can fail after queue insertion or shuffle preparation.
            // Commit the state and consume its transfer together, only after success.
            let mut staged = state.clone();
            staged.finish_transfer(transfer.clone())?;
            staged.update_restrictions();
            staged.update_queue_revision();
            *state = staged;
            *transfer_state = None;
            return Ok(true);
        }

        // when there is only one update type, we are the last of our kind, so we should update the state
        if self
            .queue
            .iter()
            .filter(|resolve| resolve.update == next.update)
            .count()
            != 1
        {
            return Ok(false);
        }

        match (next.update, state.active_context) {
            (ContextType::Default, ContextType::Default) | (ContextType::Autoplay, _) => {
                debug!(
                    "last item of type <{:?}>, finishing state setup",
                    next.update
                );
            }
            (ContextType::Default, _) => {
                debug!("skipped finishing default, because it isn't the active context");
                return Ok(false);
            }
        }

        if state.preserve_recovery_queue {
            return Ok(state.get_context(state.active_context).is_ok());
        }
        let mut staged = state.clone();
        let active_ctx = staged.get_context(staged.active_context);
        if staged.shuffling_context() && next.update == ContextType::Default {
            staged.shuffle_new()?;
        } else if matches!(active_ctx, Ok(ctx) if ctx.index.track == 0) {
            // has context, and context is not touched
            // when the index is not zero, the next index was already evaluated elsewhere
            let ctx = active_ctx.expect("checked by precondition");
            let idx = ConnectState::find_index_in_context(ctx, |t| {
                staged.current_track(|c| t.uri == c.uri)
            })
            .ok();

            staged.reset_playback_to_position(idx)?;
        } else {
            staged.fill_up_next_tracks()?;
        }

        staged.update_restrictions();
        staged.update_queue_revision();
        *state = staged;
        Ok(true)
    }
}

#[cfg(test)]
mod transfer_tests {
    use super::*;
    use crate::{
        ConnectConfig,
        protocol::{context_page::ContextPage, context_track::ContextTrack, playback::Playback},
        state::provider::IsProvider,
    };
    use protobuf::MessageField;

    const URI: &str = "spotify:track:6rqhFgbbKwnb9MLmUQDhG6";

    fn track(uid: &str) -> ContextTrack {
        ContextTrack {
            uri: Some(URI.into()),
            uid: Some(uid.into()),
            ..Default::default()
        }
    }

    fn context() -> Context {
        Context {
            uri: Some(URI.into()),
            pages: vec![ContextPage {
                tracks: vec![track("first"), track("current"), track("next")],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn fixture(autoplay: bool) -> (ConnectState, ContextResolver, Option<TransferState>) {
        let session = Session::new(crate::core::SessionConfig::default(), None);
        let mut state = ConnectState::new(ConnectConfig::default(), &session);
        let mut current = track("current");
        if autoplay {
            current
                .metadata
                .insert("autoplay.is_autoplay".into(), "true".into());
        }
        let mut transfer = TransferState {
            playback: MessageField::some(Playback {
                current_track: MessageField::some(current),
                is_paused: Some(true),
                ..Default::default()
            }),
            ..Default::default()
        };
        transfer
            .current_session
            .mut_or_insert_default()
            .context
            .mut_or_insert_default()
            .uri = Some(URI.into());
        transfer
            .queue
            .mut_or_insert_default()
            .tracks
            .push(track("queued"));
        state.set_track(state.current_track_from_transfer(&transfer).unwrap());
        (state, ContextResolver::new(session), Some(transfer))
    }

    fn resolve(resolver: &mut ContextResolver, state: &mut ConnectState, ty: ContextType) {
        let context = context();
        resolver.add(ResolveContext::from_uri(
            URI,
            URI,
            ty,
            ContextAction::Replace,
        ));
        resolver.apply_next_context(state, context).unwrap();
    }

    #[tokio::test]
    async fn context_update_preserves_duplicate_occurrences_before_transfer_completion() {
        let (mut state, mut resolver, mut transfer) = fixture(false);
        let current = track("next");
        transfer
            .as_mut()
            .unwrap()
            .playback
            .mut_or_insert_default()
            .current_track = MessageField::some(current.clone());
        state.set_track(
            state
                .current_track_from_transfer(transfer.as_ref().unwrap())
                .unwrap(),
        );
        let context = context();
        resolver.add(ResolveContext::from_context(
            context.clone(),
            ContextType::Default,
            ContextAction::Replace,
        ));
        resolver.apply_next_context(&mut state, context).unwrap();
        let uids: Vec<_> = state
            .get_context(ContextType::Default)
            .unwrap()
            .tracks
            .iter()
            .map(|track| track.uid.as_str())
            .collect();
        assert_eq!(uids, ["first", "current", "next"]);
        assert!(resolver.try_finish(&mut state, &mut transfer).unwrap());
        assert_eq!(state.player().track.uid, "next");
        assert_eq!(state.player().index.track, 2);
    }

    #[tokio::test]
    async fn context_updates_match_uids_then_distinct_uri_occurrences() {
        for (incoming, expected) in [
            (
                [Some("next"), Some("current"), Some("first")],
                ["first", "current", "next"],
            ),
            (
                [Some("new-first"), Some("new-current"), Some("new-next")],
                ["new-first", "new-current", "new-next"],
            ),
            ([Some(""), None, Some("next")], ["first", "current", "next"]),
            (
                [Some("new-first"), Some("first"), Some("next")],
                ["first", "new-first", "next"],
            ),
        ] {
            let (mut state, mut resolver, _) = fixture(false);
            let mut update = context();
            for (track, uid) in update.pages[0].tracks.iter_mut().zip(incoming) {
                let label = uid
                    .filter(|uid| !uid.is_empty())
                    .or(track.uid.as_deref())
                    .unwrap();
                track.metadata.insert("merged_uid".into(), label.into());
                track.uid = uid.map(str::to_owned);
            }
            resolver.add(ResolveContext::from_context(
                update,
                ContextType::Default,
                ContextAction::Replace,
            ));
            resolver.apply_next_context(&mut state, context()).unwrap();
            let uids: Vec<_> = state
                .get_context(ContextType::Default)
                .unwrap()
                .tracks
                .iter()
                .map(|track| track.uid.as_str())
                .collect();
            assert_eq!(uids, expected);
            let labels: Vec<_> = state
                .get_context(ContextType::Default)
                .unwrap()
                .tracks
                .iter()
                .map(|track| track.metadata["merged_uid"].as_str())
                .collect();
            assert_eq!(labels, expected);
        }
    }

    #[tokio::test]
    async fn default_first_keeps_pending_autoplay_transfer() {
        let (mut state, mut resolver, mut transfer) = fixture(true);
        resolve(&mut resolver, &mut state, ContextType::Default);
        let before = state.player().clone();
        assert!(!resolver.try_finish(&mut state, &mut transfer).unwrap());
        assert!(transfer.is_some());
        assert_eq!(state.player(), &before);
    }

    #[tokio::test]
    async fn autoplay_first_keeps_pending_default_transfer() {
        let (mut state, mut resolver, mut transfer) = fixture(false);
        resolve(&mut resolver, &mut state, ContextType::Autoplay);
        assert!(!resolver.try_finish(&mut state, &mut transfer).unwrap());
        assert!(transfer.is_some());
    }

    #[tokio::test]
    async fn completed_transfer_preserves_occurrence_and_imports_queue_once() {
        for autoplay in [false, true] {
            let (mut state, mut resolver, mut transfer) = fixture(autoplay);
            let ty = if autoplay {
                ContextType::Autoplay
            } else {
                ContextType::Default
            };
            resolve(&mut resolver, &mut state, ty);
            assert!(resolver.try_finish(&mut state, &mut transfer).unwrap());
            assert!(transfer.is_none());
            assert_eq!(state.player().track.uid, "current");
            assert_eq!(state.player().index.track, 1);
            assert_eq!(
                state
                    .player()
                    .next_tracks
                    .iter()
                    .filter(|t| t.is_queue())
                    .count(),
                1
            );
            assert!(resolver.try_finish(&mut state, &mut transfer).unwrap());
            assert_eq!(
                state
                    .player()
                    .next_tracks
                    .iter()
                    .filter(|t| t.is_queue())
                    .count(),
                1
            );
        }
    }

    #[tokio::test]
    async fn transfer_preserves_queued_uids_and_reserves_future_occurrences() {
        for autoplay in [false, true] {
            let (mut state, mut resolver, mut transfer) = fixture(autoplay);
            let pending = transfer.as_mut().unwrap();
            pending.queue.mut_or_insert_default().tracks =
                vec![track(""), track("q0"), track("external-occurrence")];
            resolve(
                &mut resolver,
                &mut state,
                if autoplay {
                    ContextType::Autoplay
                } else {
                    ContextType::Default
                },
            );
            assert!(resolver.try_finish(&mut state, &mut transfer).unwrap());
            let queued: Vec<_> = state
                .player()
                .next_tracks
                .iter()
                .filter(|t| t.is_queue())
                .map(|t| t.uid.as_str())
                .collect();
            assert!(!queued[0].is_empty());
            assert_ne!(queued[0], queued[1]);
            assert_ne!(queued[0], queued[2]);
            assert_eq!(&queued[1..], ["q0", "external-occurrence"]);
            state.add_to_queue(state.player().track.as_ref().unwrap().clone(), true);
            let queued: Vec<_> = state
                .player()
                .next_tracks
                .iter()
                .filter(|t| t.is_queue())
                .map(|t| t.uid.as_str())
                .collect();
            assert_eq!(&queued[1..], ["q0", "external-occurrence", "q1"]);
        }
    }

    #[tokio::test]
    async fn transfer_reserves_playing_queue_uid_before_new_additions() {
        let (mut state, mut resolver, mut transfer) = fixture(false);
        let pending = transfer.as_mut().unwrap();
        pending.queue.mut_or_insert_default().is_playing_queue = Some(true);
        pending.queue.mut_or_insert_default().tracks = vec![track("q8"), track("q3")];
        pending.current_session.mut_or_insert_default().current_uid = Some("current".into());
        state.set_track(state.current_track_from_transfer(pending).unwrap());
        resolve(&mut resolver, &mut state, ContextType::Default);
        assert!(resolver.try_finish(&mut state, &mut transfer).unwrap());
        state.add_to_queue(state.player().track.as_ref().unwrap().clone(), true);
        assert_eq!(state.player().track.uid, "q8");
        let queued: Vec<_> = state
            .player()
            .next_tracks
            .iter()
            .filter(|t| t.is_queue())
            .map(|t| t.uid.as_str())
            .collect();
        assert_eq!(queued, ["q3", "q9"]);
    }

    #[tokio::test]
    async fn provider_only_autoplay_transfer_uses_autoplay_context() {
        let (mut state, mut resolver, mut transfer) = fixture(false);
        state.player_mut().track.mut_or_insert_default().provider = "autoplay".into();
        resolve(&mut resolver, &mut state, ContextType::Autoplay);
        assert!(resolver.try_finish(&mut state, &mut transfer).unwrap());
        assert!(transfer.is_none());
        assert_eq!(state.active_context, ContextType::Autoplay);
    }

    #[tokio::test]
    async fn failed_completion_keeps_state_and_transfer() {
        let (mut state, mut resolver, mut transfer) = fixture(false);
        resolve(&mut resolver, &mut state, ContextType::Default);
        state.set_shuffle(true);
        state.get_context_mut(ContextType::Default).unwrap().tracks = Vec::new().into();
        let before = state.player().clone();
        assert!(resolver.try_finish(&mut state, &mut transfer).is_err());
        assert!(transfer.is_some());
        assert_eq!(state.player(), &before);
    }

    #[tokio::test]
    async fn missing_current_track_is_classified_from_transfer_metadata() {
        let (mut state, mut resolver, mut transfer) = fixture(true);
        state.player_mut().track = MessageField::none();
        resolve(&mut resolver, &mut state, ContextType::Autoplay);
        assert!(resolver.try_finish(&mut state, &mut transfer).unwrap());
        assert!(transfer.is_none());
        assert_eq!(state.player().track.uid, "current");
    }

    #[tokio::test]
    async fn queued_current_occurrence_is_not_imported_twice() {
        let (mut state, mut resolver, mut transfer) = fixture(true);
        let pending = transfer.as_mut().unwrap();
        pending.queue.mut_or_insert_default().is_playing_queue = Some(true);
        pending.queue.mut_or_insert_default().tracks[0]
            .metadata
            .insert("autoplay.is_autoplay".into(), "true".into());
        pending
            .queue
            .mut_or_insert_default()
            .tracks
            .push(track("queued-next"));
        pending.current_session.mut_or_insert_default().current_uid = Some("current".into());
        state.set_track(state.current_track_from_transfer(pending).unwrap());
        state.active_context = ContextType::Autoplay;
        resolve(&mut resolver, &mut state, ContextType::Autoplay);
        assert!(resolver.try_finish(&mut state, &mut transfer).unwrap());
        assert_eq!(state.player().track.uid, "queued");
        assert!(state.player().track.is_queue());
        assert_eq!(
            state
                .player()
                .next_tracks
                .iter()
                .filter(|t| t.is_queue())
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn autoplay_without_current_in_context_keeps_transferred_occurrence() {
        let (mut state, mut resolver, mut transfer) = fixture(true);
        let mut context = context();
        for track in &mut context.pages[0].tracks {
            track.uri = Some("spotify:track:7ouMYWpwJ422jRcDASZB7P".into());
            track.uid = Some("future".into());
        }
        resolver.add(ResolveContext::from_uri(
            URI,
            URI,
            ContextType::Autoplay,
            ContextAction::Replace,
        ));
        resolver.apply_next_context(&mut state, context).unwrap();
        assert!(resolver.try_finish(&mut state, &mut transfer).unwrap());
        assert_eq!(state.player().track.uid, "current");
        assert_eq!(state.player().track.uri, URI);
        assert_eq!(
            state
                .player()
                .next_tracks
                .iter()
                .filter(|t| t.is_queue())
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn autoplay_does_not_shuffle_unavailable_default_context() {
        let (mut state, mut resolver, mut transfer) = fixture(true);
        state.set_shuffle(true);
        resolve(&mut resolver, &mut state, ContextType::Autoplay);
        assert!(resolver.try_finish(&mut state, &mut transfer).unwrap());
        assert_eq!(state.player().track.uid, "current");
        assert!(state.shuffling_context());
    }

    #[tokio::test]
    async fn default_shuffle_preserves_queued_or_missing_current_occurrence() {
        for queued in [false, true] {
            let (mut state, mut resolver, mut transfer) = fixture(false);
            state.set_shuffle(true);
            state.player_mut().track.mut_or_insert_default().uid = "transferred".into();
            state.player_mut().track.mut_or_insert_default().uri =
                "spotify:track:7ouMYWpwJ422jRcDASZB7P".into();
            if queued {
                state.player_mut().track.mut_or_insert_default().provider = "queue".into();
            }
            resolve(&mut resolver, &mut state, ContextType::Default);
            assert!(resolver.try_finish(&mut state, &mut transfer).unwrap());
            assert_eq!(state.player().track.uid, "transferred");
            assert_eq!(state.player().track.is_queue(), queued);
        }
    }

    #[tokio::test]
    async fn newer_transfer_replaces_previous_explicit_queue() {
        let (mut state, _, mut transfer) = fixture(false);
        state.add_to_queue(state.player().track.as_ref().unwrap().clone(), false);
        assert_eq!(state.player().next_tracks.len(), 1);
        state.handle_initial_transfer(transfer.as_mut().unwrap(), Some(URI.into()));
        assert!(state.player().next_tracks.is_empty());
    }

    #[tokio::test]
    async fn required_request_bypasses_previous_unavailable_cache() {
        let (_, mut resolver, _) = fixture(false);
        let resolve =
            ResolveContext::from_uri(URI, URI, ContextType::Default, ContextAction::Replace);
        resolver.add(resolve.clone());
        resolver.mark_next_unavailable();
        resolver.remove_used_and_invalid();
        resolver.add_required(resolve);
        assert!(resolver.has_next());
    }

    #[tokio::test]
    async fn retry_delay_survives_cancelled_polling_and_is_bounded() {
        let (_, mut resolver, _) = fixture(false);
        resolver.add_required(ResolveContext::from_uri(
            URI,
            URI,
            ContextType::Default,
            ContextAction::Replace,
        ));
        assert!(resolver.retry_next());
        let deadline = resolver.retry_at;
        for _ in 0..2 {
            assert!(
                tokio::time::timeout(
                    Duration::from_millis(1),
                    resolver.get_next_context(Vec::new)
                )
                .await
                .is_err()
            );
            assert_eq!(resolver.retry_at, deadline);
        }
        assert!(!resolver.retry_next());
    }
}
