use crate::{
    core::{Error, SpotifyUri},
    protocol::player::ProvidedTrack,
    state::{
        ConnectState, SPOTIFY_MAX_NEXT_TRACKS_SIZE, SPOTIFY_MAX_PREV_TRACKS_SIZE, StateError,
        context::ContextType,
        metadata::Metadata,
        provider::{IsProvider, Provider},
    },
};
use protobuf::MessageField;
use rand::Rng;

// identifier used as part of the uid
pub const IDENTIFIER_DELIMITER: &str = "delimiter";

impl<'ct> ConnectState {
    fn new_delimiter(iteration: i64) -> ProvidedTrack {
        let mut delimiter = ProvidedTrack {
            uri: format!("spotify:{IDENTIFIER_DELIMITER}"),
            uid: format!("{IDENTIFIER_DELIMITER}{iteration}"),
            provider: Provider::Context.to_string(),
            ..Default::default()
        };
        delimiter.set_hidden(true);
        delimiter.set_iteration(iteration);

        delimiter
    }

    fn push_prev(&mut self, prev: ProvidedTrack) {
        let prev_tracks = self.prev_tracks_mut();
        // add prev track, while preserving a length of 10
        if prev_tracks.len() >= SPOTIFY_MAX_PREV_TRACKS_SIZE {
            // todo: O(n), but technically only maximal O(SPOTIFY_MAX_PREV_TRACKS_SIZE) aka O(10)
            let _ = prev_tracks.remove(0);
        }
        prev_tracks.push(prev)
    }

    /// bottom => top, aka the last track of the list is the prev track
    fn prev_tracks_mut(&mut self) -> &mut Vec<ProvidedTrack> {
        &mut self.player_mut().prev_tracks
    }

    /// bottom => top, aka the last track of the list is the prev track
    pub(super) fn prev_tracks(&self) -> &Vec<ProvidedTrack> {
        &self.player().prev_tracks
    }

    /// top => bottom, aka the first track of the list is the next track
    fn next_tracks_mut(&mut self) -> &mut Vec<ProvidedTrack> {
        &mut self.player_mut().next_tracks
    }

    /// top => bottom, aka the first track of the list is the next track
    pub(super) fn next_tracks(&self) -> &Vec<ProvidedTrack> {
        &self.player().next_tracks
    }

    pub fn set_current_track_random(&mut self) -> Result<(), Error> {
        let max_tracks = self.get_context(self.active_context)?.tracks.len();
        let rng_track = rand::rng().random_range(0..max_tracks);
        self.set_current_track(rng_track)
    }

    pub fn set_current_track(&mut self, index: usize) -> Result<(), Error> {
        let context = self.get_context(self.active_context)?;

        let new_track = context
            .tracks
            .get(index)
            .ok_or(StateError::CanNotFindTrackInContext(
                Some(index),
                context.tracks.len(),
            ))?;

        debug!(
            "set track to: {} at {} of {} tracks",
            new_track.uri,
            index,
            context.tracks.len()
        );

        self.set_track(new_track.clone());

        self.update_current_index(|i| i.track = index as u32);

        Ok(())
    }

    /// Move to the next track
    ///
    /// Updates the current track to the next track. Adds the old track
    /// to prev tracks and fills up the next tracks from the current context
    pub fn next_track(&mut self) -> Result<Option<u32>, Error> {
        let selected = self
            .next_tracks()
            .iter()
            .position(|t| !t.uid.starts_with(IDENTIFIER_DELIMITER) && !t.is_unavailable());
        match selected {
            Some(index) => self.promote_next(index).map(Some),
            None => {
                self.set_repeat_track(false);
                if let Some(old) = self.player_mut().track.take() {
                    if old.is_context() || old.is_autoplay() {
                        self.push_prev(old);
                    }
                }
                let skipped = std::mem::take(self.next_tracks_mut());
                for track in skipped {
                    if track.uid.starts_with(IDENTIFIER_DELIMITER) {
                        self.push_prev(track);
                    }
                }
                Ok(None)
            }
        }
    }

    pub(crate) fn select_queue_entry(
        &mut self,
        generation: u64,
        selection: crate::spirc::QueueSelection,
    ) -> Result<(), crate::spirc::QueueSelectionError> {
        use crate::spirc::{QueueSelectionError, validate_selection};
        let index = validate_selection(generation, selection, self.next_tracks().len())?;
        let track = &self.next_tracks()[index];
        if track.is_unavailable()
            || !matches!(
                SpotifyUri::from_uri(&track.uri),
                Ok(SpotifyUri::Track { .. } | SpotifyUri::Episode { .. })
            )
        {
            return Err(QueueSelectionError::Unavailable);
        }
        // Refilling can fail when a transfer has not resolved its context yet.
        // Stage the bounded state change so rejection cannot consume history.
        let mut staged = self.clone();
        staged
            .promote_next(index)
            .map_err(|_| QueueSelectionError::Unavailable)?;
        *self = staged;
        Ok(())
    }

    fn promote_next(&mut self, index: usize) -> Result<u32, Error> {
        self.queue_movement = Some(crate::queue::QueueMovement::Forward(index));
        self.set_repeat_track(false);
        if let Some(old) = self.player_mut().track.take() {
            if old.is_context() || old.is_autoplay() {
                self.push_prev(old);
            }
        }
        let mut prefix: Vec<_> = self.next_tracks_mut().drain(..=index).collect();
        let new_track = prefix.pop().expect("validated upcoming index");
        for track in prefix {
            if track.uid.starts_with(IDENTIFIER_DELIMITER)
                || (!track.is_unavailable() && (track.is_context() || track.is_autoplay()))
            {
                self.push_prev(track);
            }
        }

        self.fill_up_next_tracks()?;

        let update_index = if new_track.is_queue() {
            None
        } else if new_track.is_autoplay() {
            self.set_active_context(ContextType::Autoplay);
            None
        } else {
            match new_track.get_context_index() {
                Some(new_index) => Some(new_index as u32),
                None => {
                    error!("the given context track had no set context_index");
                    None
                }
            }
        };

        if let Some(update_index) = update_index {
            self.update_current_index(|i| i.track = update_index)
        } else {
            self.player_mut().index.clear()
        }

        self.set_track(new_track);
        self.update_restrictions();

        Ok(self.player().index.track)
    }

    /// Move to the prev track
    ///
    /// Updates the current track to the prev track. Adds the old track
    /// to next tracks (when from the context) and fills up the prev tracks from the
    /// current context
    pub fn prev_track(&mut self) -> Result<Option<&MessageField<ProvidedTrack>>, Error> {
        self.queue_movement = Some(crate::queue::QueueMovement::Backward);
        let old_track = self.player_mut().track.take();

        if let Some(old_track) = old_track {
            if old_track.is_context() || old_track.is_autoplay() {
                // todo: O(n)
                self.next_tracks_mut().insert(0, old_track);
            }
        }

        // handle possible delimiter
        if matches!(self.prev_tracks().last(), Some(prev) if prev.uid.starts_with(IDENTIFIER_DELIMITER))
        {
            let delimiter = self
                .prev_tracks_mut()
                .pop()
                .expect("item that was prechecked");

            let next_tracks = self.next_tracks_mut();
            if next_tracks.len() >= SPOTIFY_MAX_NEXT_TRACKS_SIZE {
                let _ = next_tracks.pop();
            }
            // todo: O(n)
            next_tracks.insert(0, delimiter)
        }

        while self.next_tracks().len() > SPOTIFY_MAX_NEXT_TRACKS_SIZE {
            let _ = self.next_tracks_mut().pop();
        }

        let new_track = match self.prev_tracks_mut().pop() {
            None => return Ok(None),
            Some(t) => t,
        };

        if matches!(self.active_context, ContextType::Autoplay if new_track.is_context()) {
            // transition back to default context
            self.set_active_context(ContextType::Default);
        }

        self.fill_up_next_tracks()?;
        self.set_track(new_track);

        if self.player().index.track == 0 {
            warn!("prev: trying to skip into negative, index update skipped")
        } else {
            self.update_current_index(|i| i.track -= 1)
        }

        self.update_restrictions();

        Ok(Some(self.current_track(|t| t)))
    }

    pub fn current_track<F: Fn(&'ct MessageField<ProvidedTrack>) -> R, R>(
        &'ct self,
        access: F,
    ) -> R {
        access(&self.player().track)
    }

    pub fn set_track(&mut self, track: ProvidedTrack) {
        self.player_mut().track = MessageField::some(track)
    }

    pub fn set_next_tracks(&mut self, mut tracks: Vec<ProvidedTrack>) {
        self.preserve_recovery_queue = false;
        self.queue_replaced = true;
        // mobile only sends a set_queue command instead of an add_to_queue command
        // in addition to handling the mobile add_to_queue handling, this should also handle
        // a mass queue addition
        tracks
            .iter_mut()
            .filter(|t| t.is_from_queue())
            .for_each(|t| {
                t.set_provider(Provider::Queue);
                // technically we could preserve the queue-uid here,
                // but it seems to work without that, so we just override it
                t.uid = format!("q{}", self.queue_count);
                self.queue_count += 1;
            });

        // when you drag 'n drop the current track in the queue view into the "Next from: ..."
        // section, it is only send as an empty item with just the provider and metadata, so we have
        // to provide set the uri from the current track manually
        tracks
            .iter_mut()
            .filter(|t| t.uri.is_empty())
            .for_each(|t| t.uri = self.current_track(|ct| ct.uri.clone()));

        self.player_mut().next_tracks = tracks;
    }

    pub fn set_prev_tracks(&mut self, tracks: Vec<ProvidedTrack>) {
        self.queue_replaced = true;
        self.player_mut().prev_tracks = tracks;
    }

    pub fn clear_prev_track(&mut self) {
        self.prev_tracks_mut().clear()
    }

    /// Remove all queued tracks
    ///
    /// Keeps the tracks from the context in the next tracks and fills
    /// them up from the current context afterwards
    pub fn clear_queue(&mut self) -> Result<(), Error> {
        self.next_tracks_mut().retain(|t| !t.is_queue());
        self.fill_up_next_tracks()?;
        self.update_queue_revision();
        Ok(())
    }

    pub fn clear_next_tracks(&mut self) {
        // respect queued track and don't throw them out of our next played tracks
        let first_non_queued_track = self
            .next_tracks()
            .iter()
            .enumerate()
            .find(|(_, track)| !track.is_queue());

        if let Some((non_queued_track, _)) = first_non_queued_track {
            while self.next_tracks().len() > non_queued_track
                && self.next_tracks_mut().pop().is_some()
            {}
        }
    }

    pub fn fill_up_next_tracks(&mut self) -> Result<(), Error> {
        let ctx = self.get_context(self.fill_up_context)?;
        let mut new_index = ctx.index.track as usize;
        let mut iteration = ctx.index.page;

        while self.next_tracks().len() < SPOTIFY_MAX_NEXT_TRACKS_SIZE {
            let ctx = self.get_context(self.fill_up_context)?;
            let track = match ctx.tracks.get(new_index) {
                None if self.repeat_context() => {
                    let delimiter = Self::new_delimiter(iteration.into());
                    iteration += 1;
                    new_index = 0;
                    delimiter
                }
                None if !matches!(self.fill_up_context, ContextType::Autoplay)
                    && self.autoplay_context.is_some()
                    && !self.repeat_context() =>
                {
                    self.update_context_index(self.fill_up_context, new_index)?;

                    // transition to autoplay as fill up context
                    self.fill_up_context = ContextType::Autoplay;
                    new_index = self.get_context(ContextType::Autoplay)?.index.track as usize;

                    // add delimiter to only display the current context
                    Self::new_delimiter(iteration.into())
                }
                None if self.autoplay_context.is_some() => {
                    match self
                        .get_context(ContextType::Autoplay)?
                        .tracks
                        .get(new_index)
                    {
                        None => break,
                        Some(ct) => {
                            new_index += 1;
                            ct.clone()
                        }
                    }
                }
                None => break,
                Some(ct) if ct.is_unavailable() || self.is_skip_track(ct, Some(iteration)) => {
                    debug!(
                        "skipped track {} during fillup as it's unavailable or should be skipped",
                        ct.uri
                    );
                    new_index += 1;
                    continue;
                }
                Some(ct) => {
                    new_index += 1;
                    ct.clone()
                }
            };

            self.next_tracks_mut().push(track);
        }

        debug!(
            "finished filling up next_tracks ({})",
            self.next_tracks().len()
        );

        self.update_context_index(self.fill_up_context, new_index)?;

        // the web-player needs a revision update, otherwise the queue isn't updated in the ui
        self.update_queue_revision();

        Ok(())
    }

    pub fn preview_next_track(&mut self) -> Option<SpotifyUri> {
        let next = if self.repeat_track() {
            self.current_track(|t| &t.uri)
        } else {
            &self.next_tracks().first()?.uri
        };

        SpotifyUri::from_uri(next).ok()
    }

    pub fn has_next_tracks(&self, min: Option<usize>) -> bool {
        if let Some(min) = min {
            self.next_tracks().len() >= min
        } else {
            !self.next_tracks().is_empty()
        }
    }

    pub fn recent_track_uris(&self) -> Vec<String> {
        let mut prev = self
            .prev_tracks()
            .iter()
            .map(|t| t.uri.clone())
            .collect::<Vec<_>>();

        prev.push(self.current_track(|t| t.uri.clone()));
        prev
    }

    pub fn mark_unavailable(&mut self, id: &SpotifyUri) -> Result<(), Error> {
        let uri = id.to_uri();

        debug!("marking {uri} as unavailable");

        let next_tracks = self.next_tracks_mut();
        while let Some(pos) = next_tracks.iter().position(|t| t.uri == uri) {
            let _ = next_tracks.remove(pos);
        }

        for next_track in next_tracks {
            Self::mark_as_unavailable_for_match(next_track, &uri)
        }

        let prev_tracks = self.prev_tracks_mut();
        while let Some(pos) = prev_tracks.iter().position(|t| t.uri == uri) {
            let _ = prev_tracks.remove(pos);
        }

        for prev_track in prev_tracks {
            Self::mark_as_unavailable_for_match(prev_track, &uri)
        }

        self.unavailable_uri.push(uri);
        self.fill_up_next_tracks()?;
        self.update_queue_revision();

        Ok(())
    }

    pub fn add_to_queue(&mut self, mut track: ProvidedTrack, rev_update: bool) {
        track.uid = format!("q{}", self.queue_count);
        self.queue_count += 1;

        self.insert_queue_track(track, rev_update);
    }

    pub(super) fn reserve_queue_uid(&mut self, uid: &str) {
        if let Some(number) = uid.strip_prefix('q').and_then(|n| n.parse::<u64>().ok()) {
            self.queue_count = self.queue_count.max(number.saturating_add(1));
        }
    }

    pub(super) fn insert_queue_track(&mut self, mut track: ProvidedTrack, rev_update: bool) {
        track.set_provider(Provider::Queue);
        if !track.is_from_queue() {
            track.set_from_queue(true);
        }

        let next_tracks = self.next_tracks_mut();
        if let Some(next_not_queued_track) = next_tracks.iter().position(|t| !t.is_queue()) {
            next_tracks.insert(next_not_queued_track, track);
        } else {
            next_tracks.push(track)
        }

        while next_tracks.len() > SPOTIFY_MAX_NEXT_TRACKS_SIZE {
            next_tracks.pop();
        }

        if rev_update {
            self.update_queue_revision();
        }
        self.update_restrictions();
    }
}

#[cfg(test)]
mod selection_tests {
    use super::*;
    use crate::protocol::{connect::Device, player::PlayerState};
    use crate::spirc::{QueueSelection, QueueSelectionError};
    use crate::state::context::StateContext;

    fn track(label: &str, provider: &str) -> ProvidedTrack {
        ProvidedTrack {
            uri: "spotify:track:6rqhFgbbKwnb9MLmUQDhG6".into(),
            uid: "same-repeated-uid".into(),
            provider: provider.into(),
            metadata: [("label".into(), label.into())].into(),
            ..Default::default()
        }
    }
    fn fixture() -> ConnectState {
        let mut state = ConnectState::default();
        state.request.device = MessageField::some(Device {
            player_state: MessageField::some(PlayerState::default()),
            ..Default::default()
        });
        state.context = Some(StateContext {
            tracks: Vec::new().into(),
            metadata: Default::default(),
            restrictions: None,
            index: Default::default(),
        });
        state.set_track(track("current", "context"));
        state.player_mut().next_tracks = vec![
            track("first", "context"),
            track("second", "context"),
            track("suffix", "queue"),
        ];
        state
    }
    #[test]
    fn selects_second_duplicate_and_preserves_suffix_and_history() {
        let mut state = fixture();
        state
            .select_queue_entry(
                7,
                QueueSelection {
                    generation: 7,
                    raw_next_index: 1,
                },
            )
            .unwrap();
        assert_eq!(state.player().track.metadata["label"], "second");
        assert_eq!(
            state
                .player()
                .next_tracks
                .iter()
                .map(|t| t.metadata["label"].as_str())
                .collect::<Vec<_>>(),
            ["suffix"]
        );
        assert_eq!(
            state
                .player()
                .prev_tracks
                .iter()
                .map(|t| t.metadata["label"].as_str())
                .collect::<Vec<_>>(),
            ["current", "first"]
        );
    }
    #[test]
    fn rejects_stale_or_invalid_without_mutating_state() {
        for (generation, raw_next_index, expected) in [
            (6, 1, QueueSelectionError::Stale),
            (7, 3, QueueSelectionError::OutOfRange),
        ] {
            let mut state = fixture();
            let before = state.request.clone();
            assert_eq!(
                state.select_queue_entry(
                    7,
                    QueueSelection {
                        generation,
                        raw_next_index
                    }
                ),
                Err(expected)
            );
            assert_eq!(state.request, before);
        }
    }
    #[test]
    fn rejects_unplayable_before_consuming_any_history() {
        let mut state = fixture();
        state.player_mut().next_tracks[1].provider = "unavailable".into();
        let before = state.request.clone();
        assert_eq!(
            state.select_queue_entry(
                7,
                QueueSelection {
                    generation: 7,
                    raw_next_index: 1
                }
            ),
            Err(QueueSelectionError::Unavailable)
        );
        assert_eq!(state.request, before);
    }
    #[test]
    fn bulk_skip_keeps_delimiters_and_ignores_manual_queue_in_history() {
        let mut state = fixture();
        state.player_mut().next_tracks = vec![
            track("queued", "queue"),
            ConnectState::new_delimiter(1),
            track("selected", "context"),
            track("suffix", "context"),
        ];
        state.set_repeat_track(true);
        state
            .select_queue_entry(
                7,
                QueueSelection {
                    generation: 7,
                    raw_next_index: 2,
                },
            )
            .unwrap();
        assert!(!state.repeat_track());
        assert_eq!(state.player().prev_tracks.len(), 2);
        assert!(state.player().prev_tracks[1].uid.starts_with("delimiter"));
        assert_eq!(state.player().next_tracks[0].metadata["label"], "suffix");
    }
    #[test]
    fn command_loads_only_selected_audio_once_and_rejects_without_loading() {
        let mut state = fixture();
        let mut loads = Vec::new();
        crate::spirc::dispatch_selection(
            &mut state,
            7,
            QueueSelection {
                generation: 7,
                raw_next_index: 1,
            },
            |id| loads.push(id),
        )
        .unwrap();
        assert_eq!(loads.len(), 1);
        assert_eq!(state.player().track.metadata["label"], "second");
        assert_eq!(
            crate::spirc::dispatch_selection(
                &mut state,
                8,
                QueueSelection {
                    generation: 7,
                    raw_next_index: 1
                },
                |id| loads.push(id)
            ),
            Err(QueueSelectionError::Stale)
        );
        assert_eq!(loads.len(), 1);
    }
    #[test]
    fn unresolved_context_rejects_without_consuming_queue() {
        let mut state = fixture();
        state.context = None;
        let before = state.request.clone();
        assert_eq!(
            state.select_queue_entry(
                7,
                QueueSelection {
                    generation: 7,
                    raw_next_index: 1
                }
            ),
            Err(QueueSelectionError::Unavailable)
        );
        assert_eq!(state.request, before);
    }
    #[test]
    fn autoplay_selection_switches_context_and_preserves_shuffle() {
        let mut state = fixture();
        state.player_mut().options =
            MessageField::some(crate::protocol::player::ContextPlayerOptions {
                shuffling_context: true,
                ..Default::default()
            });
        state.autoplay_context = Some(StateContext {
            tracks: Vec::new().into(),
            metadata: Default::default(),
            restrictions: None,
            index: Default::default(),
        });
        state.player_mut().next_tracks[1].provider = "autoplay".into();
        state
            .select_queue_entry(
                7,
                QueueSelection {
                    generation: 7,
                    raw_next_index: 1,
                },
            )
            .unwrap();
        assert_eq!(state.active_context, ContextType::Autoplay);
        assert!(state.player().options.shuffling_context);
        assert_eq!(state.player().next_tracks[0].metadata["label"], "suffix");
    }
    #[test]
    fn next_and_previous_publish_new_versions_with_continuous_identity() {
        let mut state = fixture();
        let mut tracker = crate::queue::QueueTracker::default();
        let before = state.queue_snapshot(&mut tracker).unwrap();
        state.next_track().unwrap();
        let next = state.queue_snapshot(&mut tracker).unwrap();
        assert_eq!(next.current.as_ref().unwrap().id, before.next[0].id);
        assert!(next.generation > before.generation);
        state.prev_track().unwrap();
        let previous = state.queue_snapshot(&mut tracker).unwrap();
        assert_eq!(previous.current.unwrap().id, before.current.unwrap().id);
        assert!(previous.generation > next.generation);
        assert!(state.queue_snapshot(&mut tracker).is_none());
    }
    #[test]
    fn navigation_publishes_repeat_one_cleared_and_context_retained() {
        for select in [false, true] {
            for repeat_context in [false, true] {
                let mut state = fixture();
                state.set_repeat_context(repeat_context);
                state.set_repeat_track(true);
                let mut tracker = crate::queue::QueueTracker::default();
                state.queue_snapshot(&mut tracker).unwrap();
                let mut observed = None;
                let mut events = Vec::new();
                state.publish_repeat(&mut observed, |context, track| {
                    events.push((context, track))
                });
                assert_eq!(events, [(repeat_context, true)]);
                if select {
                    crate::spirc::dispatch_selection(
                        &mut state,
                        tracker.generation,
                        QueueSelection {
                            generation: tracker.generation,
                            raw_next_index: 1,
                        },
                        |_| {},
                    )
                    .unwrap();
                } else {
                    state.next_track().unwrap();
                }
                state.publish_repeat(&mut observed, |context, track| {
                    events.push((context, track))
                });
                assert_eq!(events, [(repeat_context, true), (repeat_context, false)]);
                state.publish_repeat(&mut observed, |context, track| {
                    events.push((context, track))
                });
                assert_eq!(events.len(), 2);
            }
        }
    }

    #[test]
    fn rapid_second_selection_rejects_the_obsolete_snapshot() {
        let mut state = fixture();
        let mut tracker = crate::queue::QueueTracker::default();
        let before = state.queue_snapshot(&mut tracker).unwrap();
        let selection = QueueSelection {
            generation: before.generation,
            raw_next_index: 0,
        };
        state
            .select_queue_entry(tracker.generation, selection)
            .unwrap();
        state.queue_snapshot(&mut tracker).unwrap();
        let request = state.request.clone();
        assert_eq!(
            state.select_queue_entry(tracker.generation, selection),
            Err(QueueSelectionError::Stale)
        );
        assert_eq!(state.request, request);
    }
}
