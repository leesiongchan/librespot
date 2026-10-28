use super::{ConnectState, context::ContextType};
use crate::{
    core::{Error, SpotifyUri},
    protocol::player::PlayerState,
};

impl ConnectState {
    pub fn restore_recovery_player_state(&mut self, mut native: PlayerState) -> Result<(), Error> {
        SpotifyUri::from_uri(&native.track.uri)?;
        // Device/session bookkeeping belongs to the replacement, not the vanished runtime.
        native.session_id = self.player().session_id.clone();
        self.active_context = ContextType::for_track(&native.track);
        self.fill_up_context = self.active_context;
        if let (Some(index), Ok(context)) = (
            native.index.as_ref(),
            self.get_context_mut(self.active_context),
        ) {
            context.index = index.clone();
        }
        self.queue_count = native
            .prev_tracks
            .iter()
            .chain(native.next_tracks.iter())
            .chain(native.track.iter())
            .filter_map(|track| {
                track
                    .uid
                    .strip_prefix('q')
                    .and_then(|n| n.parse::<u64>().ok())
            })
            .max()
            .map_or(0, |n| n.saturating_add(1))
            .max(self.queue_count);
        *self.player_mut() = native;
        self.preserve_recovery_queue = true;
        self.update_queue_revision();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ConnectConfig,
        core::{Session, SessionConfig},
        protocol::player::{ContextIndex, PlayerState, ProvidedTrack},
    };
    use protobuf::MessageField;

    #[tokio::test]
    async fn recovery_autoplay_provider_and_delayed_pages_preserve_live_queue() {
        use crate::{
            context_resolver::{ContextAction, ContextResolver, ResolveContext},
            protocol::{context::Context, context_page::ContextPage, context_track::ContextTrack},
        };
        let session = Session::new(SessionConfig::default(), None);
        let mut state = ConnectState::new(ConnectConfig::default(), &session);
        let uri = "spotify:track:6rqhFgbbKwnb9MLmUQDhG6";
        let native = PlayerState {
            track: MessageField::some(ProvidedTrack {
                uri: uri.into(),
                uid: "second".into(),
                provider: "autoplay".into(),
                ..Default::default()
            }),
            next_tracks: vec![ProvidedTrack {
                uri: uri.into(),
                uid: "q5".into(),
                provider: "queue".into(),
                ..Default::default()
            }],
            context_uri: "spotify:playlist:37i9dQZF1E8NDRt5R5oIbG".into(),
            is_playing: true,
            is_paused: false,
            ..Default::default()
        };
        state.restore_recovery_player_state(native.clone()).unwrap();
        assert_eq!(state.active_context, ContextType::Autoplay);
        let context = Context {
            uri: Some(native.context_uri.clone()),
            pages: vec![ContextPage {
                tracks: vec![
                    ContextTrack {
                        uri: Some(uri.into()),
                        uid: Some("first".into()),
                        ..Default::default()
                    },
                    ContextTrack {
                        uri: Some(uri.into()),
                        uid: Some("second".into()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut resolver = ContextResolver::new(session);
        resolver.add(ResolveContext::from_context(
            context.clone(),
            ContextType::Autoplay,
            ContextAction::Replace,
        ));
        resolver.apply_next_context(&mut state, context).unwrap();
        assert_eq!(state.player().track, native.track);
        assert_eq!(state.player().next_tracks, native.next_tracks);
        assert!(state.player().is_playing);
        assert!(!state.player().is_paused);
    }

    #[tokio::test]
    async fn recovery_preserves_raw_occurrences_options_and_replacement_session() {
        let session = Session::new(SessionConfig::default(), None);
        let mut state = ConnectState::new(ConnectConfig::default(), &session);
        let session_id = state.player().session_id.clone();
        let mut native = PlayerState::new();
        native.session_id = "old".into();
        native.context_uri = "spotify:playlist:p".into();
        native.index = MessageField::some(ContextIndex {
            track: 12,
            ..Default::default()
        });
        native.track = MessageField::some(ProvidedTrack {
            uri: "spotify:track:6rqhFgbbKwnb9MLmUQDhG6".into(),
            uid: "second".into(),
            provider: "context".into(),
            ..Default::default()
        });
        native.prev_tracks.push(ProvidedTrack {
            uri: "spotify:track:6rqhFgbbKwnb9MLmUQDhG6".into(),
            uid: "first".into(),
            provider: "context".into(),
            ..Default::default()
        });
        native.next_tracks.push(ProvidedTrack {
            uri: "".into(),
            uid: "delimiter7".into(),
            provider: "queue".into(),
            ..Default::default()
        });
        native.next_tracks.push(ProvidedTrack {
            uri: "spotify:track:6rqhFgbbKwnb9MLmUQDhG6".into(),
            uid: "q7".into(),
            provider: "queue".into(),
            ..Default::default()
        });
        state.restore_recovery_player_state(native.clone()).unwrap();
        assert_eq!(state.player().track, native.track);
        assert_eq!(state.player().prev_tracks, native.prev_tracks);
        assert_eq!(state.player().next_tracks, native.next_tracks);
        assert_eq!(state.player().index, native.index);
        assert_eq!(state.player().options, native.options);
        assert_eq!(state.player().session_id, session_id);
        state.add_to_queue(
            ProvidedTrack {
                uri: "spotify:track:6rqhFgbbKwnb9MLmUQDhG6".into(),
                ..Default::default()
            },
            false,
        );
        assert!(
            state
                .player()
                .next_tracks
                .iter()
                .any(|track| track.uid == "q8")
        );
    }
}
