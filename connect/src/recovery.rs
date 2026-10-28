pub use crate::playback::player::RecoveryIdentity;
use crate::{
    protocol::{connect::Cluster, player::PlayerState, transfer_state::TransferState},
    startup::{StartupDecision, startup_decision},
};

/// Startup policy chosen by the owner of the Connect session.
#[derive(Debug, Clone, Default)]
pub enum StartupMode {
    #[default]
    /// Keep the existing paused restore and fallback policy.
    ColdLaunch,
    /// Restore available native state without taking another device over.
    Recovery(Option<RecoveryCheckpoint>),
}

/// Local intent captured before replacing a failed engine. Contains no credentials.
#[derive(Debug, Clone, Default)]
pub struct RecoveryCheckpoint {
    /// Whether the interrupted engine owned local playback.
    pub was_active: bool,
    /// Last confirmed local playing intent.
    pub should_resume: bool,
    /// Native occurrence required to authorize seek or resume.
    pub identity: Option<RecoveryIdentity>,
    /// Frozen interruption position, without elapsed outage time.
    pub position_ms: i64,
    /// Local output volume, including zero for silence.
    pub volume: f64,
    /// Fallback used only when server options are absent.
    pub shuffle_enabled: bool,
    /// Fallback repeat option: off, context, or track.
    pub repeat_mode: String,
}

pub(crate) enum RecoveryDecision {
    Foreign,
    Transfer(TransferState),
    OwnState(Box<PlayerState>),
    NoSession,
}

pub(crate) fn recovery_decision(cluster: &Cluster, own: &str) -> RecoveryDecision {
    if !cluster.active_device_id.is_empty() {
        if cluster.active_device_id != own {
            return RecoveryDecision::Foreign;
        }
        return cluster
            .player_state
            .as_ref()
            .filter(|state| !state.track.uri.is_empty())
            .cloned()
            .map(Box::new)
            .map_or(RecoveryDecision::NoSession, RecoveryDecision::OwnState);
    }
    match startup_decision(cluster, own) {
        StartupDecision::Restore(transfer) => RecoveryDecision::Transfer(transfer),
        _ => RecoveryDecision::NoSession,
    }
}

impl RecoveryCheckpoint {
    pub(crate) fn matches(&self, identity: &RecoveryIdentity) -> bool {
        self.was_active
            && self.identity.as_ref().is_some_and(|saved| {
                saved.track_uri == identity.track_uri
                    && saved
                        .track_uid
                        .as_ref()
                        .filter(|uid| !uid.is_empty())
                        .is_none_or(|uid| identity.track_uid.as_ref() == Some(uid))
                    && saved
                        .context_uri
                        .as_ref()
                        .filter(|uri| !uri.is_empty())
                        .is_none_or(|uri| identity.context_uri.as_ref() == Some(uri))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{
        connect::Cluster,
        player::{PlayerState, ProvidedTrack},
    };
    use protobuf::MessageField;

    #[test]
    fn recovery_foreign_trackless_device_never_falls_back() {
        let mut cluster = Cluster::new();
        cluster.active_device_id = "foreign".into();
        assert!(matches!(
            recovery_decision(&cluster, "own"),
            RecoveryDecision::Foreign
        ));
    }

    #[test]
    fn recovery_own_id_uses_raw_player_state_not_transfer_stub() {
        let mut cluster = Cluster::new();
        cluster.active_device_id = "own".into();
        cluster.player_state = MessageField::some(PlayerState {
            track: MessageField::some(ProvidedTrack {
                uri: "spotify:track:x".into(),
                uid: "occurrence2".into(),
                ..Default::default()
            }),
            ..Default::default()
        });
        assert!(
            matches!(recovery_decision(&cluster, "own"), RecoveryDecision::OwnState(state) if state.track.uid == "occurrence2")
        );
    }

    #[test]
    fn recovery_missing_or_malformed_session_is_unavailable() {
        for data in [vec![], vec![0xff]] {
            let mut cluster = Cluster::new();
            cluster.transfer_data = data;
            assert!(matches!(
                recovery_decision(&cluster, "own"),
                RecoveryDecision::NoSession
            ));
        }
    }
}
