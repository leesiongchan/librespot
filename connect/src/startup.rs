//! Bopify: the connect-time decision behind `ConnectConfig::startup_restore`,
//! and the mirror of another device's playback.

use crate::{
    playback::player::RemoteState,
    protocol::{connect::Cluster, playback::Playback, transfer_state::TransferState},
};
use protobuf::Message;

#[derive(Debug)]
pub(crate) enum StartupDecision {
    Remote(RemoteState),
    Restore(TransferState),
    Fallback,
}

pub(crate) fn startup_decision(cluster: &Cluster, own_device_id: &str) -> StartupDecision {
    if !cluster.active_device_id.is_empty() {
        // A trackless active device has nothing to mirror, and while any device is
        // active the transfer data is a stub that would pull its context over.
        return remote_playback(cluster, own_device_id)
            .map_or(StartupDecision::Fallback, StartupDecision::Remote);
    }
    match TransferState::parse_from_bytes(&cluster.transfer_data) {
        Ok(mut transfer) if is_usable(&transfer) => {
            // Set before handle_transfer, never as a pause after it: it also stops
            // handle_transfer extrapolating a stale timestamp past where playback was.
            transfer.playback.mut_or_insert_default().is_paused = Some(true);
            // handle_transfer rejects a missing context uri but restores a track-only session from "-".
            let context = transfer.current_session.mut_or_insert_default().context.mut_or_insert_default();
            context.uri.get_or_insert_with(|| "-".into());
            StartupDecision::Restore(transfer)
        }
        _ => StartupDecision::Fallback,
    }
}

/// What another active device is playing, or `None` when no other device is
/// active with a track. Reads `player_state`, because `transfer_data` is a
/// stub with an empty track while a device is active (run 4).
pub(crate) fn remote_playback(cluster: &Cluster, own_device_id: &str) -> Option<RemoteState> {
    let active = cluster.active_device_id.as_str();
    if active.is_empty() || active == own_device_id {
        return None;
    }
    let state = cluster.player_state.as_ref()?;
    let track_uri = state
        .track
        .as_ref()
        .map(|track| track.uri.clone())
        .filter(|uri| !uri.is_empty())?;
    let is_playing = state.is_playing && !state.is_paused;
    // Server clocks only, so a skewed Mac clock cannot move the mirror.
    let elapsed = if is_playing {
        cluster.server_timestamp_ms.saturating_sub(state.timestamp).max(0)
    } else {
        0
    };
    let mut position = state.position_as_of_timestamp.saturating_add(elapsed).max(0);
    if state.duration > 0 {
        position = position.min(state.duration);
    }
    Some(RemoteState {
        device_name: cluster
            .device
            .get(active)
            .map(|device| device.name.clone())
            .unwrap_or_default(),
        track_uri,
        is_playing,
        position_ms: u32::try_from(position).unwrap_or(u32::MAX),
        duration_ms: u32::try_from(state.duration.max(0)).unwrap_or(u32::MAX),
    })
}

/// The transfer's playback position, extrapolated forward while the reported
/// session was still playing: a session reported at 0:00 (just after a skip)
/// has still been playing since its timestamp.
pub(crate) fn transfer_position(playback: &Playback, now_ms: i64) -> i64 {
    let Some(position) = playback.position_as_of_timestamp else {
        return 0;
    };
    let position = i64::from(position);
    if playback.is_paused.unwrap_or_default() {
        return position;
    }
    match playback.timestamp {
        Some(timestamp) if timestamp > 0 => {
            position.saturating_add(now_ms.saturating_sub(timestamp)).max(0)
        }
        _ => position,
    }
}

/// Records `next` as the last emitted mirror, and says whether it differs,
/// so Spirc emits only changes.
pub(crate) fn remote_changed(last: &mut Option<RemoteState>, next: &Option<RemoteState>) -> bool {
    if last == next {
        return false;
    }
    last.clone_from(next);
    true
}

/// Checks content, not byte length: an empty Bopify session still serialises
/// to 66 bytes of transfer data.
fn is_usable(transfer: &TransferState) -> bool {
    let context = transfer.current_session.context.uri.as_deref().unwrap_or_default();
    let track = transfer.playback.current_track.uri.as_deref().unwrap_or_default();
    (!context.is_empty() && context != "-") || !track.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::connect::DeviceInfo;

    const OWN: &str = "own-device";

    /// A serialised last session, shaped like the spike's `transfer_data`.
    fn session(context: &str, track: &str, paused: bool) -> Vec<u8> {
        let mut transfer = TransferState::new();
        transfer
            .current_session
            .mut_or_insert_default()
            .context
            .mut_or_insert_default()
            .uri = Some(context.into());
        let playback = transfer.playback.mut_or_insert_default();
        playback.current_track.mut_or_insert_default().uri = Some(track.into());
        playback.is_paused = Some(paused);
        playback.position_as_of_timestamp = Some(203);
        transfer.write_to_bytes().unwrap()
    }

    fn cluster(active: &str, playing_track: &str, transfer_data: Vec<u8>) -> Cluster {
        let mut cluster = Cluster::new();
        cluster.active_device_id = active.into();
        if !active.is_empty() {
            let mut device = DeviceInfo::new();
            device.name = "iPhone".into();
            cluster.device.insert(active.into(), device);
        }
        cluster
            .player_state
            .mut_or_insert_default()
            .track
            .mut_or_insert_default()
            .uri = playing_track.into();
        cluster.transfer_data = transfer_data;
        cluster
    }

    /// `cluster`, with the active device reporting playback at a known server time.
    fn reporting(active: &str, paused: bool, server_timestamp_ms: i64) -> Cluster {
        let mut cluster = cluster(active, "spotify:track:0phqiLIH4JYk4jPissBOQE", Vec::new());
        let state = cluster.player_state.mut_or_insert_default();
        state.is_playing = true;
        state.is_paused = paused;
        state.timestamp = 1_000;
        state.position_as_of_timestamp = 5_000;
        state.duration = 200_000;
        cluster.server_timestamp_ms = server_timestamp_ms;
        cluster
    }

    fn restored(decision: StartupDecision) -> TransferState {
        match decision {
            StartupDecision::Restore(transfer) => transfer,
            other => panic!("expected Restore, got {other:?}"),
        }
    }

    #[test]
    fn restores_the_last_session_paused_when_no_device_is_active() {
        let data = session(
            "spotify:playlist:37i9dQZF1E8NDRt5R5oIbG",
            "spotify:track:3WvM2dIR9iIxMGNMP7WsNw",
            true,
        );
        let transfer = restored(startup_decision(&cluster("", "", data), OWN));
        assert_eq!(
            transfer.current_session.context.uri.as_deref(),
            Some("spotify:playlist:37i9dQZF1E8NDRt5R5oIbG")
        );
        assert_eq!(transfer.playback.is_paused, Some(true));
    }

    #[test]
    fn a_session_left_playing_by_a_vanished_device_resumes_paused_at_its_reported_position() {
        let data = session(
            "spotify:user:121047766:collection",
            "spotify:track:22Tmr1R5TVgtLOSYrgMWdu",
            false,
        );
        let transfer = restored(startup_decision(&cluster("", "", data), OWN));
        assert_eq!(transfer.playback.is_paused, Some(true));
        assert_eq!(transfer.playback.position_as_of_timestamp, Some(203));
    }

    #[test]
    fn a_session_started_from_bare_uris_is_restored_from_its_track() {
        restored(startup_decision(
            &cluster("", "", session("-", "spotify:track:x", true)),
            OWN,
        ));
    }

    #[test]
    fn an_empty_or_missing_session_falls_back() {
        for data in [
            session("", "", true),
            session("-", "", true),
            Vec::new(),
            vec![0xFF],
        ] {
            let decision = startup_decision(&cluster("", "", data), OWN);
            assert!(matches!(decision, StartupDecision::Fallback), "{decision:?}");
        }
    }

    #[test]
    fn another_device_playing_a_track_is_mirrored_never_restored() {
        // Run 4: while a device is active, transfer_data is a stub that still names a context.
        let data = session("spotify:user:121047766:collection", "", false);
        let decision = startup_decision(
            &cluster("6dce37", "spotify:track:0phqiLIH4JYk4jPissBOQE", data),
            OWN,
        );
        assert!(
            matches!(&decision, StartupDecision::Remote(remote) if remote.device_name == "iPhone"),
            "{decision:?}"
        );
    }

    #[test]
    fn an_active_device_without_a_track_falls_back_never_restores() {
        // Run 0: a crashed Bopify stayed "active" with an empty session.
        let data = session("spotify:user:121047766:collection", "", true);
        let decision = startup_decision(&cluster("ghost", "", data), OWN);
        assert!(matches!(decision, StartupDecision::Fallback), "{decision:?}");
    }

    #[test]
    fn a_track_without_a_context_uri_is_restored_as_a_track_only_session() {
        let mut transfer = TransferState::new();
        transfer
            .playback
            .mut_or_insert_default()
            .current_track
            .mut_or_insert_default()
            .uri = Some("spotify:track:x".into());
        let data = transfer.write_to_bytes().unwrap();
        let transfer = restored(startup_decision(&cluster("", "", data), OWN));
        assert_eq!(transfer.current_session.context.uri.as_deref(), Some("-"));
        assert_eq!(transfer.playback.is_paused, Some(true));
    }

    #[test]
    fn this_device_is_never_mirrored() {
        let decision = startup_decision(&cluster(OWN, "spotify:track:x", Vec::new()), OWN);
        assert!(matches!(decision, StartupDecision::Fallback), "{decision:?}");
    }

    #[test]
    fn a_playing_mirror_advances_by_server_time_only() {
        let remote = remote_playback(&reporting("6dce37", false, 3_500), OWN).unwrap();
        assert_eq!(
            remote,
            RemoteState {
                device_name: "iPhone".into(),
                track_uri: "spotify:track:0phqiLIH4JYk4jPissBOQE".into(),
                is_playing: true,
                position_ms: 7_500,
                duration_ms: 200_000,
            }
        );
    }

    #[test]
    fn a_paused_mirror_holds_its_reported_position() {
        let remote = remote_playback(&reporting("6dce37", true, 3_500), OWN).unwrap();
        assert!(!remote.is_playing);
        assert_eq!(remote.position_ms, 5_000);
    }

    #[test]
    fn a_mirrored_position_never_passes_the_track_end() {
        let remote = remote_playback(&reporting("6dce37", false, 900_000), OWN).unwrap();
        assert_eq!(remote.position_ms, 200_000);
    }

    #[test]
    fn nothing_is_mirrored_without_another_active_device_with_a_track() {
        for cluster in [
            cluster("", "spotify:track:x", Vec::new()),
            cluster(OWN, "spotify:track:x", Vec::new()),
            cluster("ghost", "", Vec::new()),
        ] {
            assert_eq!(remote_playback(&cluster, OWN), None);
        }
    }

    #[test]
    fn a_device_missing_from_the_device_list_is_mirrored_without_a_name() {
        let mut cluster = reporting("6dce37", false, 3_500);
        cluster.device.clear();
        assert_eq!(remote_playback(&cluster, OWN).unwrap().device_name, "");
    }

    #[test]
    fn only_a_changed_mirror_is_emitted() {
        let playing = remote_playback(&reporting("6dce37", false, 3_500), OWN);
        let mut last = None;
        assert!(remote_changed(&mut last, &playing));
        assert!(!remote_changed(&mut last, &playing));
        assert!(remote_changed(
            &mut last,
            &remote_playback(&reporting("6dce37", true, 3_500), OWN)
        ));
        assert!(remote_changed(&mut last, &None));
        assert!(!remote_changed(&mut last, &None));
    }

    #[test]
    fn a_paused_transfer_keeps_its_reported_position() {
        let playback = Playback {
            position_as_of_timestamp: Some(68_188),
            timestamp: Some(1_000_000),
            is_paused: Some(true),
            ..Default::default()
        };
        assert_eq!(transfer_position(&playback, 1_500_000), 68_188);
    }

    #[test]
    fn a_playing_transfer_reported_at_zero_still_counts_time_since_its_report() {
        let playback = Playback {
            position_as_of_timestamp: Some(0),
            timestamp: Some(1_000_000),
            is_paused: Some(false),
            ..Default::default()
        };
        assert_eq!(transfer_position(&playback, 1_000_000 + 79_000), 79_000);
    }

    #[test]
    fn a_playing_transfer_extrapolates_from_its_reported_position() {
        let playback = Playback {
            position_as_of_timestamp: Some(68_188),
            timestamp: Some(1_000_000),
            is_paused: Some(false),
            ..Default::default()
        };
        assert_eq!(transfer_position(&playback, 1_000_500), 68_688);
    }

    #[test]
    fn a_playing_transfer_never_goes_negative_on_clock_skew() {
        let playback = Playback {
            position_as_of_timestamp: Some(1_000),
            timestamp: Some(1_000_000),
            is_paused: Some(false),
            ..Default::default()
        };
        assert_eq!(transfer_position(&playback, 1_000_000 - 5_000), 0);
    }

    #[test]
    fn a_playing_transfer_without_a_timestamp_keeps_its_reported_position() {
        for timestamp in [None, Some(0)] {
            let playback = Playback {
                position_as_of_timestamp: Some(68_188),
                timestamp,
                is_paused: Some(false),
                ..Default::default()
            };
            assert_eq!(transfer_position(&playback, 1_000_000), 68_188);
        }
    }

    #[test]
    fn a_transfer_without_a_position_starts_at_zero() {
        let playback = Playback::default();
        assert_eq!(transfer_position(&playback, 1_000_000), 0);
    }
}
