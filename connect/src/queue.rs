use crate::{
    playback::player::QueueTrack, protocol::player::ProvidedTrack, state::provider::IsProvider,
};

#[derive(Clone, Copy, Debug)]
pub(crate) enum QueueMovement {
    Forward(usize),
    Backward,
}

#[derive(Clone, Debug)]
struct Entry {
    track: ProvidedTrack,
    id: u64,
}

#[derive(Default)]
pub(crate) struct QueueTracker {
    pub generation: u64,
    serial: u64,
    previous: Vec<Entry>,
    current: Option<Entry>,
    next: Vec<Entry>,
}

pub(crate) struct Snapshot {
    pub generation: u64,
    pub previous: Vec<QueueTrack>,
    pub current: Option<QueueTrack>,
    pub next: Vec<QueueTrack>,
}

impl crate::state::ConnectState {
    pub(crate) fn publish_repeat(
        &self,
        observed: &mut Option<(bool, bool)>,
        publish: impl FnOnce(bool, bool),
    ) {
        let current = (self.repeat_context(), self.repeat_track());
        if *observed != Some(current) {
            *observed = Some(current);
            publish(current.0, current.1);
        }
    }

    pub(crate) fn queue_snapshot(&mut self, tracker: &mut QueueTracker) -> Option<Snapshot> {
        let movement = self.queue_movement.take();
        let replaced = std::mem::take(&mut self.queue_replaced);
        let state = self.player();
        tracker.update(
            &state.prev_tracks,
            state.track.as_ref(),
            &state.next_tracks,
            movement,
            replaced,
        )
    }
}

impl QueueTracker {
    pub fn update(
        &mut self,
        previous: &[ProvidedTrack],
        current: Option<&ProvidedTrack>,
        next: &[ProvidedTrack],
        movement: Option<QueueMovement>,
        replaced: bool,
    ) -> Option<Snapshot> {
        if !replaced
            && movement.is_none()
            && self.generation > 0
            && same(&self.previous, previous)
            && same(&self.next, next)
            && self.current.as_ref().map(|e| &e.track) == current
        {
            return None;
        }
        self.generation += 1;
        let old: Vec<_> = self
            .previous
            .iter()
            .chain(self.current.iter())
            .chain(self.next.iter())
            .cloned()
            .collect();
        let all: Vec<_> = previous.iter().chain(current).chain(next.iter()).collect();
        let mut expected_previous = self.previous.clone();
        let mut expected_current = self.current.clone();
        let mut expected_next = self.next.clone();
        if !replaced {
            match movement {
                Some(QueueMovement::Forward(index)) if index < self.next.len() => {
                    expected_previous.extend(
                        self.current
                            .iter()
                            .chain(self.next[..index].iter())
                            .filter(|e| e.track.is_context() || e.track.is_autoplay())
                            .cloned(),
                    );
                    expected_current = Some(self.next[index].clone());
                    expected_next = self.next[index + 1..].to_vec();
                }
                Some(QueueMovement::Backward) => {
                    if let Some(e) = expected_previous.last() {
                        if e.track.uid.starts_with("delimiter") {
                            expected_next.insert(0, expected_previous.pop().unwrap());
                        }
                    }
                    if let Some(e) = &self.current {
                        if e.track.is_context() || e.track.is_autoplay() {
                            expected_next.insert(0, e.clone());
                        }
                    }
                    expected_current = expected_previous.pop();
                }
                _ => {}
            }
        }
        let mut used = std::collections::HashSet::new();
        let mut assign = |track: &ProvidedTrack, expected: Option<&Entry>| {
            let candidate = if !replaced {
                expected.filter(|e| &e.track == track)
            } else {
                None
            };
            let candidate = candidate.or_else(|| {
                let mut matches = old.iter().filter(|e| &e.track == track);
                let first = matches.next()?;
                if matches.next().is_none() && all.iter().filter(|t| ***t == *track).count() == 1 {
                    Some(first)
                } else {
                    None
                }
            });
            let id = match candidate.filter(|e| !used.contains(&e.id)) {
                Some(e) => e.id,
                None => {
                    self.serial += 1;
                    self.serial
                }
            };
            used.insert(id);
            Entry {
                track: track.clone(),
                id,
            }
        };
        // History is capped by the provider, so align its retained suffix.
        let offset = expected_previous.len().saturating_sub(previous.len());
        let previous: Vec<_> = previous
            .iter()
            .enumerate()
            .map(|(i, t)| assign(t, expected_previous.get(i + offset)))
            .collect();
        let current = current.map(|t| assign(t, expected_current.as_ref()));
        let next: Vec<_> = next
            .iter()
            .enumerate()
            .map(|(i, t)| assign(t, expected_next.get(i)))
            .collect();
        self.previous = previous;
        self.current = current;
        self.next = next;
        Some(Snapshot {
            generation: self.generation,
            previous: self.previous.iter().map(|e| export(e, None)).collect(),
            current: self.current.as_ref().map(|e| export(e, None)),
            next: self
                .next
                .iter()
                .enumerate()
                .map(|(i, e)| export(e, Some(i as u32)))
                .collect(),
        })
    }
}

fn same(entries: &[Entry], tracks: &[ProvidedTrack]) -> bool {
    entries.len() == tracks.len() && entries.iter().zip(tracks).all(|(e, t)| &e.track == t)
}

fn export(entry: &Entry, raw_next_index: Option<u32>) -> QueueTrack {
    QueueTrack {
        uri: entry.track.uri.clone(),
        provider: entry.track.provider.clone(),
        uid: entry.track.uid.clone(),
        id: entry.id,
        raw_next_index,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn song() -> ProvidedTrack {
        ProvidedTrack {
            uri: "spotify:track:same".into(),
            uid: "duplicate".into(),
            provider: "context".into(),
            ..Default::default()
        }
    }
    #[test]
    fn forward_keeps_exact_duplicate_identity_and_suffix() {
        let mut tracker = QueueTracker::default();
        let old = tracker
            .update(&[], Some(&song()), &[song(), song(), song()], None, false)
            .unwrap();
        let new = tracker
            .update(
                &[song(), song()],
                Some(&song()),
                &[song()],
                Some(QueueMovement::Forward(1)),
                false,
            )
            .unwrap();
        assert_eq!(new.current.as_ref().unwrap().id, old.next[1].id);
        assert_eq!(new.next[0].id, old.next[2].id);
        assert_eq!(new.previous[0].id, old.current.unwrap().id);
        assert_ne!(old.next[0].id, old.next[1].id);
        assert!(new.generation > old.generation);
    }
    #[test]
    fn external_identical_duplicates_invalidate_generation_and_identity() {
        let mut tracker = QueueTracker::default();
        let old = tracker
            .update(&[], Some(&song()), &[song(), song()], None, false)
            .unwrap();
        let new = tracker
            .update(&[], Some(&song()), &[song(), song()], None, true)
            .unwrap();
        assert!(new.generation > old.generation);
        assert_ne!(new.current.unwrap().id, old.current.unwrap().id);
        assert_ne!(new.next[0].id, old.next[0].id);
    }
    #[test]
    fn unchanged_snapshot_does_not_invalidate_clicks() {
        let mut tracker = QueueTracker::default();
        tracker
            .update(&[], Some(&song()), &[song()], None, false)
            .unwrap();
        assert!(
            tracker
                .update(&[], Some(&song()), &[song()], None, false)
                .is_none()
        );
    }
    #[test]
    fn previous_reverses_known_movement() {
        let mut tracker = QueueTracker::default();
        let old = tracker
            .update(&[song()], Some(&song()), &[song()], None, false)
            .unwrap();
        let new = tracker
            .update(
                &[],
                Some(&song()),
                &[song(), song()],
                Some(QueueMovement::Backward),
                false,
            )
            .unwrap();
        assert_eq!(new.current.unwrap().id, old.previous[0].id);
        assert_eq!(new.next[0].id, old.current.unwrap().id);
    }
}
