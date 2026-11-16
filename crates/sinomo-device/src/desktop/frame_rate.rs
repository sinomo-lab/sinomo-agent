//! Single-owner admission ceiling. No frame clock is advanced while idle.
use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

pub struct FrameRateCeiling {
    limit: Option<usize>,
    admitted: VecDeque<Instant>,
}
impl FrameRateCeiling {
    pub fn new(limit: Option<u32>) -> Self {
        Self {
            limit: limit.filter(|rate| *rate > 0).map(|rate| rate as usize),
            admitted: VecDeque::new(),
        }
    }
    pub fn delay(&mut self, now: Instant) -> Duration {
        while self
            .admitted
            .front()
            .is_some_and(|at| now.saturating_duration_since(*at) >= Duration::from_secs(1))
        {
            self.admitted.pop_front();
        }
        match self.limit {
            Some(limit) if self.admitted.len() >= limit => {
                (self.admitted[0] + Duration::from_secs(1)).saturating_duration_since(now)
            }
            _ => Duration::ZERO,
        }
    }
    pub fn admit(&mut self, now: Instant) {
        debug_assert!(self.delay(now).is_zero());
        if self.limit.is_some() {
            self.admitted.push_back(now);
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn irregular_frames_are_immediate_until_the_rolling_ceiling_is_exhausted() {
        let start = Instant::now();
        let mut ceiling = FrameRateCeiling::new(Some(3));
        for ms in [0, 10, 700] {
            let now = start + Duration::from_millis(ms);
            assert!(ceiling.delay(now).is_zero());
            ceiling.admit(now);
        }
        assert_eq!(
            ceiling.delay(start + Duration::from_millis(800)),
            Duration::from_millis(200)
        );
        assert!(ceiling.delay(start + Duration::from_millis(1000)).is_zero());
        ceiling.admit(start + Duration::from_millis(1000));
        assert_eq!(
            ceiling.delay(start + Duration::from_millis(1001)),
            Duration::from_millis(9)
        );
        assert!(ceiling.delay(start + Duration::from_secs(10)).is_zero());
    }
    #[test]
    fn unlimited_sources_keep_no_admission_history() {
        let mut ceiling = FrameRateCeiling::new(None);
        let now = Instant::now();
        for _ in 0..1000 {
            assert!(ceiling.delay(now).is_zero());
            ceiling.admit(now);
        }
        assert!(ceiling.admitted.is_empty());
    }
}
