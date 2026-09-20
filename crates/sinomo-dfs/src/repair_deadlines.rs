//! Weighted expiry index for read-only repair status projections.
//!
//! Updates and suffix queries visit at most 65 nodes, independently of the
//! number of captures. Expiry changes observations without a metadata write.

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct RepairDeadlines {
    total: usize,
    children: [Option<std::sync::Arc<RepairDeadlines>>; 2],
}

impl RepairDeadlines {
    pub(crate) fn replace(&mut self, deadline: u64, before: usize, after: usize) {
        self.replace_at(deadline, before, after, 64);
    }

    fn replace_at(&mut self, deadline: u64, before: usize, after: usize, remaining: u32) {
        debug_assert!(self.total >= before);
        self.total = self.total.saturating_sub(before).saturating_add(after);
        if remaining == 0 || before == after {
            return;
        }
        let child = &mut self.children[((deadline >> (remaining - 1)) & 1) as usize];
        let node = std::sync::Arc::make_mut(child.get_or_insert_with(Default::default));
        node.replace_at(deadline, before, after, remaining - 1);
        if node.total == 0 {
            *child = None;
        }
    }

    pub(crate) fn count_after(&self, now: u64) -> usize {
        let mut count = 0usize;
        let mut node = self;
        for bit in (0..64).rev() {
            let side = ((now >> bit) & 1) as usize;
            if side == 0 {
                count =
                    count.saturating_add(node.children[1].as_ref().map_or(0, |next| next.total));
            }
            let Some(next) = node.children[side].as_deref() else {
                break;
            };
            node = next;
        }
        count
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.total == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn weighted_expiry_queries_match_ordered_reference_after_replacements() {
        let mut index = RepairDeadlines::default();
        let mut expected = std::collections::BTreeMap::<u64, usize>::new();
        for (deadline, weight) in [
            (0, 2),
            (u64::MAX, 3),
            (50, 4),
            (25, 2),
            (50, 1),
            (0, 0),
            (25, 0),
        ] {
            let snapshot = index.clone();
            let old_count = snapshot.count_after(0);
            let before = expected.insert(deadline, weight).unwrap_or(0);
            index.replace(deadline, before, weight);
            assert_eq!(snapshot.count_after(0), old_count);
            for now in [0, 1, 24, 25, 49, 50, 51, u64::MAX - 1, u64::MAX] {
                assert_eq!(
                    index.count_after(now),
                    expected
                        .range((std::ops::Bound::Excluded(now), std::ops::Bound::Unbounded))
                        .map(|(_, weight)| weight)
                        .sum::<usize>()
                );
            }
        }
        for (deadline, weight) in expected {
            index.replace(deadline, weight, 0);
        }
        assert!(index.is_empty());
        assert_eq!(index.children, [None, None]);
    }
}
