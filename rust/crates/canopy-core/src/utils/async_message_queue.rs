use std::collections::VecDeque;

/// A generic non-blocking FIFO message queue.
///
/// Items enqueued after [`drain`](Self::drain) are silently dropped. Dequeue
/// returns `None` when the queue is empty.
pub struct AsyncMessageQueue<T> {
    items: VecDeque<T>,
    drained: bool,
}

impl<T> Default for AsyncMessageQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> AsyncMessageQueue<T> {
    pub fn new() -> Self {
        Self {
            items: VecDeque::new(),
            drained: false,
        }
    }

    /// Add an item unless the queue has been drained.
    pub fn enqueue(&mut self, item: T) {
        if !self.drained {
            self.items.push_back(item);
        }
    }

    /// Remove and return the next item, or `None` when empty.
    pub fn dequeue(&mut self) -> Option<T> {
        self.items.pop_front()
    }

    /// Signal that no more items will be enqueued.
    pub fn drain(&mut self) {
        self.drained = true;
    }

    /// Number of items currently in the queue.
    pub fn size(&self) -> usize {
        self.items.len()
    }

    /// Whether [`drain`](Self::drain) has been called.
    pub fn is_drained(&self) -> bool {
        self.drained
    }
}

#[cfg(test)]
mod tests {
    use super::AsyncMessageQueue;

    #[test]
    fn dequeues_items_in_fifo_order() {
        let mut queue = AsyncMessageQueue::new();
        queue.enqueue("a");
        queue.enqueue("b");
        queue.enqueue("c");

        assert_eq!(queue.dequeue(), Some("a"));
        assert_eq!(queue.dequeue(), Some("b"));
        assert_eq!(queue.dequeue(), Some("c"));
    }

    #[test]
    fn returns_none_when_empty() {
        let mut queue: AsyncMessageQueue<String> = AsyncMessageQueue::new();
        assert_eq!(queue.dequeue(), None);
    }

    #[test]
    fn returns_remaining_items_then_none_after_drain() {
        let mut queue = AsyncMessageQueue::new();
        queue.enqueue("x");
        queue.enqueue("y");

        queue.drain();

        assert_eq!(queue.dequeue(), Some("x"));
        assert_eq!(queue.dequeue(), Some("y"));
        assert_eq!(queue.dequeue(), None);
    }

    #[test]
    fn silently_drops_items_enqueued_after_drain() {
        let mut queue = AsyncMessageQueue::new();
        queue.drain();
        queue.enqueue("dropped");

        assert_eq!(queue.size(), 0);
    }

    #[test]
    fn tracks_size_accurately() {
        let mut queue = AsyncMessageQueue::new();
        assert_eq!(queue.size(), 0);

        queue.enqueue(1);
        queue.enqueue(2);
        assert_eq!(queue.size(), 2);

        queue.dequeue();
        assert_eq!(queue.size(), 1);
    }

    #[test]
    fn reports_is_drained_correctly() {
        let mut queue = AsyncMessageQueue::<i32>::new();
        assert!(!queue.is_drained());

        queue.drain();
        assert!(queue.is_drained());
    }

    #[test]
    fn handles_multiple_sequential_enqueue_dequeue_cycles() {
        let mut queue = AsyncMessageQueue::new();

        for item in 0..5 {
            queue.enqueue(item);
            assert_eq!(queue.dequeue(), Some(item));
        }
    }
}
