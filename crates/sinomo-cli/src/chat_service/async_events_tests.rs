use super::*;

fn request(after_sequence: u64, max_events: usize, wait_ms: u64) -> wire::ChatSessionEventsRequest {
    wire::ChatSessionEventsRequest {
        client_id: "client".into(),
        after_sequence,
        max_events,
        wait_ms,
    }
}

fn publish(log: &ChatEventLog) {
    log.push(wire::ChatEvent::Notice {
        session_id: "session".into(),
        notice: "test".into(),
    });
}

#[tokio::test]
async fn asynchronous_reads_preserve_paging_cursor_and_generation() {
    let log = ChatEventLog::new();
    for _ in 0..3 {
        publish(&log);
    }
    for (cursor, limit) in [(0, 0), (1, 1), (0, usize::MAX), (3, 2)] {
        let request = request(cursor, limit, 0);
        let blocking = log.poll(&request);
        let asynchronous = log.poll_async(&request).await;
        assert_eq!(asynchronous.next_sequence, blocking.next_sequence);
        assert_eq!(
            asynchronous.first_available_sequence,
            blocking.first_available_sequence
        );
        assert_eq!(asynchronous.generation, blocking.generation);
        assert_eq!(
            asynchronous.execution_available,
            blocking.execution_available
        );
        assert_eq!(
            asynchronous
                .events
                .iter()
                .map(|e| e.sequence)
                .collect::<Vec<_>>(),
            blocking
                .events
                .iter()
                .map(|e| e.sequence)
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn pending_readers_do_not_occupy_blocking_workers_and_all_wake_on_publication() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let log = ChatEventLog::new();
        let mut readers = Vec::new();
        for _ in 0..16 {
            let log = log.clone();
            readers.push(tokio::spawn(async move {
                log.poll_async(&request(0, 10, 5000)).await
            }));
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(readers.iter().all(|reader| !reader.is_finished()));
        tokio::time::timeout(
            Duration::from_millis(300),
            tokio::task::spawn_blocking(|| ()),
        )
        .await
        .expect("event readers exhausted blocking pool")
        .unwrap();
        publish(&log);
        for reader in readers {
            let response = tokio::time::timeout(Duration::from_millis(300), reader)
                .await
                .expect("publication did not wake reader")
                .unwrap();
            assert_eq!(response.events.len(), 1);
            assert_eq!(response.next_sequence, 1);
        }
    });
}

#[tokio::test]
async fn cancelling_an_idle_reader_does_not_lose_future_publications() {
    let log = ChatEventLog::new();
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            log.poll_async(&request(0, 10, 5000))
        )
        .await
        .is_err()
    );
    let reader = {
        let log = log.clone();
        tokio::spawn(async move { log.poll_async(&request(0, 10, 5000)).await })
    };
    publish(&log);
    let response = tokio::time::timeout(Duration::from_millis(300), reader)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.next_sequence, 1);
}
