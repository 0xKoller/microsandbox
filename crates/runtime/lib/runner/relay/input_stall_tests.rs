//! Regression coverage for recoverable console input stalls.

use std::time::Duration;

use super::*;

//--------------------------------------------------------------------------------------------------
// Functions
//--------------------------------------------------------------------------------------------------

fn blocked_write(shared: Arc<ConsoleSharedState>) -> tokio::task::JoinHandle<bool> {
    tokio::spawn(async move {
        #[cfg(unix)]
        let capacity = AsyncFd::new(shared.rx_capacity_wake.as_raw_fd()).unwrap();

        push_bulk_fragment_with_timeout(
            &shared,
            Bytes::from_static(b"next"),
            #[cfg(unix)]
            &capacity,
            Duration::from_millis(20),
        )
        .await
    })
}

//--------------------------------------------------------------------------------------------------
// Tests
//--------------------------------------------------------------------------------------------------

#[tokio::test]
async fn input_stall_retains_frame_and_recovers_on_capacity_notification() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let shared = Arc::new(ConsoleSharedState::with_capacity(4));
        shared.rx_ring.push(Bytes::from_static(b"full")).unwrap();
        let mut health = shared.input_stalled.subscribe();
        let started = tokio::time::Instant::now();
        let writer = blocked_write(Arc::clone(&shared));

        health.wait_for(|stalled| *stalled).await.unwrap();
        assert!(started.elapsed() >= Duration::from_millis(20));
        assert!(!writer.is_finished());
        assert!(input_is_stalled(&shared, None));

        // Dropping the popped fragment releases its byte charge; wake the
        // writer exactly as the console consumer does when capacity returns.
        assert_eq!(shared.rx_ring.pop().unwrap().as_ref(), b"full");
        shared.rx_capacity_wake.wake();

        assert!(writer.await.unwrap());
        assert_eq!(shared.rx_ring.pop().unwrap().as_ref(), b"next");
        assert!(!input_is_stalled(&shared, None));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn input_stall_shutdown_and_cancellation_release_the_health_gate() {
    for cancel in [false, true] {
        tokio::time::timeout(Duration::from_secs(2), async {
            let shared = Arc::new(ConsoleSharedState::with_capacity(4));
            shared.rx_ring.push(Bytes::from_static(b"full")).unwrap();
            let mut health = shared.input_stalled.subscribe();
            let writer = blocked_write(Arc::clone(&shared));

            health.wait_for(|stalled| *stalled).await.unwrap();

            if cancel {
                writer.abort();
                assert!(writer.await.unwrap_err().is_cancelled());
                // Wake the Windows blocking capacity waiter before its runtime exits.
                shared.close();
            } else {
                shared.close();
                assert!(!writer.await.unwrap());
            }

            assert!(!*health.borrow());
            assert_eq!(shared.rx_ring.pop().unwrap().as_ref(), b"full");
            assert!(shared.rx_ring.pop().is_none());
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn input_stall_on_bulk_lane_also_closes_client_admission() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let control = Arc::new(ConsoleSharedState::with_capacity(4));
        let bulk = Arc::new(ConsoleSharedState::with_capacity(4));
        bulk.rx_ring.push(Bytes::from_static(b"full")).unwrap();
        let writer = blocked_write(Arc::clone(&bulk));

        wait_for_input_stall(&control, Some(&bulk)).await;
        assert!(!input_is_stalled(&control, None));
        assert!(input_is_stalled(&control, Some(&bulk)));

        drop(bulk.rx_ring.pop());
        bulk.rx_capacity_wake.wake();

        assert!(writer.await.unwrap());
        assert!(!input_is_stalled(&control, Some(&bulk)));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn input_stall_temporary_backpressure_keeps_admission_open() {
    tokio::time::timeout(Duration::from_secs(2), async {
        let shared = Arc::new(ConsoleSharedState::with_capacity(4));
        shared.rx_ring.push(Bytes::from_static(b"full")).unwrap();
        let writer_shared = Arc::clone(&shared);
        let writer = tokio::spawn(async move {
            #[cfg(unix)]
            let capacity = AsyncFd::new(writer_shared.rx_capacity_wake.as_raw_fd()).unwrap();

            push_bulk_fragment_with_timeout(
                &writer_shared,
                Bytes::from_static(b"next"),
                #[cfg(unix)]
                &capacity,
                Duration::from_secs(60),
            )
            .await
        });

        tokio::task::yield_now().await;
        assert!(!input_is_stalled(&shared, None));
        drop(shared.rx_ring.pop());
        shared.rx_capacity_wake.wake();

        assert!(writer.await.unwrap());
        assert!(!input_is_stalled(&shared, None));
        assert_eq!(shared.rx_ring.pop().unwrap().as_ref(), b"next");
    })
    .await
    .unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn input_stall_admission_gate_preserves_existing_client_output() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let directory = tempfile::tempdir_in("/tmp").unwrap();
        let endpoint = directory.path().join("agent.sock");
        let shared = Arc::new(ConsoleSharedState::with_capacity(16 * 1024));
        let mut relay = AgentRelay::new(&endpoint, Arc::clone(&shared))
            .await
            .unwrap();
        relay.ready_frame = Some(tests::encoded_message(
            MessageType::Ready,
            &Ready::default(),
        ));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let (drain_tx, mut drain_rx) = mpsc::channel(1);

        let mut client = tokio::net::UnixStream::connect(&endpoint).await.unwrap();
        let run = tokio::spawn(relay.run(shutdown_rx, drain_tx));
        let mut range = [0; 8];
        client.read_exact(&mut range).await.unwrap();
        let id = u32::from_be_bytes(range[..4].try_into().unwrap());
        read_raw_frame(&mut client).await.unwrap();

        // Detection is exercised with real capacity waits in the tests above.
        // Set the lane health explicitly here to isolate the admission contract.
        shared.input_stalled.send_replace(true);
        let mut rejected = tokio::net::UnixStream::connect(&endpoint).await.unwrap();
        assert!(rejected.read_exact(&mut range).await.is_err());
        assert!(!run.is_finished());
        assert!(drain_rx.try_recv().is_err());

        shared
            .tx_ring
            .push(tests::encoded_message_id(MessageType::Pong, id, &()))
            .unwrap();
        shared.tx_wake.wake();
        let response = read_raw_frame(&mut client).await.unwrap();
        assert_eq!(
            decode_frame(response.data.as_ref()).unwrap().t,
            MessageType::Pong
        );

        shared.input_stalled.send_replace(false);
        let mut recovered = tokio::net::UnixStream::connect(&endpoint).await.unwrap();
        recovered.read_exact(&mut range).await.unwrap();
        read_raw_frame(&mut recovered).await.unwrap();

        drop(shutdown_tx);
        assert!(run.await.unwrap().is_ok());
    })
    .await
    .unwrap();
}
