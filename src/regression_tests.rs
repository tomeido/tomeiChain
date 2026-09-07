//! Regression coverage for public-node resource and chain validation boundaries.

use crate::*;

#[test]
fn rejects_ambiguous_hash_fields_and_excessive_metadata() {
    for (namespace, tags, signature) in [
        ("ns\0other", vec![], None),
        ("ns", vec![("key\0other".into(), "value".into())], None),
        ("ns", vec![("key".into(), "value\0other".into())], None),
        ("ns", vec![("key".into(), "value".into()); 65], None),
        (
            "ns",
            vec![("key".into(), "x".repeat(MAX_METADATA_BYTES))],
            None,
        ),
        ("ns", vec![], Some("x".repeat(MAX_METADATA_BYTES + 1))),
    ] {
        assert!(matches!(
            Transaction::new(namespace.into(), "payload".into(), signature, tags),
            Err(ChainError::InvalidTx(_))
        ));
    }
}

#[tokio::test]
async fn full_byte_budget_does_not_claim_tags_and_recovers_after_capacity_returns() {
    let (chain, dir) = tests::test_chain();
    chain.mempool.lock().await.bytes = MAX_MEMPOOL_BYTES;
    let tags = vec![("story".to_string(), "resource-regression".to_string())];
    let (_, result) = chain
        .submit_tx("ns".into(), "small payload".into(), None, tags.clone())
        .await
        .unwrap();
    assert!(matches!(result, SubmitOutcome::PoolFull));
    {
        let mut pool = chain.mempool.lock().await;
        assert!(pool.queue.is_empty());
        assert!(pool.ids.is_empty());
        assert!(pool.pending_tags.is_empty());
        pool.bytes = 0;
    }
    let (id, result) = chain
        .submit_tx("ns".into(), "small payload".into(), None, tags)
        .await
        .unwrap();
    assert!(matches!(result, SubmitOutcome::Queued));
    assert!(chain.mempool.lock().await.bytes > 0);
    chain.seal_pending_block().await.unwrap().unwrap();
    assert_eq!(chain.store.tx_height(&id).unwrap(), Some(1));
    let pool = chain.mempool.lock().await;
    assert_eq!(pool.bytes, 0);
    assert!(pool.ids.is_empty());
    assert!(pool.pending_tags.is_empty());
    drop(pool);
    drop(chain);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn verification_rejects_self_consistent_replacement_genesis() {
    let dir = tests::temp_dir("genesis-regression");
    let store = Store::open(&dir).unwrap();
    let altered = Block::with_timestamp(0, "0".repeat(64), vec![], 1).unwrap();
    altered.verify_integrity().unwrap();
    store
        .commit_block(
            &altered,
            &BlockchainState {
                height: 0,
                tip_hash: altered.hash.clone(),
                total_txs: 0,
            },
        )
        .unwrap();
    assert!(!store.verify_chain(0, 0).unwrap().ok);
    drop(store);
    assert!(Chain::bootstrap(&tests::test_config(dir.clone())).is_err());
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn verification_rejects_valid_hash_stored_at_wrong_height() {
    let dir = tests::temp_dir("height-regression");
    let store = Store::open(&dir).unwrap();
    let (state, _) = store.init_or_resume().unwrap();
    let wrong_height = Block::with_timestamp(7, state.tip_hash, vec![], 1).unwrap();
    wrong_height.verify_integrity().unwrap();
    store
        .blocks
        .insert(
            1u64.to_be_bytes(),
            bincode::serialize(&wrong_height).unwrap(),
        )
        .unwrap();
    assert!(!store.verify_chain(0, 1).unwrap().ok);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn follower_guards_reject_submission_and_sealing() {
    let (chain, dir) = tests::test_chain();
    let mut chain = Arc::try_unwrap(chain)
        .ok()
        .expect("only test owns the chain");
    chain.network.config.role = network::Role::Follower;
    assert!(chain
        .submit_tx("ns".into(), "must not be sequenced".into(), None, vec![])
        .await
        .is_err());
    assert!(chain.seal_pending_block().await.unwrap().is_none());
    assert_eq!(chain.snapshot().await.0.height, 0);
    assert!(chain.mempool.lock().await.queue.is_empty());
    let (code, _) = route(&chain, "POST", "/api/blocks", None, b"[]").await;
    assert_eq!(code, 403);
    drop(chain);
    std::fs::remove_dir_all(dir).unwrap();
}
