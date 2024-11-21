use anyhow::{Context, Result};
use config::Import as _;
use config::{Committee, KeyPair, Parameters};
use consensus::Consensus;
use primary::{Certificate, Primary};
use store::Store;
use worker::Worker;

use tokio::sync::mpsc::{channel, Receiver};

// use minimal_template_node::cli::NarwhalParams;

/// The default channel capacity.
pub const CHANNEL_CAPACITY: usize = 1_000;

pub async fn start_narwhal(n_keys: String, n_committee: String, n_store: String) {
    log::warn!("Starting Narwhal...");
    run_narwhal(n_keys, n_committee, n_store).await.unwrap()
}

pub async fn run_narwhal(n_keys: String, n_committee: String, n_store: String) -> Result<()> {
    // Read the committee and node's keypair from file.
    let keypair = KeyPair::import(&n_keys).context("Failed to load the node's keypair")?;
    let committee =
        Committee::import(&n_committee).context("Failed to load the committee information")?;

    // Load default parameters if none are specified.
    let parameters = Parameters::default();

    // Make the data store.
    let store = Store::new(&n_store).context("Failed to create a store")?;
    
    // Channels the sequence of certificates.
    let (tx_output, rx_output) = channel(CHANNEL_CAPACITY);

    let name = keypair.name.clone();

    log::warn!("Starting Worker...");

    // Spawn a single worker.
    {
        let id = 0;
        Worker::spawn(
            name,
            id,
            committee.clone(),
            parameters.clone(),
            store.clone(),
        );
    }

    log::warn!("Starting Primary...");

    // Check whether to run a primary, a worker, or an entire authority.
    // Spawn the primary and consensus core.
    {
        let (tx_new_certificates, rx_new_certificates) = channel(CHANNEL_CAPACITY);
        let (tx_feedback, rx_feedback) = channel(CHANNEL_CAPACITY);
        Primary::spawn(
            keypair,
            committee.clone(),
            parameters.clone(),
            store.clone(),
            /* tx_consensus */ tx_new_certificates,
            /* rx_consensus */ rx_feedback,
        );
        Consensus::spawn(
            committee.clone(),
            parameters.gc_depth,
            /* rx_primary */ rx_new_certificates,
            /* tx_primary */ tx_feedback,
            tx_output,
        );
    }

    log::warn!("Analyzing...");

    // Analyze the consensus' output.
    analyze(rx_output, &n_store).await;

    // If this expression is reached, the program ends and all other tasks terminate.
    unreachable!();
}

/// Receives an ordered list of certificates and apply any application-specific logic.
async fn analyze(mut rx_output: Receiver<Certificate>, n_store: &str) {
    while let Some(certificate) = rx_output.recv().await {
        log::debug!("Certificate: {:?}", certificate);
        for (digest, worker_id) in certificate.header.payload.iter() {
            log::warn!(
                "LEDGER {} -> {:?} from {:?}",
                certificate.header,
                digest,
                worker_id
            );

            let db = rocksdb::DB::open_for_read_only(
                &rocksdb::Options::default(),
                n_store,
                true,
            )
            .unwrap();

            let key = digest.clone().to_vec();
            if let Ok(Some(_value)) = db.get(&key) {
                log::warn!("BATCH FOUND: {:?}", hex::encode(&key));
                // match bincode::deserialize(&value) {
                //     Ok(worker::worker::WorkerMessage::Batch(batch)) => {
                //         log::warn!("BATCH: {:?}", batch);
                //         for tx in batch {
                //             println!("DeliverTx'ing: {:?}", tx);
                //         }
                //     }
                //     _ => unreachable!(),
                // }
            } else {
                log::error!("BATCH NOT FOUND: {:?}", key);
                unreachable!()
            };
        }
        // NOTE: Here goes the application logic.
    }
}