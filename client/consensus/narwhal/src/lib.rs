use anyhow::{Context, Result};
use bytes::Bytes;
use config::Import as _;
use config::{Committee, KeyPair, Parameters};
use consensus::Consensus;
use futures::SinkExt;
use futures::StreamExt;
use polkadot_sdk::{sc_transaction_pool_api::TransactionPool, sp_runtime::traits::Block as BlockT};
use primary::{Certificate, Primary};
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;
use store::Store;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use worker::Worker;

// use minimal_template_node::cli::NarwhalParams;

pub type TxHash<P> = <P as TransactionPool>::Hash;

/// The default channel capacity.
pub const CHANNEL_CAPACITY: usize = 1_000;

pub struct NarwhalParams<B: BlockT, TP> {
    pub pool: Arc<TP>,
    pub n_keys: String,
    pub n_committee: String,
    pub n_store: String,
    pub _phantom: PhantomData<B>,
}

async fn deviate_transaction<B, TP>(
    mut receiver: futures::channel::mpsc::Receiver<TxHash<TP>>,
    worker_address: SocketAddr,
) -> Result<()>
where
    B: BlockT + 'static,
    TP: TransactionPool<Block = B>,
{
    while let Some(tx) = receiver.next().await {
        log::warn!("Received tx: {:?}", tx);
        let serialized_tx: Vec<u8> =
            serde_json::to_vec(&tx).context("Failed to serialize transaction")?;

        let stream = TcpStream::connect(worker_address).await?;
        // LengthDelimitedCodec will prepend a 4-byte length header (big-endian)
        let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
        transport.send(Bytes::from(serialized_tx)).await?;

        // TODO: Avoid Unnecessary Blocking - Remove it or consider consider a rate-limiting mechanism (e.g., tokio::time::Interval)
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }

    Ok(())
}

pub async fn start_narwhal<B, TP>(params: NarwhalParams<B, TP>) -> Result<()>
where
    B: BlockT + 'static,
    TP: TransactionPool<Block = B>,
{
    log::warn!("Starting Narwhal...");

    // Read the committee and node's keypair from file.
    let keypair = KeyPair::import(&params.n_keys).context("Failed to load the node's keypair")?;
    let committee = Committee::import(&params.n_committee)
        .context("Failed to load the committee information")?;

    let name = keypair.name.clone();
    //TODO: make id dynamic from index position in committee json file
    let id = 0;

    let receiver = params.pool.import_notification_stream();
    let worker_address = committee
        .worker(&name, &id)
        .expect("Our public key or worker id is not in the committee")
        .transactions;

    // Consume received txn in the mempool concurrently
    // TODO: handle shutdown_signal in a tokio::select!
    tokio::spawn(async move {
        let _ = deviate_transaction::<B, TP>(receiver, worker_address).await;
    });

    // Run Narwhal
    if let Err(err) = run_narwhal(keypair, committee, &params.n_store).await {
        log::error!("Error while running Narwhal: {:?}", err);
    }

    Ok(())
}

pub async fn run_narwhal(keypair: KeyPair, committee: Committee, n_store: &str) -> Result<()> {
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
    let _ = analyze(rx_output, &n_store).await;

    // If this expression is reached, the program ends and all other tasks terminate.
    // TODO: handle this more properly with specific error if needed
    unreachable!();
}

/// Receives an ordered list of certificates and apply any application-specific logic.
async fn analyze(mut rx_output: Receiver<Certificate>, n_store: &str) -> Result<()> {
    while let Some(certificate) = rx_output.recv().await {
        log::debug!("Certificate: {:?}", certificate);
        for (digest, worker_id) in certificate.header.payload.iter() {
            log::warn!(
                "LEDGER {} -> {:?} from {:?}",
                certificate.header,
                digest,
                worker_id
            );

            let db = rocksdb::DB::open_for_read_only(&rocksdb::Options::default(), n_store, true)
                .context("Failed to open RocksDB in read-only mode")?;

            let key = digest.as_ref();
            if let Ok(Some(_value)) = db.get(key) {
                log::warn!("BATCH FOUND: {:?}", hex::encode(key));
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

    Ok(())
}
