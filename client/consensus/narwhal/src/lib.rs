use anyhow::{Context, Result};
use bytes::Bytes;
use config::Import as _;
use config::{Committee, KeyPair, Parameters};
use consensus::Consensus;
use futures::SinkExt;
use polkadot_sdk::{
    sc_block_builder::BlockBuilderApi,
    sc_consensus::BlockImport,
    sp_api::{ApiExt, CallApiAt, ProvideRuntimeApi},
    sp_blockchain::HeaderBackend,
    sp_consensus::SelectChain,
    sp_runtime::{traits::Block as BlockT, OpaqueExtrinsic},
    sp_timestamp,
};
use primary::{Certificate, Primary};
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;
use store::Store;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use worker::Worker;

use engine::{Engine, LocalPool};

pub mod engine;
pub mod rpc;
pub mod transaction_driver;

/// The default channel capacity.
pub const CHANNEL_CAPACITY: usize = 1_000;

pub struct NarwhalParams<B: BlockT, C, SC, I>
where
    B: BlockT,
    C: HeaderBackend<B> + ProvideRuntimeApi<B> + Send + Sync + 'static,
    C::Api: ApiExt<B> + BlockBuilderApi<B>,
    SC: SelectChain<B> + 'static,
    I: BlockImport<B> + Send + Sync + 'static,
{
    pub client: Arc<C>,
    pub select_chain: SC,
    pub block_import: I,
    pub config: ConfigPaths,
    pub _phantom: PhantomData<B>,
}

#[derive(Clone, Debug)]
pub struct ConfigPaths {
    pub keypair: String,
    pub committee: String,
    pub store: String,
}

pub struct AuxData {
    keypair: KeyPair,
    committee: Committee,
}

impl AuxData {
    // Read the committee, store and node's keypair from file.
    pub fn import(config: &ConfigPaths) -> Result<Self> {
        let keypair =
            KeyPair::import(&config.keypair).context("Failed to load the node's keypair")?;
        let committee = Committee::import(&config.committee)
            .context("Failed to load the committee information")?;

        Ok(Self { keypair, committee })
    }
}

pub async fn start_narwhal<B, C, SC, I>(params: NarwhalParams<B, C, SC, I>) -> Result<()>
where
    B: BlockT<Extrinsic = OpaqueExtrinsic> + 'static,
    C: HeaderBackend<B> + ProvideRuntimeApi<B> + CallApiAt<B> + Send + Sync + 'static,
    C::Api: ApiExt<B> + BlockBuilderApi<B>,
    SC: SelectChain<B> + 'static,
    I: BlockImport<B> + Send + Sync + 'static,
    <I as BlockImport<B>>::Error: Sync,
{
    log::warn!("Starting Narwhal");

    let narwhal_pool = Arc::new(LocalPool::new());
    let mut block_sealer = Engine::new(
        params.client,
        params.block_import,
        params.select_chain,
        narwhal_pool.clone(),
    );

    let create_inherent_data_providers =
        Box::new(|_, _| async { Ok(sp_timestamp::InherentDataProvider::from_system_time()) });

    tokio::spawn(async move {
        if let Err(err) = block_sealer.run(create_inherent_data_providers).await {
            log::error!("Error in block sealing task: {:?}", err);
        }
    });

    // Run Narwhal consensus
    if let Err(err) = run_narwhal(narwhal_pool.clone(), &params.config).await {
        log::error!("Error while running Narwhal: {:?}", err);
    }

    Ok(())
}

pub async fn run_narwhal(narwhal_pool: Arc<LocalPool>, config: &ConfigPaths) -> Result<()> {
    // TODO: handle shutdown_signal in a tokio::select!
    log::warn!("Running Narwhal...");

    // Channels the sequence of certificates.
    let (tx_output, rx_output) = channel(CHANNEL_CAPACITY);

    // Consensus parameters
    let parameters = Parameters::default();

    let AuxData { keypair, committee } =
        AuxData::import(config).context("Failed to initialize AuxData used for authoring")?;
    let store = Store::new(&config.store).context("Failed to create a store")?;

    let keypair_name = keypair.name.clone();
    let worker_id: u32 = 0; //TODO: make id dynamic from index position in committee json file

    log::warn!("Starting Worker");
    // Spawn a single worker.
    {
        Worker::spawn(
            keypair_name.clone(),
            worker_id,
            committee.clone(),
            parameters.clone(),
            store.clone(),
        );
    }

    log::warn!("Starting Primary");
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

    if let Err(err) = process_certificate(store, rx_output, narwhal_pool).await {
        log::error!("Error in process_certificate: {:?}", err);
    }

    unreachable!();
}

// Submit a validated transaction to consensus client (e.g narwhal worker)
pub async fn process_transaction(transaction: Vec<u8>, client_addr: SocketAddr) -> Result<()> {
    let stream = TcpStream::connect(client_addr).await?;
    // LengthDelimitedCodec will prepend a 4-byte length header (big-endian)
    let mut transport: Framed<TcpStream, LengthDelimitedCodec> =
        Framed::new(stream, LengthDelimitedCodec::new());
    transport.send(Bytes::from(transaction)).await?;

    // TODO: Avoid Unnecessary Blocking - Remove it or consider consider a rate-limiting mechanism (e.g., tokio::time::Interval)
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    Ok(())
}

// Submits processed transaction extracted from the certificate payload into the custom pool narwhal_pool
pub async fn process_certificate(
    mut store: Store,
    mut rx_certificate: Receiver<Certificate>,
    narwhal_pool: Arc<LocalPool>,
) -> Result<()> {
    log::warn!("Processing certificates...");
    while let Some(certificate) = rx_certificate.recv().await {
        log::debug!("Certificate: {:?}", certificate);
        for (digest, worker_id) in certificate.header.payload.iter() {
            log::warn!(
                "LEDGER {} -> {:?} from {:?}",
                certificate.header,
                digest,
                worker_id
            );

            if let Ok(Some(value)) = store.read(digest.to_vec()).await {
                log::warn!("BATCH FOUND: {:?}", digest.to_vec());
                match bincode::deserialize(&value) {
                    Ok(worker::worker::WorkerMessage::Batch(batch)) => {
                        log::warn!("BATCH: {:?}", batch);
                        for raw_tx in batch {
                            // Deserialize tx into OpaqueExtrinsic
                            match serde_json::from_slice::<OpaqueExtrinsic>(&raw_tx) {
                                Ok(transaction) => {
                                    log::warn!(
                                        "DriveTx'ing - deserialize transaction: {:?}",
                                        transaction
                                    );
                                    narwhal_pool.import(transaction);
                                }
                                Err(err) => {
                                    log::error!("Failed to deserialize transaction: {:?}", err);
                                }
                            }
                        }
                    }
                    _ => unreachable!(),
                }
            } else {
                log::error!("BATCH NOT FOUND: {:?}", digest.to_vec());
                unreachable!()
            };
        }
    }

    Ok(())
}
