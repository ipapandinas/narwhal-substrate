use anyhow::{Context, Result};
use bytes::Bytes;
use config::{Committee, KeyPair, Parameters};
use config::{Import as _, WorkerId};
use consensus::Consensus;
use crypto::PublicKey;
use futures::SinkExt;
use futures::StreamExt;
use polkadot_sdk::sc_consensus::BlockImport;
use polkadot_sdk::sp_consensus::Proposal;
use polkadot_sdk::sp_inherents::InherentData;
use polkadot_sdk::sp_runtime::Digest;
use polkadot_sdk::{
    sc_block_builder::BlockBuilderApi,
    sc_transaction_pool_api::{TransactionFor, TransactionPool, TxHash},
    sp_api::{ApiExt, CallApiAt, ProvideRuntimeApi},
    sp_blockchain::{self, HeaderBackend},
    sp_consensus::{DisableProofRecording, Environment, Proposer},
    sp_runtime::traits::{Block as BlockT, Header as HeaderT},
};
use primary::{Certificate, Primary};
use std::future::{self, Future};
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::{pin::Pin, sync::Arc};
use store::Store;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use worker::Worker;

// use minimal_template_node::cli::NarwhalParams;

/// The default channel capacity.
pub const CHANNEL_CAPACITY: usize = 1_000;

pub struct NarwhalParams<B: BlockT, TP, C, I>
where
    B: BlockT,
    TP: TransactionPool<Block = B> + 'static,
    C: HeaderBackend<B> + ProvideRuntimeApi<B> + Send + Sync + 'static,
    C::Api: ApiExt<B> + BlockBuilderApi<B>,
    I: BlockImport<B> + Send + Sync + 'static,
{
    pub pool: Arc<TP>,
    pub client: Arc<C>,
    pub block_import: I,
    pub n_keys: String,
    pub n_committee: String,
    pub n_store: String,
    pub _phantom: PhantomData<B>,
}

pub struct AuxData {
    keypair: KeyPair,
    committee: Committee,
    store: Store,
}

impl AuxData {
    // Read the committee and node's keypair from file.
    pub fn import(keypair_path: String, commitee_path: String, store_path: String) -> Result<Self> {
        let keypair =
            KeyPair::import(&keypair_path).context("Failed to load the node's keypair")?;
        let committee = Committee::import(&commitee_path)
            .context("Failed to load the committee information")?;
        let store = Store::new(&store_path).context("Failed to create a store")?;

        Ok(Self {
            keypair,
            committee,
            store,
        })
    }
}

pub struct TransactionDriver<TP: TransactionPool + 'static> {
    pub pool: Arc<TP>,
}

impl<TP> TransactionDriver<TP>
where
    TP: TransactionPool + 'static,
{
    pub fn new(pool: Arc<TP>) -> Self {
        Self { pool }
    }

    // Consume received txn in the substrate mempool and drive them to consensus worker
    pub async fn run(
        &self,
        committee: Committee,
        keypair_name: &PublicKey,
        worker_id: &WorkerId,
    ) -> Result<()> {
        let worker_address = committee
            .worker(keypair_name, worker_id)
            .expect("Our public key or worker id is not in the committee")
            .transactions;

        log::warn!("Driving transactions...");
        // TODO: handle shutdown_signal in a tokio::select!
        while let Some(tx) = self.pool.import_notification_stream().next().await {
            log::warn!("Received tx: {:?}", tx);
            let serialized_tx: Vec<u8> =
                serde_json::to_vec(&tx).context("Failed to serialize transaction")?;

            match process_transaction(serialized_tx, worker_address).await {
                Ok(_) => (),
                Err(err) => log::error!("Transaction processing failed: {}", err),
            }
        }

        Ok(())
    }
}

// Seals new blocks using the custom proposer factory
pub struct BlockSealer<B, TP, C, I>
where
    B: BlockT,
    TP: TransactionPool<Block = B> + 'static,
    C: HeaderBackend<B> + ProvideRuntimeApi<B> + Send + Sync + 'static,
    C::Api: ApiExt<B> + BlockBuilderApi<B>,
{
    client: Arc<C>,
    block_import: I,
    pool: Arc<CustomTransactionPool<B, TP>>,
    proposer_factory: Arc<CustomProposerFactory<B, TP, C>>,
}

impl<B, TP, C, I> BlockSealer<B, TP, C, I>
where
    B: BlockT,
    TP: TransactionPool<Block = B> + 'static,
    C: HeaderBackend<B> + ProvideRuntimeApi<B> + Send + Sync + 'static,
    C::Api: ApiExt<B> + BlockBuilderApi<B>,
    I: BlockImport<B> + Send + Sync + 'static,
{
    pub fn new(client: Arc<C>, block_import: I, pool: Arc<CustomTransactionPool<B, TP>>) -> Self {
        let proposer_factory = Arc::new(CustomProposerFactory::new(client.clone(), pool.clone()));

        Self {
            client,
            block_import,
            pool,
            proposer_factory,
        }
    }

    pub async fn run(&self) -> Result<()> {
        log::warn!("Block sealing...");
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));
        interval.reset();

        loop {
            interval.tick().await;
            match self.seal_block().await {
                Ok(_) => log::warn!("Block sealed successfully."),
                Err(err) => log::error!("Error sealing block: {:?}", err),
            }
        }
    }

    async fn seal_block(&self) -> Result<()> {
        Ok(())
    }
}

pub async fn start_narwhal<B, TP, C, I>(params: NarwhalParams<B, TP, C, I>) -> Result<()>
where
    B: BlockT + 'static,
    TP: TransactionPool<Block = B> + 'static,
    C: HeaderBackend<B> + ProvideRuntimeApi<B> + Send + Sync + 'static,
    C::Api: ApiExt<B> + BlockBuilderApi<B>,
    I: BlockImport<B> + Send + Sync + 'static,
{
    log::warn!("Starting Narwhal...");

    let narwhal_pool = Arc::new(CustomTransactionPool::<B, TP>::new());
    let aux_data = AuxData::import(params.n_keys, params.n_committee, params.n_store)
        .context("Failed to initialize AuxData used for authoring")?;

    let block_sealer = BlockSealer::new(params.client, params.block_import, narwhal_pool.clone());

    log::warn!("Spawning block sealer task...");
    // Spawn the block sealing task
    tokio::spawn(async move {
        if let Err(err) = block_sealer.run().await {
            log::error!("Error in block sealing: {:?}", err);
        }
    });

    // Run Narwhal consensus
    if let Err(err) = run_narwhal(params.pool, aux_data).await {
        log::error!("Error while running Narwhal: {:?}", err);
    }

    Ok(())
}

pub async fn run_narwhal<B, TP>(substrate_pool: Arc<TP>, aux_data: AuxData) -> Result<()>
where
    B: BlockT + 'static,
    TP: TransactionPool<Block = B> + 'static,
{
    // TODO: handle shutdown_signal in a tokio::select!
    log::warn!("Running Narwhal...");

    // Channels the sequence of certificates.
    let (tx_output, rx_output) = channel(CHANNEL_CAPACITY);

    // Load default parameters if none are specified.
    let parameters = Parameters::default();

    let AuxData {
        keypair,
        committee,
        store,
    } = aux_data;

    let keypair_name = keypair.name.clone();
    let worker_id: u32 = 0; //TODO: make id dynamic from index position in committee json file

    log::warn!("Starting Worker...");
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

    tokio::spawn(async move {
        let driver = TransactionDriver::new(substrate_pool.clone());
        let _ = driver.run(committee, &keypair_name, &worker_id).await;
    });

    if let Err(err) = process_certificate(store, rx_output).await {
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
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    Ok(())
}

// Submits processed transaction extracted from the certificate payload into the custom pool narwhal_pool
pub async fn process_certificate(
    mut store: Store,
    mut rx_certificate: Receiver<Certificate>,
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
                        for tx in batch {
                            println!("DeliverTx'ing: {:?}", tx);
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

// CustomTransactionPool
#[derive(Clone)]
pub struct CustomTransactionPool<B: BlockT, TP: TransactionPool<Block = B>> {
    transactions: Vec<TransactionFor<TP>>,
}

impl<B: BlockT + 'static, TP: TransactionPool<Block = B>> CustomTransactionPool<B, TP> {
    pub fn new() -> Self {
        Self {
            transactions: Vec::new(),
        }
    }

    pub fn import(&mut self, tx: TransactionFor<TP>) {
        // let mut lock = self.transactions.lock().unwrap();
        // lock.push(tx)
        todo!()
    }

    pub fn pop_all(&self) -> Vec<TransactionFor<TP>> {
        // let mut lock = self.transactions.lock().unwrap();
        // std::mem::take(&mut *lock)
        todo!()
    }
}

// CustomProposerFactory

#[derive(Clone)]
pub struct CustomProposerFactory<B, TP, C>
where
    B: BlockT,
    TP: TransactionPool<Block = B> + 'static,
    C: HeaderBackend<B> + ProvideRuntimeApi<B> + Send + Sync + 'static,
    C::Api: ApiExt<B> + BlockBuilderApi<B>,
{
    client: Arc<C>,
    custom_transaction_pool: Arc<CustomTransactionPool<B, TP>>,
}

impl<B, TP, C> CustomProposerFactory<B, TP, C>
where
    B: BlockT,
    TP: TransactionPool<Block = B> + 'static,
    C: HeaderBackend<B> + ProvideRuntimeApi<B> + Send + Sync + 'static,
    C::Api: ApiExt<B> + BlockBuilderApi<B>,
{
    pub fn new(client: Arc<C>, custom_transaction_pool: Arc<CustomTransactionPool<B, TP>>) -> Self {
        Self {
            client,
            custom_transaction_pool,
        }
    }
}

impl<B: BlockT, TP: TransactionPool<Block = B>, C> Environment<B>
    for CustomProposerFactory<B, TP, C>
where
    B: BlockT,
    TP: TransactionPool<Block = B> + Clone + 'static,
    C: HeaderBackend<B> + ProvideRuntimeApi<B> + CallApiAt<B> + Send + Sync + Clone + 'static,
    C::Api: ApiExt<B> + BlockBuilderApi<B>,
{
    type CreateProposer = future::Ready<Result<CustomProposer<B, TP, C>, sp_blockchain::Error>>;
    type Proposer = CustomProposer<B, TP, C>;
    type Error = sp_blockchain::Error;

    fn init(&mut self, parent_header: &B::Header) -> Self::CreateProposer {
        future::ready(Ok(CustomProposer {
            factory: self.clone(),
            parent_hash: parent_header.hash(),
        }))
    }
}

// CustomProposer

pub struct CustomProposer<B, TP, C>
where
    B: BlockT,
    TP: TransactionPool<Block = B> + 'static,
    C: HeaderBackend<B> + ProvideRuntimeApi<B> + CallApiAt<B> + Send + Sync + 'static,
    C::Api: ApiExt<B> + BlockBuilderApi<B>,
{
    factory: CustomProposerFactory<B, TP, C>,
    parent_hash: B::Hash,
}

impl<B, TP, C> CustomProposer<B, TP, C>
where
    B: BlockT,
    TP: TransactionPool<Block = B> + 'static,
    C: HeaderBackend<B> + ProvideRuntimeApi<B> + CallApiAt<B> + Send + Sync + 'static,
    C::Api: ApiExt<B> + BlockBuilderApi<B>,
{
    fn propose_with(
        &mut self,
        _inherent_data: InherentData,
        _inherent_digests: Digest,
        // deadline: std::time::Instant,
        // block_size_limit: Option<usize>,
    ) -> future::Ready<Result<Proposal<B, ()>, sp_blockchain::Error>> {
        // let block_builder = BlockBuilderBuilder::new(&*self.factory.client)
        //     .on_parent_block(self.parent_hash)
        //     .fetch_parent_block_number(&*self.factory.client)
        //     .unwrap()
        //     .with_inherent_digests(pre_digests)
        //     .build()
        //     .unwrap();

        // let mut block = match block_builder.build().map_err(|e| e.into()) {
        //     Ok(b) => b.block,
        //     Err(e) => return future::ready(Err(e)),
        // };

        // // mutate the block header according to the mutator.
        // (self.factory.mutator)(&mut block.header, Stage::PreSeal);

        // future::ready(Ok(Proposal {
        //     block,
        //     proof: (),
        //     storage_changes: Default::default(),
        // }))
        todo!();
    }
}

impl<B, TP: TransactionPool<Block = B>, C> Proposer<B> for CustomProposer<B, TP, C>
where
    B: BlockT,
    TP: TransactionPool<Block = B> + 'static,
    C: HeaderBackend<B> + ProvideRuntimeApi<B> + CallApiAt<B> + Send + Sync + 'static,
    C::Api: ApiExt<B> + BlockBuilderApi<B>,
{
    type Proposal = Pin<Box<dyn Future<Output = Result<Proposal<B, ()>, Self::Error>> + Send>>;
    type Error = sp_blockchain::Error;
    type ProofRecording = DisableProofRecording;
    type Proof = ();

    fn propose(
        self,
        _inherent_data: InherentData,
        _inherent_digests: Digest,
        _max_duration: std::time::Duration,
        _block_size_limit: Option<usize>,
    ) -> Self::Proposal {
        // self.propose_with(
        //     inherent_data,
        //     inherent_digests,
        //     // max_duration,
        //     // block_size_limit,
        // )
        todo!()
    }
}
