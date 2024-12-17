use anyhow::{Context, Result};
use bytes::Bytes;
use config::{Committee, KeyPair, Parameters};
use config::{Import as _, WorkerId};
use consensus::Consensus;
use crypto::PublicKey;
use futures::SinkExt;
use futures::StreamExt;
use polkadot_sdk::{
    sc_block_builder::{BlockBuilderApi, BlockBuilderBuilder},
    sc_consensus::{
        BlockImport, BlockImportParams, ForkChoiceStrategy, StateAction, StorageChanges,
    },
    sc_service::InPoolTransaction,
    sc_transaction_pool_api::{TransactionPool, TxHash},
    sp_api::{ApiExt, CallApiAt, ProvideRuntimeApi},
    sp_blockchain::{self, HeaderBackend},
    sp_consensus::{
        BlockOrigin, DisableProofRecording, Environment, Proposal, Proposer, SelectChain,
    },
    sp_inherents::{CreateInherentDataProviders, InherentData, InherentDataProvider},
    sp_runtime::{
        traits::{Block as BlockT, Header as HeaderT},
        Digest, OpaqueExtrinsic,
    },
    sp_timestamp,
};
use primary::{Certificate, Primary};
use std::future::{self};
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use store::Store;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use worker::Worker;

// use minimal_template_node::cli::NarwhalParams;

/// The default channel capacity.
pub const CHANNEL_CAPACITY: usize = 1_000;
/// max duration for creating a proposal in secs
pub const MAX_PROPOSAL_DURATION: u64 = 1;

pub type LocalTransaction<B> = <B as BlockT>::Extrinsic;
type CIDP<B> = dyn CreateInherentDataProviders<
    B,
    (),
    InherentDataProviders = sp_timestamp::InherentDataProvider,
>;

pub struct NarwhalParams<B: BlockT, TP, C, SC, I>
where
    B: BlockT,
    TP: TransactionPool<Block = B> + 'static,
    C: HeaderBackend<B> + ProvideRuntimeApi<B> + Send + Sync + 'static,
    C::Api: ApiExt<B> + BlockBuilderApi<B>,
    SC: SelectChain<B> + 'static,
    I: BlockImport<B> + Send + Sync + 'static,
{
    pub pool: Arc<TP>,
    pub client: Arc<C>,
    pub select_chain: SC,
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
pub struct BlockSealer<B, C, SC, I>
where
    B: BlockT,
    C: HeaderBackend<B> + ProvideRuntimeApi<B> + Send + Sync + 'static,
    SC: SelectChain<B> + 'static,
    C::Api: ApiExt<B> + BlockBuilderApi<B>,
{
    select_chain: SC,
    block_import: I,
    proposer_factory: CustomProposerFactory<B, C>,
}

impl<B, C, SC, I> BlockSealer<B, C, SC, I>
where
    B: BlockT<Extrinsic = OpaqueExtrinsic>,
    C: HeaderBackend<B> + ProvideRuntimeApi<B> + CallApiAt<B> + Send + Sync + 'static,
    C::Api: ApiExt<B> + BlockBuilderApi<B>,
    SC: SelectChain<B> + 'static,
    I: BlockImport<B> + Send + Sync + 'static,
{
    pub fn new(
        client: Arc<C>,
        block_import: I,
        select_chain: SC,
        local_pool: Arc<LocalPool>,
    ) -> Self {
        let proposer_factory = CustomProposerFactory::new(client.clone(), local_pool.clone());

        Self {
            block_import,
            select_chain,
            proposer_factory,
        }
    }

    pub async fn run(
        &mut self,
        create_inherent_data_providers: Box<CIDP<B>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>
    where
        <I as BlockImport<B>>::Error: Sync,
    {
        log::warn!("Block sealing...");
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));
        interval.reset();

        loop {
            interval.tick().await;
            match self.seal_block(&*create_inherent_data_providers).await {
                Ok(_) => log::warn!("Block sealed successfully."),
                Err(err) => log::error!("Error sealing block: {:?}", err),
            }
        }
    }

    async fn seal_block(
        &mut self,
        create_inherent_data_providers: &CIDP<B>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>
    where
        <I as BlockImport<B>>::Error: Sync,
    {
        let future = async {
            let parent = self.select_chain.best_chain().await?;

            let inherent_data_providers = create_inherent_data_providers
                .create_inherent_data_providers(parent.hash(), ())
                .await
                .map_err(|e| anyhow::anyhow!("Error creating inherent data providers: {}", e))?;

            let inherent_data = inherent_data_providers.create_inherent_data().await?;
            let digest = Digest::default();

            let proposer = self.proposer_factory.init(&parent).await?;

            let proposal = proposer
                .propose(
                    inherent_data.clone(),
                    digest,
                    Duration::from_secs(MAX_PROPOSAL_DURATION),
                    None,
                )
                .await?;

            let (header, body) = proposal.block.deconstruct();
            log::warn!("header: {:?}", header);
            log::warn!("body: {:?}", body);

            let mut params = BlockImportParams::new(BlockOrigin::Own, header.clone());
            params.body = Some(body);
            params.finalized = true;
            params.fork_choice = Some(ForkChoiceStrategy::LongestChain);
            params.state_action =
                StateAction::ApplyChanges(StorageChanges::Changes(proposal.storage_changes));

            let res = self.block_import.import_block(params).await?;
            log::warn!("res: {:?}", res);

            Ok::<(), anyhow::Error>(())
        };

        if let Err(err) = future.await {
            log::error!("Error in proposal: {:?}", err);
        }

        // Remove transaction from substrate_pool if successfully executed and sealed in the block
        Ok(())
    }
}

pub async fn start_narwhal<B, TP, C, SC, I>(params: NarwhalParams<B, TP, C, SC, I>) -> Result<()>
where
    B: BlockT<Extrinsic = OpaqueExtrinsic> + 'static,
    TP: TransactionPool<Block = B> + 'static,
    C: HeaderBackend<B> + ProvideRuntimeApi<B> + CallApiAt<B> + Send + Sync + 'static,
    C::Api: ApiExt<B> + BlockBuilderApi<B>,
    SC: SelectChain<B> + 'static,
    I: BlockImport<B> + Send + Sync + 'static,
    <I as BlockImport<B>>::Error: Sync,
{
    log::warn!("Starting Narwhal...");

    let narwhal_pool = Arc::new(LocalPool::new());
    let aux_data = AuxData::import(params.n_keys, params.n_committee, params.n_store)
        .context("Failed to initialize AuxData used for authoring")?;

    let mut block_sealer = BlockSealer::new(
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
    if let Err(err) = run_narwhal(params.pool, narwhal_pool.clone(), aux_data).await {
        log::error!("Error while running Narwhal: {:?}", err);
    }

    Ok(())
}

pub async fn run_narwhal<B, TP>(
    substrate_pool: Arc<TP>,
    narwhal_pool: Arc<LocalPool>,
    aux_data: AuxData,
) -> Result<()>
where
    B: BlockT<Extrinsic = OpaqueExtrinsic> + 'static,
    TP: TransactionPool<Block = B> + 'static,
{
    // TODO: handle shutdown_signal in a tokio::select!
    log::warn!("Running Narwhal...");

    // Channels the sequence of certificates.
    let (tx_output, rx_output) = channel(CHANNEL_CAPACITY);

    // Consensus parameters
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

    let substrate_pool_clone = Arc::clone(&substrate_pool);
    tokio::spawn(async move {
        let driver = TransactionDriver::new(substrate_pool_clone);
        let _ = driver.run(committee, &keypair_name, &worker_id).await;
    });

    if let Err(err) = process_certificate(store, rx_output, substrate_pool, narwhal_pool).await {
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
pub async fn process_certificate<B, TP>(
    mut store: Store,
    mut rx_certificate: Receiver<Certificate>,
    substrate_pool: Arc<TP>,
    narwhal_pool: Arc<LocalPool>,
) -> Result<()>
where
    B: BlockT<Extrinsic = OpaqueExtrinsic> + 'static,
    TP: TransactionPool<Block = B> + 'static,
{
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
                            log::warn!("DriveTx'ing - raw tx: {:?}", tx);
                            // Deserialize tx into TxHash
                            match serde_json::from_slice::<TxHash<TP>>(&tx) {
                                Ok(tx_hash) => {
                                    log::warn!("DriveTx'ing - deserialize tx HASH: {:?}", tx_hash);
                                    if let Some(certified_tx) =
                                        substrate_pool.ready_transaction(&tx_hash)
                                    {
                                        let tx_data = certified_tx.data().clone();
                                        log::warn!("DriveTx'ing - tx_data: {:?}", tx_data);
                                        // Import ordered and certified txn for block sealing
                                        narwhal_pool.import(tx_data);
                                    } else {
                                        log::warn!("Transaction not ready: {:?}", tx);
                                    }
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

// CustomTransactionPool
#[derive(Clone)]
pub struct LocalPool {
    pool: Arc<Mutex<Vec<OpaqueExtrinsic>>>,
}

impl LocalPool {
    pub fn new() -> Self {
        Self {
            pool: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn import(&self, tx: OpaqueExtrinsic) {
        let mut lock = self
            .pool
            .lock()
            .expect("Failed to acquire lock for the mutex");
        lock.push(tx);
    }

    pub fn pop_all(&self) -> Vec<OpaqueExtrinsic> {
        let mut lock = self
            .pool
            .lock()
            .expect("Failed to acquire lock for the mutex");
        std::mem::take(&mut *lock)
    }
}

#[derive(Clone)]
pub struct CustomProposerFactory<B, C>
where
    B: BlockT,
    C: HeaderBackend<B> + ProvideRuntimeApi<B> + Send + Sync + 'static,
    C::Api: ApiExt<B> + BlockBuilderApi<B>,
{
    client: Arc<C>,
    pub transaction_pool: Arc<LocalPool>,
    pub _phantom: PhantomData<B>,
}

impl<B, C> CustomProposerFactory<B, C>
where
    B: BlockT,
    C: HeaderBackend<B> + ProvideRuntimeApi<B> + Send + Sync + 'static,
    C::Api: ApiExt<B> + BlockBuilderApi<B>,
{
    pub fn new(client: Arc<C>, transaction_pool: Arc<LocalPool>) -> Self {
        Self {
            client,
            transaction_pool,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<B: BlockT, C> Environment<B> for CustomProposerFactory<B, C>
where
    B: BlockT<Extrinsic = OpaqueExtrinsic>,
    C: HeaderBackend<B> + ProvideRuntimeApi<B> + CallApiAt<B> + Send + Sync + 'static,
    C::Api: ApiExt<B> + BlockBuilderApi<B>,
{
    type CreateProposer = future::Ready<Result<CustomProposer<B, C>, sp_blockchain::Error>>;
    type Proposer = CustomProposer<B, C>;
    type Error = sp_blockchain::Error;

    fn init(&mut self, parent_header: &B::Header) -> Self::CreateProposer {
        future::ready(Ok(CustomProposer {
            client: self.client.clone(),
            parent_hash: parent_header.hash(),
            parent_number: *parent_header.number(),
            transaction_pool: self.transaction_pool.clone(),
        }))
    }
}

pub struct CustomProposer<B, C>
where
    B: BlockT,
    C: HeaderBackend<B> + ProvideRuntimeApi<B> + CallApiAt<B> + Send + Sync + 'static,
    C::Api: ApiExt<B> + BlockBuilderApi<B>,
{
    client: Arc<C>,
    parent_hash: B::Hash,
    parent_number: <<B as BlockT>::Header as HeaderT>::Number,
    transaction_pool: Arc<LocalPool>,
}

impl<B, C> CustomProposer<B, C>
where
    B: BlockT<Extrinsic = OpaqueExtrinsic>,
    C: HeaderBackend<B> + ProvideRuntimeApi<B> + CallApiAt<B> + Send + Sync + 'static,
    C::Api: ApiExt<B> + BlockBuilderApi<B>,
{
    fn propose_with(
        self,
        inherent_data: InherentData,
        _inherent_digests: Digest,
    ) -> Result<Proposal<B, ()>, sp_blockchain::Error> {
        let mut block_builder = BlockBuilderBuilder::new(&*self.client)
            .on_parent_block(self.parent_hash)
            .with_parent_block_number(self.parent_number)
            .build()?;

        let inherents = block_builder.create_inherents(inherent_data)?;

        for inherent in inherents {
            match block_builder.push(inherent.clone()) {
                Ok(()) => log::warn!("Inherent Applied: {:?}", inherent),
                Err(e) => log::error!("Error applying inherent: {:?} - {:?}", inherent, e),
            }
        }

        let txns = self.transaction_pool.pop_all();

        for txn in txns {
            match block_builder.push(txn.clone()) {
                Ok(()) => log::warn!("Txn Applied: {:?}", txn),
                Err(e) => log::error!("Error applying txn: {:?} - {:?}", txn, e),
            }
        }

        let (block, storage_changes, _proof) = block_builder.build()?.into_inner();
        Ok(Proposal {
            block,
            proof: (),
            storage_changes,
        })
    }
}

impl<B, C> Proposer<B> for CustomProposer<B, C>
where
    B: BlockT<Extrinsic = OpaqueExtrinsic>,
    C: HeaderBackend<B> + ProvideRuntimeApi<B> + CallApiAt<B> + Send + Sync + 'static,
    C::Api: ApiExt<B> + BlockBuilderApi<B>,
{
    type Proposal = future::Ready<Result<Proposal<B, ()>, Self::Error>>;
    type Error = sp_blockchain::Error;
    type ProofRecording = DisableProofRecording;
    type Proof = ();

    fn propose(
        self,
        inherent_data: InherentData,
        inherent_digests: Digest,
        _max_duration: std::time::Duration,
        _block_size_limit: Option<usize>,
    ) -> Self::Proposal {
        let res = self.propose_with(inherent_data, inherent_digests);

        future::ready(res)
    }
}
