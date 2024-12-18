use anyhow::{Context, Result};
use bytes::Bytes;
use config::Import as _;
use config::{Committee, KeyPair, Parameters};
use consensus::Consensus;
use futures::SinkExt;
use polkadot_sdk::{
    sc_block_builder::{BlockBuilderApi, BlockBuilderBuilder},
    sc_consensus::{
        BlockImport, BlockImportParams, ForkChoiceStrategy, StateAction, StorageChanges,
    },
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

pub mod rpc;

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

pub struct TransactionDriver {
    worker_address: SocketAddr,
}

impl TransactionDriver {
    pub fn new(aux_data: AuxData) -> Self {
        let keypair_name = aux_data.keypair.name;
        let worker_id: u32 = 0; //TODO: make id dynamic from index position in committee json file

        let worker_address = aux_data
            .committee
            .worker(&keypair_name, &worker_id)
            .expect("Our public key or worker id is not in the committee")
            .transactions;

        Self { worker_address }
    }

    pub async fn execute_transaction_impl(&self, transaction: OpaqueExtrinsic) -> Result<()> {
        // TODO: VERIFY transaction
        self.submit(transaction, self.worker_address).await
    }

    // Submit a verified transaction to a consensus worker
    async fn submit(&self, transaction: OpaqueExtrinsic, client_addr: SocketAddr) -> Result<()> {
        log::warn!("Submitting transaction...");
        let serialized_tx: Vec<u8> =
            serde_json::to_vec(&transaction).context("Failed to serialize transaction")?;

        match process_transaction(serialized_tx, client_addr).await {
            Ok(_) => (),
            Err(err) => log::error!("Transaction processing failed: {}", err),
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
        // log::warn!("Block sealing...");
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(2));
        interval.reset();

        loop {
            interval.tick().await;
            match self.seal_block(&*create_inherent_data_providers).await {
                Ok(_) => (),
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

            let mut params = BlockImportParams::new(BlockOrigin::Own, header.clone());
            params.body = Some(body);
            params.finalized = true;
            params.fork_choice = Some(ForkChoiceStrategy::LongestChain);
            params.state_action =
                StateAction::ApplyChanges(StorageChanges::Changes(proposal.storage_changes));

            let _ = self.block_import.import_block(params).await?;
            // log::warn!("res: {:?}", res);

            Ok::<(), anyhow::Error>(())
        };

        if let Err(err) = future.await {
            log::error!("Error in proposal: {:?}", err);
        }

        Ok(())
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
    log::warn!("Starting Narwhal...");

    let narwhal_pool = Arc::new(LocalPool::new());
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
                Ok(()) => (),
                // log::warn!("Inherent Applied: {:?}", inherent),
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
