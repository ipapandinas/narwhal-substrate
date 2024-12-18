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
use std::{
    future::{self},
    marker::PhantomData,
    sync::{Arc, Mutex},
    time::Duration,
};

pub type LocalTransaction<B> = <B as BlockT>::Extrinsic;
type CIDP<B> = dyn CreateInherentDataProviders<
    B,
    (),
    InherentDataProviders = sp_timestamp::InherentDataProvider,
>;

/// max duration for creating a proposal in secs
pub const MAX_PROPOSAL_DURATION: u64 = 1;

// Seals new blocks using the custom proposer factory
pub struct Engine<B, C, SC, I>
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

impl<B, C, SC, I> Engine<B, C, SC, I>
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
