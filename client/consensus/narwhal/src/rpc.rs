use codec::Decode;
use jsonrpsee::{
    core::{async_trait, RpcResult},
    proc_macros::rpc,
    types::error::ErrorObject,
};
use polkadot_sdk::{
    sp_core::Bytes,
    sp_runtime::{traits::Block as BlockT, OpaqueExtrinsic},
};

use crate::{transaction_driver::TransactionDriver, AuxData, ConfigPaths};

pub enum Error {
    ConfigError,
    DecodeError,
    LogicError,
}

impl From<Error> for i32 {
    fn from(e: Error) -> i32 {
        match e {
            Error::ConfigError => 1,
            Error::DecodeError => 2,
            Error::LogicError => 3,
        }
    }
}

#[rpc(client, server)]
pub trait DagApi {
    #[method(name = "dag_executeTransaction")]
    async fn execute_transaction(&self, transaction: Bytes) -> RpcResult<()>;
}

pub struct Dag<B> {
    config: ConfigPaths,
    _marker: std::marker::PhantomData<B>,
}

impl<B> Dag<B> {
    pub fn new(config: ConfigPaths) -> Self {
        Self {
            config,
            _marker: Default::default(),
        }
    }
}

#[async_trait]
impl<B> DagApiServer for Dag<B>
where
    B: BlockT<Extrinsic = OpaqueExtrinsic>,
{
    async fn execute_transaction(&self, transaction: Bytes) -> RpcResult<()> {
        log::warn!("RPC execute_transaction...");
        let uxt: <B as BlockT>::Extrinsic = Decode::decode(&mut &*transaction).map_err(|e| {
            ErrorObject::owned(
                Error::DecodeError.into(),
                "Unable to decode transaction received",
                Some(e.to_string()),
            )
        })?;

        let aux_data = AuxData::import(&self.config).map_err(|e| {
            ErrorObject::owned(
                Error::ConfigError.into(),
                "Failed to initialize AuxData from config for RPC",
                Some(e.to_string()),
            )
        })?;

        let transaction_driver = TransactionDriver::new(aux_data);
        transaction_driver
            .execute_transaction_impl(uxt)
            .await
            .map_err(|e| {
                ErrorObject::owned(
                    Error::LogicError.into(),
                    "Unable to submit transaction",
                    Some(e.to_string()),
                )
            })?;
        Ok(())
    }
}
