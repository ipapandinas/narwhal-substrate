// This file is part of Substrate.

// Copyright (C) Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: Apache-2.0

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A collection of node-specific RPC methods.
//! Substrate provides the `sc-rpc` crate, which defines the core RPC layer
//! used by Substrate nodes. This file extends those RPC definitions with
//! capabilities that are specific to this project's runtime configuration.

#![warn(missing_docs)]

use jsonrpsee::RpcModule;
use minimal_template_runtime::interface::{AccountId, Nonce, OpaqueBlock};
use narwhal_consensus::{
    rpc::{Dag, DagApiServer},
    ConfigPaths,
};
use polkadot_sdk::{
    sc_transaction_pool_api::TransactionPool,
    sp_blockchain::{Error as BlockChainError, HeaderBackend, HeaderMetadata},
    sp_runtime::{traits::Block as BlockT, OpaqueExtrinsic},
    *,
};
use std::sync::Arc;

/// Full client dependencies.
pub struct FullDeps<B, C, P> {
    /// The client instance to use.
    pub client: Arc<C>,
    /// Transaction pool instance.
    pub pool: Arc<P>,
    /// Narwhal Config Paths.
    pub narwhal_config: ConfigPaths,
    /// Marker for Block generic type.
    pub _marker: std::marker::PhantomData<B>,
}

#[docify::export]
/// Instantiate all full RPC extensions.
pub fn create_full<B, C, P>(
    deps: FullDeps<B, C, P>,
) -> Result<RpcModule<()>, Box<dyn std::error::Error + Send + Sync>>
where
    B: BlockT<Extrinsic = OpaqueExtrinsic>,
    C: Send
        + Sync
        + 'static
        + sp_api::ProvideRuntimeApi<OpaqueBlock>
        + HeaderBackend<OpaqueBlock>
        + HeaderMetadata<OpaqueBlock, Error = BlockChainError>
        + 'static,
    C::Api: sp_block_builder::BlockBuilder<OpaqueBlock>,
    C::Api: substrate_frame_rpc_system::AccountNonceApi<OpaqueBlock, AccountId, Nonce>,
    P: TransactionPool + 'static,
{
    use polkadot_sdk::substrate_frame_rpc_system::{System, SystemApiServer};
    let mut module = RpcModule::new(());
    let FullDeps {
        client,
        pool,
        narwhal_config,
        _marker,
    } = deps;

    module.merge(System::new(client.clone(), pool.clone()).into_rpc())?;
    module.merge(Dag::<B>::new(narwhal_config).into_rpc())?;

    Ok(module)
}
