use anyhow::{Context, Result};
use polkadot_sdk::sp_runtime::OpaqueExtrinsic;
use std::net::SocketAddr;

use crate::{process_transaction, AuxData};

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
