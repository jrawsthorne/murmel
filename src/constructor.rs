//
// Copyright 2018-2019 Tamas Blummer
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//!
//! # Construct the Murmel stack
//!
//! Assembles modules of this library to a complete service
//!

use bitcoin::{
    network::{
        constants::{ServiceFlags, Network}
    }
};
use crate::chaindb::{ChainDB, SharedChainDB};
use crate::dispatcher::Dispatcher;
use crate::dns::dns_seed;
use crate::error::Error;
use futures::{
    executor::{ThreadPool, ThreadPoolBuilder},
    future,
    Poll as Async,
    FutureExt,
    task::{SpawnExt},
};
use crate::headerdownload::HeaderDownload;
use crate::p2p::{P2P, P2PControl, PeerMessageSender, PeerSource};
use crate::ping::Ping;
use rand::{RngCore, thread_rng};
use std::{
    net::SocketAddr,
    path::Path,
    sync::{Arc, mpsc, Mutex, RwLock, atomic::AtomicUsize},
};
use crate::timeout::Timeout;
use crate::downstream::{DownStreamDummy, SharedDownstream};
use bitcoin::network::message::NetworkMessage;
use bitcoin::network::message::RawNetworkMessage;
use crate::{addrman::AddressManager, p2p::{ConnectionType, BitcoinP2PConfig}};
use std::time::Duration;

const MAX_PROTOCOL_VERSION: u32 = 70001;
const USER_AGENT: &'static str = concat!("/Murmel:", env!("CARGO_PKG_VERSION"), '/');

/// The complete stack
pub struct Constructor {
    p2p: Arc<P2P<NetworkMessage, RawNetworkMessage, BitcoinP2PConfig>>,
    /// this should be accessed by Lightning
    pub downstream: SharedDownstream
}

impl Constructor {
    /// open DBs
    pub fn open_db(path: Option<&Path>, network: Network, _birth: u64) -> Result<SharedChainDB, Error> {
        let mut chaindb =
            if let Some(path) = path {
                ChainDB::new(path, network)?
            } else {
                ChainDB::mem(network)?
            };
        chaindb.init()?;
        Ok(Arc::new(RwLock::new(chaindb)))
    }

    /// Construct the stack
    pub fn new(network: Network, listen: Vec<SocketAddr>, chaindb: SharedChainDB) -> Result<Constructor, Error> {
        const BACK_PRESSURE: usize = 10;

        let (to_dispatcher, from_p2p) = mpsc::sync_channel(BACK_PRESSURE);


        let p2pconfig = BitcoinP2PConfig {
            network,
            nonce: thread_rng().next_u64(),
            max_protocol_version: MAX_PROTOCOL_VERSION,
            user_agent: USER_AGENT.to_owned(),
            height: AtomicUsize::new(0),
            server: !listen.is_empty()
        };

        let (p2p, p2p_control) =
            P2P::new(p2pconfig, PeerMessageSender::new(to_dispatcher), BACK_PRESSURE);

        let downstream = Arc::new(Mutex::new(DownStreamDummy {}));

        let timeout = Arc::new(Mutex::new(Timeout::new(p2p_control.clone())));

        let mut dispatcher = Dispatcher::new(from_p2p);

        dispatcher.add_listener(HeaderDownload::new(chaindb.clone(), p2p_control.clone(), timeout.clone(), downstream.clone()));
        dispatcher.add_listener(Ping::new(p2p_control.clone(), timeout.clone()));

        for addr in &listen {
            p2p_control.send(P2PControl::Bind(addr.clone()));
        }

        Ok(Constructor { p2p, downstream })
    }

    /// Run the stack. This should be called AFTER registering listener of the ChainWatchInterface,
    /// so they are called as the stack catches up with the blockchain
    /// * peers - connect to these peers at startup (might be empty)
    /// * min_connections - keep connections with at least this number of peers. Peers will be randomly chosen
    /// from those discovered in earlier runs
    pub fn run(&mut self, network: Network, peers: Vec<SocketAddr>, min_connections: usize) -> Result<(), Error> {

        let mut executor = ThreadPoolBuilder::new().name_prefix("bitcoin-connect").pool_size(2).create().expect("can not start futures thread pool");

        let p2p = self.p2p.clone();

        // manual connections should support compact filters
        for addr in &peers {
            executor.spawn(p2p.add_peer("bitcoin", PeerSource::Outgoing(addr.clone(), ConnectionType::Filter)).map(|_|())).expect("can not spawn task for peers");
        }

        let mut keep_connected = KeepConnected {
            min_block_connections: min_connections,
            min_filter_connections: min_connections,
            p2p: self.p2p.clone(),
            address_manager: AddressManager::new(),
            cex: executor.clone(),
            network,
        };
        
        std::thread::spawn(move || keep_connected.run());

        let p2p = self.p2p.clone();
        let mut cex = executor.clone();
        executor.run(future::poll_fn(move |_| {
            p2p.poll_events("bitcoin", &mut cex);
            Async::Ready(())
        }));
        Ok(())
    }
}

#[derive(Clone)]
struct KeepConnected {
    cex: ThreadPool,
    address_manager: AddressManager,
    p2p: Arc<P2P<NetworkMessage, RawNetworkMessage, BitcoinP2PConfig>>,
    min_block_connections: usize,
    min_filter_connections: usize,
    network: Network
}

impl KeepConnected {
    pub fn run(&mut self) {

        let filter_addrs = dns_seed(self.network, Some(ServiceFlags::WITNESS | ServiceFlags::COMPACT_FILTERS));
        let block_addrs = dns_seed(self.network, Some(ServiceFlags::WITNESS | ServiceFlags::NETWORK));

        for addr in filter_addrs {
            self.address_manager.add(addr, ServiceFlags::WITNESS | ServiceFlags::COMPACT_FILTERS);
        }

        for addr in block_addrs {
            self.address_manager.add(addr, ServiceFlags::WITNESS | ServiceFlags::NETWORK);
        }

        loop {
            let block_needed = self
                .min_block_connections
                .saturating_sub(self.p2p.n_outbound_peers(ConnectionType::Block));

            let filter_needed = self
                .min_block_connections
                .saturating_sub(self.p2p.n_outbound_peers(ConnectionType::Filter));

            for _ in 0..block_needed {
                self.add_peer(ConnectionType::Block);
            }

            for _ in 0..filter_needed {
                self.add_peer(ConnectionType::Filter);
            }

            std::thread::sleep(Duration::from_secs(10));
        }
    }

    fn add_peer(&mut self, connection_type: ConnectionType) {
        let addr = match connection_type {
            ConnectionType::Block => self.address_manager.get_full_block_addr(),
            ConnectionType::Filter => self.address_manager.get_compact_filter_addr()
        };
        if let Some(addr) = addr {
            let add = self
                .p2p
                .add_peer("bitcoin", PeerSource::Outgoing(addr, connection_type))
                .map(|_| ());
            self
                .cex
                .spawn(add)
                .expect("can not add peer for outgoing connection");
        }
    }
}