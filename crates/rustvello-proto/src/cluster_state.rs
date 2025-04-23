// src/node_state/node_state.rs

use std::fmt;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::collections::HashMap;
use uuid::Uuid;

pub type AvailableNodes = HashMap<Uuid, NodeInfo>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub enum NodeStatus {
    #[default]
    Unknown,
    Leader,
    Follower,
    Initializing,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct NodeInfo {
    pub start_date: i64,       // Start time in UTC
    pub node_addr: SocketAddr, // Node address
}

#[serde_as]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NodeState {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub node_id: Uuid,
    pub node_info: NodeInfo,
    // Mapping of node_id to node_addr for known nodes
    pub available_nodes: AvailableNodes,
    #[serde_as(as = "Option<serde_with::DisplayFromStr>")]
    pub main_node_id: Option<Uuid>,
    // Current status of this node (Leader, Follower, Candidate)
    pub current_status: NodeStatus,
    // Common id of this cluster of nodes
    pub cluster_id: Option<Uuid>,
}

impl fmt::Display for NodeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "NodeState:")?;
        writeln!(f, "  Node ID: {}", self.node_id)?;
        writeln!(f, "  Node Info:")?;
        writeln!(f, "    - Start Date (UTC): {}", self.node_info.start_date)?;
        writeln!(f, "    - Node Address: {}", self.node_info.node_addr)?;

        writeln!(f, "  Available Nodes:")?;
        if self.available_nodes.is_empty() {
            writeln!(f, "    - None")?;
        } else {
            for (node_id, node_info) in &self.available_nodes {
                writeln!(f, "    - Node ID: {}", node_id)?;
                writeln!(f, "      - Start Date (UTC): {}", node_info.start_date)?;
                writeln!(f, "      - Node Address: {}", node_info.node_addr)?;
            }
        }

        match &self.main_node_id {
            Some(leader_id) => writeln!(f, "  Main Node ID (Leader): {}", leader_id)?,
            None => writeln!(f, "  Main Node ID (Leader): None")?,
        }

        writeln!(f, "  Current Status: {:?}", self.current_status)?;

        match &self.cluster_id {
            Some(cluster_id) => writeln!(f, "  Cluster ID: {}", cluster_id)?,
            None => writeln!(f, "  Cluster ID: None")?,
        }

        Ok(())
    }
}

impl NodeState {
    /// Check if the current node is the leader
    pub fn is_leader(&self) -> bool {
        self.current_status == NodeStatus::Leader
    }

    /// Check if the current node is a follower
    pub fn is_follower(&self) -> bool {
        self.current_status == NodeStatus::Follower
    }

    /// Get the addr from the available node
    pub fn get_node_addr(&self, node_id: Uuid) -> Result<SocketAddr, String> {
        match self.available_nodes.get(&node_id) {
            Some(node_info) => Ok(node_info.node_addr),
            None => Err(format!("Node Info not found for {node_id}")),
        }
    }

    /// Get leader_data
    /// Returns leader id and leader_info if any
    pub fn get_leader_data(&self) -> Option<(Uuid, &NodeInfo)> {
        if let Some(leader_id) = self.main_node_id {
            if let Some(leader_info) = self.available_nodes.get(&leader_id) {
                return Some((leader_id, leader_info));
            }
        }
        None
    }

    /// Function to check if the node is "lonely" (i.e., if the available_nodes is empty or only contains itself).
    pub fn is_lonely(&self) -> bool {
        self.available_nodes.is_empty()
            || (self.available_nodes.len() == 1 && self.available_nodes.contains_key(&self.node_id))
    }
}
