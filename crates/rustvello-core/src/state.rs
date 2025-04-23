// crates/rustvello/src/state.rs

use std::cmp;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;

use arc_swap::{ArcSwap, ArcSwapOption};
use imbl::{HashMap, HashSet, OrdMap, Vector};
use uuid::Uuid;

use rustvello_proto::*;

pub struct State {
    nodes: Arc<ArcSwap<HashMap<Uuid, NodeInfo>>>,
    element_kinds: OnceLock<Arc<HashSet<String>>>,
    registered_metrics: OnceLock<Arc<HashMap<String, Arc<MetricDefinition>>>>,
    metric_order: Arc<ArcSwap<Vector<Arc<MetricDefinition>>>>,
    min_element_id: Arc<ArcSwapOption<ElementId>>,
    max_element_id: Arc<ArcSwapOption<ElementId>>,
    range_to_node: Arc<ArcSwap<OrdMap<OrdRangeInc, Uuid>>>,
    finest_resolution: AtomicU8,
    element_id_map: Arc<ArcSwap<HashMap<LocalElementId, ElementId>>>,
}

impl Default for State {
    fn default() -> Self {
        Self::new(TimestampResolution::default())
    }
}

impl State {
    pub fn new(default_resolution: TimestampResolution) -> Self {
        Self {
            nodes: Arc::new(ArcSwap::from_pointee(HashMap::new())),
            element_kinds: OnceLock::new(),
            registered_metrics: OnceLock::new(),
            metric_order: Arc::new(ArcSwap::from_pointee(Vector::new())),
            min_element_id: Arc::new(ArcSwapOption::empty()),
            max_element_id: Arc::new(ArcSwapOption::empty()),
            range_to_node: Arc::new(ArcSwap::from_pointee(OrdMap::new())),
            finest_resolution: default_resolution.as_u8().into(),
            element_id_map: Arc::new(ArcSwap::from_pointee(HashMap::new())),
        }
    }

    /// Update the nodes
    pub async fn update_nodes(&self, new_nodes: HashMap<Uuid, NodeInfo>) {
        self.nodes.store(Arc::new(new_nodes));
    }

    pub async fn get_nodes(&self) -> HashMap<Uuid, NodeInfo> {
        let nodes = self.nodes.load();
        (**nodes).clone()
    }

    /// Inits `element_kinds` only once. Subsequent calls will return an error.
    pub async fn init_element_kinds(&self, element_kinds: &[ElementKindRegistration]) {
        let codes_set = element_kinds
            .iter()
            .map(|kind| kind.code.clone())
            .collect::<HashSet<String>>();
        let _ = self.element_kinds.set(Arc::new(codes_set));
    }

    /// Check if an element kind code is valid.
    pub fn is_valid_element_kind_code(&self, element_kind_code: &str) -> bool {
        if let Some(kinds) = self.element_kinds.get() {
            kinds.contains(element_kind_code)
        } else {
            false
        }
    }

    /// Inits `registered_metrics` only once. Subsequent calls will return an error.
    pub async fn init_metrics(&self, metric_definitions: &[MetricDefinition]) {
        let metrics = metric_definitions
            .iter()
            .map(|m| (m.code.clone(), Arc::new(m.clone()))) // Map code to an Arc<MetricDefinition>
            .collect::<HashMap<String, Arc<MetricDefinition>>>();

        // Attempt to set `registered_metrics` only once
        let _ = self.registered_metrics.set(Arc::new(metrics));
    }

    /// Check if a metric code is valid.
    pub fn is_valid_metric_code(&self, metric_code: &str) -> bool {
        // Load the Arc<HashMap> from `registered_metrics` and check for the presence of the code
        self.registered_metrics
            .get()
            .map_or(false, |metrics| metrics.contains_key(metric_code))
    }

    /// Update `metric_order` atomically with a new order.
    /// ! This is only updated in one tasks, no concurrency issues
    /// * It can safely being read from multiple threads
    pub async fn update_metric_order(&self, metric_order: Vec<MetricDefinition>) {
        let ordered_metrics = metric_order
            .into_iter()
            .map(Arc::new) // Wrap each MetricDefinition in an Arc
            .collect::<Vector<_>>(); // Collect into an imbl Vector
        self.metric_order.store(Arc::new(ordered_metrics));
    }

    // Retrieve a Vec<&MetricDefinition> slice for cases where that's required.
    /// Returns a reference to the ordered metric definitions as a slice.
    pub fn get_metric_order(&self) -> Arc<Vector<Arc<MetricDefinition>>> {
        self.metric_order.load_full()
    }

    /// Updates `min_element_id` and `max_element_id` based on the provided `element_id`.
    /// If either is `None`, it will set both to the `element_id`.
    /// Otherwise, it updates `min_element_id` if `element_id` is smaller,
    /// and `max_element_id` if `element_id` is larger.
    pub async fn update_min_max_element_id(&self, element_id: ElementId) {
        self.min_element_id.rcu(|current_min| {
            Some(Arc::new(match current_min.as_deref() {
                Some(&min_id) => cmp::min(min_id, element_id),
                None => element_id,
            }))
        });

        self.max_element_id.rcu(|current_max| {
            Some(Arc::new(match current_max.as_deref() {
                Some(&max_id) => cmp::max(max_id, element_id),
                None => element_id,
            }))
        });
    }

    pub async fn get_element_id_range(&self) -> (Option<ElementId>, Option<ElementId>) {
        let min_id = self.min_element_id.load_full().as_deref().cloned();
        let max_id = self.max_element_id.load_full().as_deref().cloned();
        (min_id, max_id)
    }

    pub async fn update_node_elem_ranges(&self, ranges: &[NodeElementRange]) {
        self.range_to_node.rcu(|current| {
            let mut new_map = (**current).clone();
            for node_range in ranges {
                new_map.insert(node_range.range.clone(), node_range.node_id);
            }
            Arc::new(new_map)
        });
    }

    pub async fn get_node_elem_ranges(&self) -> OrdMap<OrdRangeInc, Uuid> {
        let ranges = self.range_to_node.load();
        (**ranges).clone()
    }

    /// Find the node ID corresponding to a given element ID
    pub fn find_node(&self, element_id: u64) -> Option<Uuid> {
        let map = self.range_to_node.load();
        let bound = OrdRangeInc::new_bound(element_id);

        // Find the last range that starts before or at `element_id`
        map.range(..=bound).last().and_then(|(range, node_id)| {
            if range.contains(&element_id) {
                Some(*node_id)
            } else {
                None
            }
        })
    }

    /// Find the node address corresponding to a given element ID
    pub fn find_element_node_addr(&self, element_id: u64) -> Option<SocketAddr> {
        let node_id = self.find_node(element_id)?;
        let nodes = self.nodes.load();
        nodes.get(&node_id).map(|node_info| node_info.node_addr)
    }

    /// Update `finest_resolution` atomically.
    pub async fn update_finest_resolution(&self, finest_resolution: TimestampResolution) {
        self.finest_resolution
            .store(finest_resolution.as_u8(), Ordering::SeqCst);
    }

    /// Retrieve the current `finest_resolution` as `TimestampResolution`.
    pub fn get_finest_resolution(&self) -> TimestampResolution {
        TimestampResolution::from_u8(self.finest_resolution.load(Ordering::SeqCst))
    }

    pub async fn update_element_id(&self, local_id: LocalElementId, element_id: ElementId) {
        self.element_id_map.rcu(|current| {
            let mut new_map = (**current).clone();
            new_map.insert(local_id, element_id);
            Arc::new(new_map)
        });
    }

    // Retrieve an ElementId from a LocalElementId
    pub fn get_element_id(&self, local_id: &LocalElementId) -> Option<ElementId> {
        let element_map = self.element_id_map.load();
        element_map.get(local_id).cloned()
    }
}
