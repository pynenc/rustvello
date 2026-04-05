//! Invocation family tree: tree building and SVG rendering.

pub mod render;
pub mod tree;

pub use render::render_family_tree_svg;
pub use tree::{build_family_tree, FamilyTreeNode};
