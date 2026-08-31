//! SVG rendering for invocation family trees.
//!
//! Matches pynmon's `family_tree_svg.py` — time-ordered grid layout with
//! weighted column scoring, centered text, adaptive Bézier curves, and
//! saturated status-color fill for the focus node.

use std::collections::HashMap;
use std::fmt::Write;

use super::tree::FamilyTreeNode;
use crate::util::escape::xml_escape;
use crate::util::formatting::{format_duration_secs, truncate_chars};
use crate::util::status_colors;

const NODE_W: f64 = 300.0;
const NODE_H: f64 = 66.0;
const COL_GAP: f64 = 24.0;
const GAP_V: f64 = 12.0;
const PAD: f64 = 8.0;
const MAX_COLS: usize = 8;
const HIERARCHY_W: f64 = 28.0;
const TRUNC_H: f64 = 22.0;

/// Light tint per status (10% opacity approximations for non-focus nodes).
fn status_tint(status: &str) -> &'static str {
    match status {
        "Success" => "#e8f5e9",
        "Failed" => "#fdecea",
        "Running" => "#e3f2fd",
        "Pending" => "#fff8e1",
        "Registered" => "#f5f5f5",
        "Retry" => "#f3e5f5",
        "Paused" => "#e0f2f1",
        "Killed" => "#fdecea",
        "ConcurrencyControlled" => "#fff3e0",
        "ConcurrencyControlledFinal" => "#fdecea",
        "Rerouted" => "#e0f2f1",
        "PendingRecovery" => "#fff3e0",
        "RunningRecovery" => "#e3f2fd",
        _ => "#ffffff",
    }
}

/// Flattened node carrying original tree reference and parent.
struct FlatNode<'a> {
    node: &'a FamilyTreeNode,
    parent_id: Option<&'a str>,
}

/// Render a family tree as an SVG string (pynmon-style layout).
///
/// `focus_id` highlights the "current" invocation with saturated fill.
pub fn render_family_tree_svg(root: &FamilyTreeNode, focus_id: Option<&str>) -> String {
    // Flatten tree and build parent map
    let mut flat_nodes: Vec<FlatNode<'_>> = Vec::new();
    flatten_tree(root, None, &mut flat_nodes);

    if flat_nodes.is_empty() {
        return "<svg></svg>".to_owned();
    }

    // Sort globally by created_at (time-ordered layout like pynmon)
    flat_nodes.sort_by(|a, b| a.node.created_at.cmp(&b.node.created_at));

    // Assign positions using weighted column scoring
    let (x_positions, y_positions) = assign_positions(&flat_nodes);

    // Compute SVG viewport
    let (svg_w, svg_h) = svg_viewport(&flat_nodes, &x_positions, &y_positions);

    let mut buf = String::with_capacity(4096);

    // SVG header
    let _ = write!(
        buf,
        "<svg id=\"ft-svg\" xmlns=\"http://www.w3.org/2000/svg\" \
         width=\"{svg_w:.0}\" height=\"{svg_h:.0}\" \
         viewBox=\"0 0 {svg_w:.0} {svg_h:.0}\" \
         style=\"font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;\">"
    );

    // Hover CSS (inline, matching pynmon)
    buf.push_str("<style>");
    buf.push_str(".ft-node{transition:opacity .15s;cursor:pointer;-webkit-user-select:none;user-select:none}");
    buf.push_str(".ft-node text{pointer-events:none}");
    buf.push_str(".ft-edge{transition:opacity .15s,stroke-width .15s}");
    buf.push_str("#ft-group.ft-hover .ft-node{opacity:.25}");
    buf.push_str("#ft-group.ft-hover .ft-edge{opacity:.12}");
    buf.push_str("#ft-group.ft-hover .ft-node.ft-related{opacity:1}");
    buf.push_str("#ft-group.ft-hover .ft-edge.ft-related{opacity:1;stroke-width:3;stroke:#1976d2}");
    buf.push_str("</style>");

    let _ = write!(buf, "<g id=\"ft-group\">");

    // Build lookup by invocation_id
    let pos_lookup: HashMap<&str, (f64, f64)> = flat_nodes
        .iter()
        .map(|f| {
            let id = f.node.invocation_id.as_str();
            let cx = x_positions[id];
            let y = y_positions[id];
            (id, (cx, y))
        })
        .collect();

    // Render connections first (behind nodes)
    for flat in &flat_nodes {
        if let Some(parent_id) = flat.parent_id {
            if let (Some(&(pcx, py)), Some(&(ccx, cy))) = (
                pos_lookup.get(parent_id),
                pos_lookup.get(flat.node.invocation_id.as_str()),
            ) {
                render_connection(
                    &mut buf,
                    pcx,
                    py,
                    ccx,
                    cy,
                    parent_id,
                    &flat.node.invocation_id,
                );
            }
        }
    }

    // Render nodes
    let focus = focus_id.unwrap_or("");
    for flat in &flat_nodes {
        let id = flat.node.invocation_id.as_str();
        if let Some(&(cx, y)) = pos_lookup.get(id) {
            render_node(&mut buf, flat.node, cx, y, id == focus);
        }
    }

    buf.push_str("</g>");
    buf.push_str("</svg>");
    buf
}

/// Flatten tree into a list preserving parent linkage.
fn flatten_tree<'a>(
    node: &'a FamilyTreeNode,
    parent_id: Option<&'a str>,
    out: &mut Vec<FlatNode<'a>>,
) {
    out.push(FlatNode { node, parent_id });
    for child in &node.children {
        flatten_tree(child, Some(&node.invocation_id), out);
    }
}

/// Assign positions using pynmon's time-ordered weighted column scoring.
///
/// Nodes are sorted by `created_at`. Each node picks the column that
/// minimises `Y + distance_from_parent_column × HIERARCHY_W`.
fn assign_positions<'a>(
    flat_nodes: &'a [FlatNode<'a>],
) -> (HashMap<&'a str, f64>, HashMap<&'a str, f64>) {
    let num_nodes = flat_nodes.len();
    let num_cols = MAX_COLS.min(2.max((num_nodes as f64).sqrt().ceil() as usize));
    let col_step = NODE_W + COL_GAP;

    let mut col_bottoms = vec![0.0f64; num_cols];
    let mut min_y = 0.0f64;
    let mut x_positions: HashMap<&str, f64> = HashMap::with_capacity(num_nodes);
    let mut y_positions: HashMap<&str, f64> = HashMap::with_capacity(num_nodes);
    let mut node_cols: HashMap<&str, usize> = HashMap::with_capacity(num_nodes);

    for flat in flat_nodes {
        let id = flat.node.invocation_id.as_str();
        let pref_col = flat
            .parent_id
            .and_then(|pid| node_cols.get(pid).copied())
            .unwrap_or(0);

        let mut best_score = f64::INFINITY;
        let mut best_y = 0.0f64;
        let mut best_col = 0usize;

        for (col, col_bottom) in col_bottoms.iter().enumerate().take(num_cols) {
            let y = min_y.max(*col_bottom);
            let dist = (col as isize - pref_col as isize).unsigned_abs();
            let score = y + dist as f64 * HIERARCHY_W;
            if score < best_score || (score == best_score && y < best_y) {
                best_score = score;
                best_y = y;
                best_col = col;
            }
        }

        let cx = PAD + best_col as f64 * col_step + NODE_W / 2.0;
        x_positions.insert(id, cx);
        y_positions.insert(id, best_y);
        node_cols.insert(id, best_col);

        let node_h = NODE_H + if flat.node.truncated { TRUNC_H } else { 0.0 };
        col_bottoms[best_col] = best_y + node_h + GAP_V;
        min_y = best_y;
    }

    (x_positions, y_positions)
}

/// Compute SVG viewport size from positions.
fn svg_viewport(
    flat_nodes: &[FlatNode<'_>],
    x_positions: &HashMap<&str, f64>,
    y_positions: &HashMap<&str, f64>,
) -> (f64, f64) {
    let max_cx = x_positions.values().cloned().fold(0.0f64, f64::max);
    let max_x = max_cx + NODE_W / 2.0 + PAD;

    let max_bottom = flat_nodes.iter().fold(0.0f64, |acc, f| {
        let id = f.node.invocation_id.as_str();
        let y = y_positions.get(id).copied().unwrap_or(0.0);
        let h = NODE_H + if f.node.truncated { TRUNC_H } else { 0.0 };
        acc.max(y + h)
    });
    let max_y = max_bottom + PAD;

    (max_x.max(300.0), max_y)
}

/// Render a single tree node as an SVG group (pynmon style).
///
/// Focus node: saturated status-color fill, dark border, white text.
/// Normal node: light tint fill, subtle border, dark text.
///
/// Each node carries `data-status-color` / `data-status-tint` so that
/// client-side JS can toggle focus without re-fetching the SVG.
fn render_node(buf: &mut String, node: &FamilyTreeNode, cx: f64, y: f64, is_focus: bool) {
    let x = cx - NODE_W / 2.0;
    let color = status_colors::hex_color(&node.status);
    let status_name = xml_escape(&format!("{:?}", node.status));
    let inv_id = xml_escape(&node.invocation_id);
    let max_chars = (NODE_W / 7.0) as usize;
    let tint = status_tint(&status_name);

    let (bg, stroke, text_primary, text_secondary, text_muted) = if is_focus {
        (
            color,
            "stroke=\"#212529\" stroke-width=\"3\"".to_string(),
            "#ffffff",
            "rgba(255,255,255,0.85)",
            "rgba(255,255,255,0.7)",
        )
    } else {
        (
            tint,
            "stroke=\"#dee2e6\" stroke-width=\"1\"".to_owned(),
            "#212529",
            "#495057",
            "#6c757d",
        )
    };

    let mono = "font-family='SFMono-Regular,Menlo,Monaco,Consolas,monospace'";
    let sans = "font-family='-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif'";

    // Open node group — include data attrs for client-side focus toggling
    let focus_cls = if is_focus { " ft-focus" } else { "" };
    let _ = write!(
        buf,
        "<g class=\"ft-node{focus_cls}\" data-inv-id=\"{inv_id}\" \
         data-status-color=\"{color}\" data-status-tint=\"{tint}\" \
         style=\"cursor:pointer\">"
    );

    // Dashed focus outline — always rendered, hidden for non-focus nodes
    let outline_display = if is_focus { "" } else { " display=\"none\"" };
    let _ = write!(
        buf,
        "<rect class=\"ft-focus-ring\" x=\"{fx:.1}\" y=\"{fy:.1}\" width=\"{fw}\" height=\"{fh}\" \
         rx=\"6\" fill=\"none\" stroke=\"{color}\" stroke-width=\"2\" \
         stroke-dasharray=\"6,3\" opacity=\"0.6\"{outline_display}/>",
        fx = x - 3.0,
        fy = y - 3.0,
        fw = NODE_W + 6.0,
        fh = NODE_H + 6.0,
    );

    // Truncated nodes get dashed border
    let node_stroke = if node.truncated && !is_focus {
        "stroke=\"#90a4ae\" stroke-width=\"1.5\" stroke-dasharray=\"6,3\""
    } else {
        &stroke
    };

    // Main rect
    let _ = write!(
        buf,
        "<rect class=\"ft-bg\" x=\"{x:.1}\" y=\"{y:.1}\" width=\"{NODE_W}\" height=\"{NODE_H}\" \
         rx=\"4\" fill=\"{bg}\" {node_stroke}/>"
    );

    // Status indicator bar on the left
    let _ = write!(
        buf,
        "<rect x=\"{x:.1}\" y=\"{y:.1}\" width=\"4\" height=\"{NODE_H}\" \
         rx=\"2\" fill=\"{color}\"/>"
    );

    // Line 1: Invocation ID (monospace, centered)
    let _ = write!(
        buf,
        "<text class=\"ft-txt-sec\" x=\"{cx:.1}\" y=\"{ty:.1}\" text-anchor=\"middle\" \
         font-size=\"9.5\" {mono} fill=\"{text_secondary}\">{inv_id}</text>",
        ty = y + 14.0,
    );

    // Line 2: Module (centered)
    let module = xml_escape(truncate_chars(&node.task_module, max_chars));
    let _ = write!(
        buf,
        "<text class=\"ft-txt-mut\" x=\"{cx:.1}\" y=\"{ty:.1}\" text-anchor=\"middle\" \
         font-size=\"9\" {sans} fill=\"{text_muted}\">{module}</text>",
        ty = y + 28.0,
    );

    // Line 3: Function (bold, centered)
    let func = xml_escape(truncate_chars(&node.task_func, max_chars));
    let _ = write!(
        buf,
        "<text class=\"ft-txt-pri\" x=\"{cx:.1}\" y=\"{ty:.1}\" text-anchor=\"middle\" \
         font-size=\"11\" {sans} fill=\"{text_primary}\" font-weight=\"600\">{func}</text>",
        ty = y + 41.0,
    );

    // Line 4: Datetime + elapsed (centered)
    let time_str = node.created_at.format("%Y-%m-%d %H:%M:%S");
    let elapsed = format_duration_secs(node.elapsed_secs);
    let _ = write!(
        buf,
        "<text class=\"ft-txt-mut\" x=\"{cx:.1}\" y=\"{ty:.1}\" text-anchor=\"middle\" \
         font-size=\"9\" {sans} fill=\"{text_muted}\">{time_str} \u{23f1} {elapsed}</text>",
        ty = y + 55.0,
    );

    // Truncated "load more" badge (pynmon style)
    if node.truncated {
        let badge_w = 110.0;
        let badge_x = cx - badge_w / 2.0;
        let badge_y = y + NODE_H + 4.0;
        let _ = write!(
            buf,
            "<g class=\"ft-load-more\" data-expand-id=\"{inv_id}\" style=\"cursor:pointer\">\
             <rect x=\"{badge_x:.1}\" y=\"{badge_y:.1}\" width=\"{badge_w}\" height=\"18\" \
             rx=\"10\" fill=\"#f0f4f8\" stroke=\"#90caf9\" stroke-width=\"1\"/>\
             <text x=\"{cx:.1}\" y=\"{ty:.1}\" text-anchor=\"middle\" \
             font-size=\"10\" {sans} fill=\"#1976d2\">\u{25bc} load more</text></g>",
            ty = badge_y + 13.0,
        );
    }

    buf.push_str("</g>");
}

/// Render adaptive Bézier connection between parent and child (pynmon style).
///
/// Vertical S-curve when child is below parent; horizontal curve when at same row.
fn render_connection(
    buf: &mut String,
    parent_cx: f64,
    parent_y: f64,
    child_cx: f64,
    child_y: f64,
    parent_inv_id: &str,
    child_inv_id: &str,
) {
    let p_id = xml_escape(parent_inv_id);
    let c_id = xml_escape(child_inv_id);
    let attrs = format!(
        "fill=\"none\" stroke=\"#78909c\" stroke-width=\"2\" opacity=\"0.7\" \
         class=\"ft-edge\" data-parent-id=\"{p_id}\" data-child-id=\"{c_id}\""
    );
    let dot_r = 3;

    let parent_bottom = parent_y + NODE_H;
    let child_top = child_y;

    if child_top >= parent_bottom - 2.0 {
        // Child is below parent — vertical S-curve
        let mid_y = (parent_bottom + child_top) / 2.0;
        let _ = write!(
            buf,
            "<path d=\"M{parent_cx:.1},{parent_bottom:.1} \
             C{parent_cx:.1},{mid_y:.1} {child_cx:.1},{mid_y:.1} \
             {child_cx:.1},{child_top:.1}\" {attrs}/>"
        );
        let _ = write!(
            buf,
            "<circle cx=\"{parent_cx:.1}\" cy=\"{parent_bottom:.1}\" \
             r=\"{dot_r}\" fill=\"#78909c\" opacity=\"0.7\"/>"
        );
    } else {
        // Same row or child above — horizontal curve
        let (sx, ex) = if child_cx > parent_cx {
            (parent_cx + NODE_W / 2.0, child_cx - NODE_W / 2.0)
        } else {
            (parent_cx - NODE_W / 2.0, child_cx + NODE_W / 2.0)
        };
        let sy = parent_y + NODE_H * 0.7;
        let ey = child_y + NODE_H * 0.3;
        let mid_x = (sx + ex) / 2.0;
        let _ = write!(
            buf,
            "<path d=\"M{sx:.1},{sy:.1} \
             C{mid_x:.1},{sy:.1} {mid_x:.1},{ey:.1} \
             {ex:.1},{ey:.1}\" {attrs}/>"
        );
        let _ = write!(
            buf,
            "<circle cx=\"{sx:.1}\" cy=\"{sy:.1}\" \
             r=\"{dot_r}\" fill=\"#78909c\" opacity=\"0.7\"/>"
        );
    }
}
