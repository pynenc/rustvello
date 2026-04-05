//! Color assignment for timeline runners using golden ratio HSL distribution.

use std::collections::HashMap;

/// An RGB color.
#[derive(Debug, Clone)]
pub struct Color {
    pub r: u8,
    pub g: u8,
    pub b: u8,
}

impl Color {
    pub fn new(r: u8, g: u8, b: u8) -> Self {
        Self { r, g, b }
    }

    /// Convert to hex string like "#1a2b3c".
    pub fn to_hex(&self) -> String {
        format!("#{:02x}{:02x}{:02x}", self.r, self.g, self.b)
    }

    /// Create from HSL values (h: 0..1, s: 0..1, l: 0..1).
    pub fn from_hsl(h: f64, s: f64, l: f64) -> Self {
        let (r, g, b) = hsl_to_rgb(h, s, l);
        Self {
            r: (r * 255.0) as u8,
            g: (g * 255.0) as u8,
            b: (b * 255.0) as u8,
        }
    }
}

/// Convert HSL to RGB. All values in 0..1.
fn hsl_to_rgb(h: f64, s: f64, l: f64) -> (f64, f64, f64) {
    if s == 0.0 {
        return (l, l, l);
    }
    let q = if l < 0.5 {
        l * (1.0 + s)
    } else {
        l + s - l * s
    };
    let p = 2.0 * l - q;
    let r = hue_to_rgb(p, q, h + 1.0 / 3.0);
    let g = hue_to_rgb(p, q, h);
    let b = hue_to_rgb(p, q, h - 1.0 / 3.0);
    (r, g, b)
}

fn hue_to_rgb(p: f64, q: f64, t: f64) -> f64 {
    let mut t = t;
    if t < 0.0 {
        t += 1.0;
    }
    if t > 1.0 {
        t -= 1.0;
    }
    if t < 1.0 / 6.0 {
        return p + (q - p) * 6.0 * t;
    }
    if t < 1.0 / 2.0 {
        return q;
    }
    if t < 2.0 / 3.0 {
        return p + (q - p) * (2.0 / 3.0 - t) * 6.0;
    }
    p
}

/// Golden ratio constant for maximum visual separation.
const GOLDEN_RATIO: f64 = 0.618_033_988_749_895;

/// Assigns visually distinct colors to hosts/runners using golden ratio hue spacing.
#[derive(Debug)]
pub struct HostColorAssigner {
    next_hue: f64,
    saturation: f64,
    lightness: f64,
    cache: HashMap<String, Color>,
}

impl Default for HostColorAssigner {
    fn default() -> Self {
        Self {
            next_hue: 0.0,
            saturation: 0.65,
            lightness: 0.50,
            cache: HashMap::new(),
        }
    }
}

impl HostColorAssigner {
    /// Get or assign a color for a host/runner ID.
    pub fn color_for(&mut self, host_id: &str) -> Color {
        if let Some(c) = self.cache.get(host_id) {
            return c.clone();
        }
        let color = Color::from_hsl(self.next_hue, self.saturation, self.lightness);
        self.next_hue = (self.next_hue + GOLDEN_RATIO) % 1.0;
        self.cache.insert(host_id.to_owned(), color.clone());
        color
    }
}

/// Generates shade variants for workers belonging to the same host.
#[derive(Debug)]
pub struct WorkerShadeGenerator {
    base: Color,
}

impl WorkerShadeGenerator {
    pub fn new(base: Color) -> Self {
        Self { base }
    }

    /// Generate a shade variant based on the worker index.
    /// Even indices get lighter, odd indices get darker.
    pub fn shade(&self, index: usize) -> Color {
        let offset = ((index / 2) + 1) as f64 * 15.0;
        let sign = if index % 2 == 0 { 1.0 } else { -1.0 };
        Color::new(
            clamp_u8(self.base.r as f64 + sign * offset),
            clamp_u8(self.base.g as f64 + sign * offset),
            clamp_u8(self.base.b as f64 + sign * offset),
        )
    }
}

fn clamp_u8(v: f64) -> u8 {
    v.clamp(0.0, 255.0) as u8
}

/// Caches host and worker colors for consistent rendering.
#[derive(Debug, Default)]
pub struct ColorScheme {
    host_assigner: HostColorAssigner,
    shade_cache: HashMap<String, Color>,
}

impl ColorScheme {
    /// Get a color for a runner, using the parent host color as base.
    /// `runner_id` format is typically "host-worker" or just a plain ID.
    pub fn color_for_runner(&mut self, runner_id: &str) -> Color {
        if let Some(c) = self.shade_cache.get(runner_id) {
            return c.clone();
        }
        // Use the full runner_id as the host key (no host/worker splitting)
        let color = self.host_assigner.color_for(runner_id);
        self.shade_cache.insert(runner_id.to_owned(), color.clone());
        color
    }

    /// Get the hex color string for a runner.
    pub fn hex_for_runner(&mut self, runner_id: &str) -> String {
        self.color_for_runner(runner_id).to_hex()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_golden_ratio_distribution() {
        let mut assigner = HostColorAssigner::default();
        let c1 = assigner.color_for("host1");
        let c2 = assigner.color_for("host2");
        let c3 = assigner.color_for("host3");
        // Colors should be different
        assert_ne!(c1.to_hex(), c2.to_hex());
        assert_ne!(c2.to_hex(), c3.to_hex());
        // Same host should return same color
        assert_eq!(assigner.color_for("host1").to_hex(), c1.to_hex());
    }

    #[test]
    fn test_hsl_to_rgb_red() {
        let c = Color::from_hsl(0.0, 1.0, 0.5);
        assert_eq!(c.r, 255);
        assert_eq!(c.g, 0);
        assert_eq!(c.b, 0);
    }

    #[test]
    fn test_shade_generator() {
        let base = Color::new(128, 128, 128);
        let gen = WorkerShadeGenerator::new(base);
        let s0 = gen.shade(0); // lighter
        let s1 = gen.shade(1); // darker
        assert!(s0.r > 128);
        assert!(s1.r < 128);
    }

    #[test]
    fn test_color_scheme_consistency() {
        let mut scheme = ColorScheme::default();
        let hex1 = scheme.hex_for_runner("runner-1");
        let hex2 = scheme.hex_for_runner("runner-1");
        assert_eq!(hex1, hex2);
    }
}
