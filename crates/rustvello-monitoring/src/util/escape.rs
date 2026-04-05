//! Centralized XML/HTML escape function.

/// Escape all five XML special characters for safe embedding in HTML and SVG.
pub fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_all_five_entities() {
        assert_eq!(
            xml_escape("a & b < c > d \" e ' f"),
            "a &amp; b &lt; c &gt; d &quot; e &apos; f"
        );
    }

    #[test]
    fn empty_string() {
        assert_eq!(xml_escape(""), "");
    }

    #[test]
    fn no_special_chars() {
        assert_eq!(xml_escape("hello world 123"), "hello world 123");
    }

    #[test]
    fn all_special() {
        assert_eq!(xml_escape("&<>\"'"), "&amp;&lt;&gt;&quot;&apos;");
    }
}
