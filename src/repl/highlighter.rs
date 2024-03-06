use reedline::{ExampleHighlighter, Highlighter, StyledText};

const KEYWORDS: [&str; 8] = [
    "select", "from", "where", "limit", "order by", "with", "insert", "into",
];

// TODO: implement our own highlighter
#[allow(missing_debug_implementations)]
pub struct SQLKeywordHighlighter {
    inner: ExampleHighlighter,
}

impl Default for SQLKeywordHighlighter {
    fn default() -> Self {
        let inner = ExampleHighlighter::new(
            KEYWORDS.into_iter().map(|s| s.to_string()).collect(),
        );

        Self { inner }
    }
}

impl SQLKeywordHighlighter {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Highlighter for SQLKeywordHighlighter {
    fn highlight(&self, line: &str, cursor: usize) -> StyledText {
        self.inner.highlight(line, cursor)
    }
}
