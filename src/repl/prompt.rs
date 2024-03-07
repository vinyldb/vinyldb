use colored::Colorize;
use reedline::{Prompt, PromptEditMode, PromptHistorySearch};
use std::borrow::Cow;

#[derive(Debug, Copy, Clone)]
pub struct VinylPrompt {
    last_cmd_succeed: bool,
}

impl VinylPrompt {
    const PROMPT: &'static str = "V ";

    pub fn new(last_cmd_succeed: bool) -> Self {
        VinylPrompt { last_cmd_succeed }
    }
}

impl Prompt for VinylPrompt {
    fn render_prompt_left(&self) -> Cow<str> {
        Cow::Owned(
            if self.last_cmd_succeed {
                Self::PROMPT.green()
            } else {
                Self::PROMPT.red()
            }
            .to_string(),
        )
    }

    fn render_prompt_right(&self) -> Cow<str> {
        Cow::Borrowed("")
    }

    fn render_prompt_indicator(
        &self,
        _prompt_mode: PromptEditMode,
    ) -> Cow<str> {
        Cow::Borrowed("")
    }

    fn render_prompt_multiline_indicator(&self) -> Cow<str> {
        Cow::Borrowed("")
    }

    fn render_prompt_history_search_indicator(
        &self,
        _history_search: PromptHistorySearch,
    ) -> Cow<str> {
        Cow::Borrowed("")
    }
}
