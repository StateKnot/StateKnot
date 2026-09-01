// Copyright 2026 StateKnot contributors
// SPDX-License-Identifier: Apache-2.0

use thiserror::Error;

use crate::ProviderHttpOptions;

/// One decoded Server-Sent Event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SseEvent {
    pub(crate) event: Option<String>,
    pub(crate) data: String,
}

/// Incremental, resource-bounded WHATWG event-stream decoder.
pub(crate) struct SseDecoder {
    limits: ProviderHttpOptions,
    pending: Vec<u8>,
    data: String,
    event: Option<String>,
    total_bytes: usize,
}

impl SseDecoder {
    pub(crate) fn new(limits: ProviderHttpOptions) -> Self {
        Self {
            limits,
            pending: Vec::new(),
            data: String::new(),
            event: None,
            total_bytes: 0,
        }
    }

    pub(crate) fn push(&mut self, chunk: &[u8]) -> Result<Vec<SseEvent>, SseDecodeError> {
        self.total_bytes = self
            .total_bytes
            .checked_add(chunk.len())
            .ok_or(SseDecodeError::TotalTooLarge)?;
        if self.total_bytes > self.limits.maximum_sse_total_bytes() {
            return Err(SseDecodeError::TotalTooLarge);
        }
        self.pending.extend_from_slice(chunk);
        let mut events = Vec::new();
        loop {
            let Some((line_end, terminator_len)) = next_line(&self.pending) else {
                if self.pending.len() > self.limits.maximum_sse_line_bytes() {
                    return Err(SseDecodeError::LineTooLarge);
                }
                break;
            };
            if line_end > self.limits.maximum_sse_line_bytes() {
                return Err(SseDecodeError::LineTooLarge);
            }
            let line = self.pending[..line_end].to_vec();
            self.pending.drain(..line_end + terminator_len);
            self.consume_line(&line, &mut events)?;
        }
        Ok(events)
    }

    pub(crate) fn finish(mut self) -> Result<Vec<SseEvent>, SseDecodeError> {
        let mut events = Vec::new();
        if !self.pending.is_empty() {
            if self.pending.len() > self.limits.maximum_sse_line_bytes() {
                return Err(SseDecodeError::LineTooLarge);
            }
            let line = std::mem::take(&mut self.pending);
            self.consume_line(&line, &mut events)?;
        }
        if !self.data.is_empty() {
            self.dispatch(&mut events)?;
        }
        Ok(events)
    }

    fn consume_line(
        &mut self,
        line: &[u8],
        events: &mut Vec<SseEvent>,
    ) -> Result<(), SseDecodeError> {
        let line = std::str::from_utf8(line).map_err(|_| SseDecodeError::InvalidUtf8)?;
        if line.is_empty() {
            self.dispatch(events)?;
            return Ok(());
        }
        if line.starts_with(':') {
            return Ok(());
        }
        let (field, value) = line.split_once(':').map_or((line, ""), |(field, value)| {
            (field, value.strip_prefix(' ').unwrap_or(value))
        });
        match field {
            "event" => {
                validate_field(value)?;
                self.event = Some(value.to_owned());
            }
            "data" => {
                validate_field(value)?;
                let extra = value.len() + usize::from(!self.data.is_empty());
                if self.data.len().saturating_add(extra) > self.limits.maximum_sse_event_bytes() {
                    return Err(SseDecodeError::EventTooLarge);
                }
                if !self.data.is_empty() {
                    self.data.push('\n');
                }
                self.data.push_str(value);
            }
            _ => {}
        }
        Ok(())
    }

    fn dispatch(&mut self, events: &mut Vec<SseEvent>) -> Result<(), SseDecodeError> {
        if self.data.is_empty() {
            self.event = None;
        } else {
            if self.data.len() > self.limits.maximum_sse_event_bytes() {
                return Err(SseDecodeError::EventTooLarge);
            }
            events.push(SseEvent {
                event: self.event.take(),
                data: std::mem::take(&mut self.data),
            });
        }
        Ok(())
    }
}

fn next_line(input: &[u8]) -> Option<(usize, usize)> {
    input
        .iter()
        .position(|byte| matches!(byte, b'\r' | b'\n'))
        .map(|index| {
            if input[index] == b'\r' && input.get(index + 1) == Some(&b'\n') {
                (index, 2)
            } else {
                (index, 1)
            }
        })
}

fn validate_field(value: &str) -> Result<(), SseDecodeError> {
    if value.chars().any(|character| character == '\0') {
        Err(SseDecodeError::NullByte)
    } else {
        Ok(())
    }
}

/// Invalid or resource-exhausting event-stream framing.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum SseDecodeError {
    #[error("provider event stream exceeded its total byte ceiling")]
    TotalTooLarge,
    #[error("provider event stream line exceeded its byte ceiling")]
    LineTooLarge,
    #[error("provider event stream event exceeded its byte ceiling")]
    EventTooLarge,
    #[error("provider event stream was not valid UTF-8")]
    InvalidUtf8,
    #[error("provider event stream field contained a null byte")]
    NullByte,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_fragmented_crlf_comments_and_multiline_data() {
        let mut decoder = SseDecoder::new(ProviderHttpOptions::default());
        assert!(decoder.push(b": ping\r").unwrap().is_empty());
        assert!(
            decoder
                .push(b"\nevent: answer\r\ndata: {\"a\":\r\n")
                .unwrap()
                .is_empty()
        );
        let events = decoder.push(b"data: 1}\r\n\r\n").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event.as_deref(), Some("answer"));
        assert_eq!(events[0].data, "{\"a\":\n1}");
    }

    #[test]
    fn finish_dispatches_unterminated_event() {
        let mut decoder = SseDecoder::new(ProviderHttpOptions::default());
        assert!(decoder.push(b"data: done").unwrap().is_empty());
        assert_eq!(decoder.finish().unwrap()[0].data, "done");
    }
}
