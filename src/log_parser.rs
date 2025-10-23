// Copyright 2022 Blockdaemon Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Log Parser Module
//!
//! This module parses Solana transaction log messages to extract:
//! 1. Program execution logs (INVOKE, LOG, DATA, SUCCESS, FAILED, CONSUMED)
//! 2. Anchor events and custom program events
//!
//! Log message format patterns:
//! - "Program <program_id> invoke [<depth>]"
//! - "Program <program_id> success"
//! - "Program <program_id> failed: <error>"
//! - "Program <program_id> consumed <units> of <total> compute units"
//! - "Program log: <message>"
//! - "Program data: <base64_data>"

use {
    base64::{engine::general_purpose::STANDARD as BASE64, Engine},
    std::collections::HashSet,
};

/// Type of program log
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogType {
    Unknown = 0,
    Invoke = 1,
    Log = 2,
    Data = 3,
    Success = 4,
    Failed = 5,
    Consumed = 6,
}

/// Parsed program log entry
#[derive(Debug, Clone)]
pub struct ProgramLog {
    pub log_type: LogType,
    pub program_id: String,
    pub message: String,
    pub depth: u32,
    pub data: Option<Vec<u8>>,
    pub error: Option<String>,
}

/// Parsed Anchor/custom event
#[derive(Debug, Clone)]
pub struct ChainEvent {
    pub program_id: String,
    pub discriminator: String,
    pub data: Vec<u8>,
    pub instruction_index: u32,
}

/// Program call stack entry
#[derive(Debug, Clone)]
struct CallStackEntry {
    program_id: String,
    depth: u32,
    instruction_index: u32,
}

/// Log Parser
pub struct LogParser {
    /// Event discriminators to capture (8-byte hex strings)
    event_discriminators: HashSet<[u8; 8]>,
    /// Whether to extract all program logs
    extract_program_logs: bool,
    /// Whether to parse Anchor events
    extract_anchor_events: bool,
}

impl LogParser {
    /// Create a new LogParser
    pub fn new(
        event_discriminators: HashSet<[u8; 8]>,
        extract_program_logs: bool,
        extract_anchor_events: bool,
    ) -> Self {
        Self {
            event_discriminators,
            extract_program_logs,
            extract_anchor_events,
        }
    }

    /// Parse transaction log messages
    pub fn parse_logs(
        &self,
        log_messages: &[String],
    ) -> (Vec<ProgramLog>, Vec<ChainEvent>) {
        let mut program_logs = Vec::new();
        let mut chain_events = Vec::new();
        let mut call_stack: Vec<CallStackEntry> = Vec::new();
        let mut instruction_index = 0u32;

        for log_message in log_messages {
            let log_message = log_message.trim();

            // Skip empty lines
            if log_message.is_empty() {
                continue;
            }

            // Parse different log types
            if log_message.starts_with("Program ") {
                if log_message.contains(" invoke [") {
                    // "Program <program_id> invoke [<depth>]"
                    if let Some(parsed) = self.parse_invoke(log_message) {
                        call_stack.push(CallStackEntry {
                            program_id: parsed.program_id.clone(),
                            depth: parsed.depth,
                            instruction_index,
                        });

                        if self.extract_program_logs {
                            program_logs.push(parsed);
                        }

                        // Increment instruction index for top-level invocations
                        if call_stack.len() == 1 {
                            instruction_index += 1;
                        }
                    }
                } else if log_message.contains(" success") {
                    // "Program <program_id> success"
                    if let Some(parsed) = self.parse_success(log_message, &call_stack) {
                        if self.extract_program_logs {
                            program_logs.push(parsed);
                        }
                        call_stack.pop();
                    }
                } else if log_message.contains(" failed:") {
                    // "Program <program_id> failed: <error>"
                    if let Some(parsed) = self.parse_failed(log_message, &call_stack) {
                        if self.extract_program_logs {
                            program_logs.push(parsed);
                        }
                        call_stack.pop();
                    }
                } else if log_message.contains(" consumed ") {
                    // "Program <program_id> consumed <units> of <total> compute units"
                    if let Some(parsed) = self.parse_consumed(log_message, &call_stack) {
                        if self.extract_program_logs {
                            program_logs.push(parsed);
                        }
                    }
                } else if log_message.starts_with("Program log: ") {
                    // "Program log: <message>"
                    if let Some(parsed) = self.parse_program_log(log_message, &call_stack) {
                        if self.extract_program_logs {
                            program_logs.push(parsed);
                        }
                    }
                } else if log_message.starts_with("Program data: ") {
                    // "Program data: <base64_data>"
                    if let Some(parsed) = self.parse_program_data(log_message, &call_stack) {
                        if self.extract_program_logs {
                            program_logs.push(parsed.clone());
                        }

                        // Try to extract event if enabled
                        if self.extract_anchor_events {
                            if let Some(event) = self.extract_event(&parsed, &call_stack, instruction_index) {
                                chain_events.push(event);
                            }
                        }
                    }
                }
            }
        }

        (program_logs, chain_events)
    }

    /// Parse "Program <program_id> invoke [<depth>]"
    fn parse_invoke(&self, log: &str) -> Option<ProgramLog> {
        // Format: "Program <program_id> invoke [<depth>]"
        let parts: Vec<&str> = log.split_whitespace().collect();
        if parts.len() >= 4 && parts[0] == "Program" && parts[2] == "invoke" {
            let program_id = parts[1].to_string();
            let depth_str = parts[3].trim_start_matches('[').trim_end_matches(']');
            let depth = depth_str.parse::<u32>().unwrap_or(1);

            return Some(ProgramLog {
                log_type: LogType::Invoke,
                program_id,
                message: log.to_string(),
                depth,
                data: None,
                error: None,
            });
        }
        None
    }

    /// Parse "Program <program_id> success"
    fn parse_success(&self, log: &str, call_stack: &[CallStackEntry]) -> Option<ProgramLog> {
        let parts: Vec<&str> = log.split_whitespace().collect();
        if parts.len() >= 3 && parts[0] == "Program" && parts[2] == "success" {
            let program_id = parts[1].to_string();
            let depth = call_stack.last().map(|e| e.depth).unwrap_or(1);

            return Some(ProgramLog {
                log_type: LogType::Success,
                program_id,
                message: log.to_string(),
                depth,
                data: None,
                error: None,
            });
        }
        None
    }

    /// Parse "Program <program_id> failed: <error>"
    fn parse_failed(&self, log: &str, call_stack: &[CallStackEntry]) -> Option<ProgramLog> {
        // Format: "Program <program_id> failed: <error message>"
        if let Some(failed_pos) = log.find(" failed: ") {
            let parts: Vec<&str> = log[..failed_pos].split_whitespace().collect();
            if parts.len() >= 2 && parts[0] == "Program" {
                let program_id = parts[1].to_string();
                let error = log[failed_pos + 9..].to_string();
                let depth = call_stack.last().map(|e| e.depth).unwrap_or(1);

                return Some(ProgramLog {
                    log_type: LogType::Failed,
                    program_id,
                    message: log.to_string(),
                    depth,
                    data: None,
                    error: Some(error),
                });
            }
        }
        None
    }

    /// Parse "Program <program_id> consumed <units> of <total> compute units"
    fn parse_consumed(&self, log: &str, call_stack: &[CallStackEntry]) -> Option<ProgramLog> {
        let parts: Vec<&str> = log.split_whitespace().collect();
        if parts.len() >= 6 && parts[0] == "Program" && parts[2] == "consumed" {
            let program_id = parts[1].to_string();
            let depth = call_stack.last().map(|e| e.depth).unwrap_or(1);

            return Some(ProgramLog {
                log_type: LogType::Consumed,
                program_id,
                message: log.to_string(),
                depth,
                data: None,
                error: None,
            });
        }
        None
    }

    /// Parse "Program log: <message>"
    fn parse_program_log(&self, log: &str, call_stack: &[CallStackEntry]) -> Option<ProgramLog> {
        if let Some(message) = log.strip_prefix("Program log: ") {
            let (program_id, depth) = if let Some(entry) = call_stack.last() {
                (entry.program_id.clone(), entry.depth)
            } else {
                ("unknown".to_string(), 0)
            };

            return Some(ProgramLog {
                log_type: LogType::Log,
                program_id,
                message: message.to_string(),
                depth,
                data: None,
                error: None,
            });
        }
        None
    }

    /// Parse "Program data: <base64_data>"
    fn parse_program_data(&self, log: &str, call_stack: &[CallStackEntry]) -> Option<ProgramLog> {
        if let Some(data_str) = log.strip_prefix("Program data: ") {
            let (program_id, depth) = if let Some(entry) = call_stack.last() {
                (entry.program_id.clone(), entry.depth)
            } else {
                ("unknown".to_string(), 0)
            };

            // Try to decode base64
            let data = BASE64.decode(data_str.as_bytes()).ok();

            return Some(ProgramLog {
                log_type: LogType::Data,
                program_id,
                message: data_str.to_string(),
                depth,
                data,
                error: None,
            });
        }
        None
    }

    /// Extract Anchor event from program data
    fn extract_event(
        &self,
        program_log: &ProgramLog,
        call_stack: &[CallStackEntry],
        instruction_index: u32,
    ) -> Option<ChainEvent> {
        // Get decoded data
        let data = program_log.data.as_ref()?;

        // Anchor events start with 8-byte discriminator
        if data.len() < 8 {
            return None;
        }

        let discriminator: [u8; 8] = data[0..8].try_into().ok()?;

        // Check if we should capture this event
        if !self.event_discriminators.is_empty() && !self.event_discriminators.contains(&discriminator) {
            return None;
        }

        // Convert discriminator to hex string
        let discriminator_hex = hex::encode(discriminator);

        // Get current instruction index from call stack
        let current_instruction_index = call_stack
            .last()
            .map(|e| e.instruction_index)
            .unwrap_or(instruction_index);

        Some(ChainEvent {
            program_id: program_log.program_id.clone(),
            discriminator: discriminator_hex,
            data: data.to_vec(),
            instruction_index: current_instruction_index,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_invoke() {
        let parser = LogParser::new(HashSet::new(), true, false);
        let log = "Program 11111111111111111111111111111111 invoke [1]";
        let result = parser.parse_invoke(log);

        assert!(result.is_some());
        let parsed = result.unwrap();
        assert_eq!(parsed.log_type, LogType::Invoke);
        assert_eq!(parsed.program_id, "11111111111111111111111111111111");
        assert_eq!(parsed.depth, 1);
    }

    #[test]
    fn test_parse_program_log() {
        let parser = LogParser::new(HashSet::new(), true, false);
        let call_stack = vec![CallStackEntry {
            program_id: "TestProgram".to_string(),
            depth: 1,
            instruction_index: 0,
        }];

        let log = "Program log: Hello, World!";
        let result = parser.parse_program_log(log, &call_stack);

        assert!(result.is_some());
        let parsed = result.unwrap();
        assert_eq!(parsed.log_type, LogType::Log);
        assert_eq!(parsed.message, "Hello, World!");
    }

    #[test]
    fn test_parse_program_data() {
        let parser = LogParser::new(HashSet::new(), true, false);
        let call_stack = vec![CallStackEntry {
            program_id: "TestProgram".to_string(),
            depth: 1,
            instruction_index: 0,
        }];

        // Base64 encoded "test data"
        let log = "Program data: dGVzdCBkYXRh";
        let result = parser.parse_program_data(log, &call_stack);

        assert!(result.is_some());
        let parsed = result.unwrap();
        assert_eq!(parsed.log_type, LogType::Data);
        assert!(parsed.data.is_some());
        assert_eq!(parsed.data.unwrap(), b"test data");
    }

    #[test]
    fn test_parse_failed() {
        let parser = LogParser::new(HashSet::new(), true, false);
        let call_stack = vec![CallStackEntry {
            program_id: "TestProgram".to_string(),
            depth: 1,
            instruction_index: 0,
        }];

        let log = "Program TestProgram failed: custom program error: 0x1";
        let result = parser.parse_failed(log, &call_stack);

        assert!(result.is_some());
        let parsed = result.unwrap();
        assert_eq!(parsed.log_type, LogType::Failed);
        assert!(parsed.error.is_some());
        assert_eq!(parsed.error.unwrap(), "custom program error: 0x1");
    }

    #[test]
    fn test_full_transaction_logs() {
        let parser = LogParser::new(HashSet::new(), true, false);

        let logs = vec![
            "Program 11111111111111111111111111111111 invoke [1]".to_string(),
            "Program log: Instruction: Transfer".to_string(),
            "Program 11111111111111111111111111111111 success".to_string(),
        ];

        let (program_logs, _) = parser.parse_logs(&logs);

        assert_eq!(program_logs.len(), 3);
        assert_eq!(program_logs[0].log_type, LogType::Invoke);
        assert_eq!(program_logs[1].log_type, LogType::Log);
        assert_eq!(program_logs[2].log_type, LogType::Success);
    }

    #[test]
    fn test_nested_program_calls() {
        let parser = LogParser::new(HashSet::new(), true, false);

        let logs = vec![
            "Program ProgramA invoke [1]".to_string(),
            "Program log: Starting operation".to_string(),
            "Program ProgramB invoke [2]".to_string(),
            "Program log: Inner operation".to_string(),
            "Program ProgramB success".to_string(),
            "Program ProgramA success".to_string(),
        ];

        let (program_logs, _) = parser.parse_logs(&logs);

        assert_eq!(program_logs.len(), 6);
        assert_eq!(program_logs[0].depth, 1);
        assert_eq!(program_logs[2].depth, 2);
    }
}
