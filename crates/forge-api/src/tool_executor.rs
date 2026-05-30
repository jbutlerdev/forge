//! Tool Executor Module
//! 
//! Executes tools (bash, read, write, edit) in the sandbox.
//! This is where actual tool work happens - NOT in pi.

use serde::{Deserialize, Serialize};
use std::path::Path;
use std::process::Stdio;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;

/// Tool execution errors
#[derive(Error, Debug)]
pub enum ToolError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Tool not found: {0}")]
    NotFound(String),
    
    #[error("Invalid input: {0}")]
    InvalidInput(String),
    
    #[error("Tool execution failed: {0}")]
    ExecutionFailed(String),
    
    #[error("Timeout: {0}")]
    Timeout(String),
}

/// Tool execution result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolOutput {
    pub success: bool,
    pub output: Option<String>,
    pub error: Option<String>,
}

/// Input for bash tool
#[derive(Debug, Deserialize)]
pub struct BashInput {
    pub command: String,
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,
}

fn default_timeout() -> u64 {
    30000
}

/// Input for read tool
#[derive(Debug, Deserialize)]
pub struct ReadInput {
    pub path: String,
    #[serde(default = "default_offset")]
    pub offset: usize,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_offset() -> usize {
    1
}

fn default_limit() -> usize {
    100
}

/// Input for write tool
#[derive(Debug, Deserialize)]
pub struct WriteInput {
    pub path: String,
    pub content: String,
}

/// Input for edit tool
#[derive(Debug, Deserialize)]
pub struct EditInput {
    pub path: String,
    pub old_text: String,
    pub new_text: String,
}

/// Unified tool input
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct ToolInput {
    pub session_id: String,
    pub tool: String,
    pub input: serde_json::Value,
    #[serde(rename = "tool_call_id")]
    pub tool_call_id: String,
}

/// Tool executor that runs tools in the sandbox
pub struct ToolExecutor {
    /// Working directory for tool execution
    working_dir: String,
    /// Whether to execute in sandbox container
    in_sandbox: bool,
    /// Container name (if in_sandbox) - reserved for future use
    #[allow(dead_code)]
    container_name: Option<String>,
    /// Nix shell expression or path (if configured)
    nix_shell: Option<String>,
}

impl ToolExecutor {
    /// Create a new tool executor with the given working directory (reserved for future use)
    #[allow(dead_code)]
    pub fn new(working_dir: String, in_sandbox: bool) -> Self {
        Self { 
            working_dir,
            in_sandbox,
            container_name: None,
            nix_shell: None,
        }
    }
    
    /// Create a new tool executor for a specific container (reserved for future use)
    #[allow(dead_code)]
    pub fn new_for_container(working_dir: String, container_name: String) -> Self {
        Self {
            working_dir,
            in_sandbox: true,
            container_name: Some(container_name),
            nix_shell: None,
        }
    }
    
    /// Create a new tool executor with nix shell support
    pub fn new_with_nix(working_dir: String, in_sandbox: bool, nix_shell: Option<String>) -> Self {
        Self {
            working_dir,
            in_sandbox,
            container_name: None,
            nix_shell,
        }
    }
    
    /// Wrap a command with nix-shell if configured
    /// 
    /// Returns (command_to_run, command_to_pass)
    /// If nix-shell is configured, returns ("bash", "nix-shell -c '...' -p pkg")
    /// Otherwise, returns ("bash", "command")
    fn wrap_command(&self, command: &str) -> (String, String) {
        match &self.nix_shell {
            Some(nix_expr) => {
                // Check if it's a path or an expression
                if nix_expr.starts_with('/') || nix_expr.starts_with('.') || nix_expr.ends_with(".nix") {
                    // It's a path to a .nix file or shell.nix
                    let wrapped = format!("nix-shell '{}' -c '{}'", nix_expr, command.replace("'", "'\"'\"'"));
                    tracing::debug!("Using nix-shell with path: {}", nix_expr);
                    ("bash".to_string(), wrapped)
                } else {
                    // It's a nix shell expression (packages)
                    let wrapped = format!("nix-shell -p {} -c '{}'", nix_expr, command.replace("'", "'\"'\"'"));
                    tracing::debug!("Using nix-shell with expression: {}", nix_expr);
                    ("bash".to_string(), wrapped)
                }
            }
            None => {
                ("bash".to_string(), command.to_string())
            }
        }
    }
    
    /// Execute a tool by name with the given input
    pub async fn execute(&self, tool_name: &str, input: serde_json::Value) -> Result<ToolOutput, ToolError> {
        let span = tracing::info_span!(
            "tool_execute",
            tool = %tool_name,
            working_dir = %self.working_dir,
            in_sandbox = %self.in_sandbox
        );
        let _guard = span.enter();
        
        tracing::debug!(input = ?input, "Executing tool");
        
        match tool_name {
            "bash" => self.execute_bash(input).await,
            "read" => self.execute_read(input).await,
            "write" => self.execute_write(input).await,
            "edit" => self.execute_edit(input).await,
            _ => {
                tracing::error!("Unknown tool: {}", tool_name);
                Err(ToolError::NotFound(tool_name.to_string()))
            }
        }
    }
    
    /// Execute a bash command
    async fn execute_bash(&self, input: serde_json::Value) -> Result<ToolOutput, ToolError> {
        let span = tracing::info_span!("bash_execute", cwd = %self.working_dir, has_nix = %self.nix_shell.is_some());
        let _guard = span.enter();
        
        let input: BashInput = serde_json::from_value(input)
            .map_err(|e| {
                tracing::error!("Invalid bash input: {}", e);
                ToolError::InvalidInput(e.to_string())
            })?;
        
        tracing::debug!(
            command = %input.command,
            timeout_ms = %input.timeout_ms,
            "Executing bash command"
        );
        
        // Sanitize command length for logging
        let cmd_preview = if input.command.len() > 100 {
            format!("{}... ({} chars)", &input.command[..100], input.command.len())
        } else {
            input.command.clone()
        };
        tracing::info!(command_preview = %cmd_preview, "Running bash command");
        
        // Determine the command wrapper (nix-shell if configured)
        let (cmd_to_run, wrapped_command) = self.wrap_command(&input.command);
        
        // Execute command
        let mut cmd = Command::new(&cmd_to_run);
        cmd.arg("-c")
            .arg(&wrapped_command)
            .current_dir(&self.working_dir)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        
        // Set timeout
        let timeout = std::time::Duration::from_millis(input.timeout_ms);
        
        match tokio::time::timeout(timeout, cmd.output()).await {
            Ok(Ok(output)) => {
                let stdout = String::from_utf8_lossy(&output.stdout).to_string();
                let stderr = String::from_utf8_lossy(&output.stderr).to_string();
                
                let stdout_empty = stdout.is_empty();
                let stdout_len = stdout.len();
                let stderr_len = stderr.len();
                let result_output = if stdout_empty { stderr.clone() } else { stdout };
                let error = if stderr.is_empty() || !stdout_empty { None } else { Some(stderr) };
                
                let success = output.status.success();
                tracing::info!(
                    success = %success,
                    exit_code = ?output.status.code(),
                    stdout_len = %stdout_len,
                    stderr_len = %stderr_len,
                    "Bash command completed"
                );
                
                Ok(ToolOutput {
                    success,
                    output: Some(result_output),
                    error,
                })
            }
            Ok(Err(e)) => {
                tracing::error!("Failed to execute bash: {}", e);
                Err(ToolError::ExecutionFailed(e.to_string()))
            }
            Err(_) => {
                tracing::warn!(timeout_ms = %input.timeout_ms, "Bash command timed out");
                Err(ToolError::Timeout(format!(
                    "Command timed out after {}ms",
                    input.timeout_ms
                )))
            }
        }
    }
    
    /// Read a file
    async fn execute_read(&self, input: serde_json::Value) -> Result<ToolOutput, ToolError> {
        let span = tracing::info_span!("read_execute", cwd = %self.working_dir);
        let _guard = span.enter();
        
        let input: ReadInput = serde_json::from_value(input)
            .map_err(|e| {
                tracing::error!("Invalid read input: {}", e);
                ToolError::InvalidInput(e.to_string())
            })?;
        
        let path = Path::new(&self.working_dir).join(&input.path);
        tracing::debug!(path = ?path, offset = %input.offset, limit = %input.limit, "Reading file");
        
        let mut file = tokio::fs::File::open(&path).await
            .map_err(|e| {
                tracing::error!("Failed to open file {:?}: {}", path, e);
                ToolError::ExecutionFailed(format!("Failed to open file: {}", e))
            })?;
        
        let mut content = String::new();
        file.read_to_string(&mut content).await
            .map_err(|e| {
                tracing::error!("Failed to read file {:?}: {}", path, e);
                ToolError::ExecutionFailed(format!("Failed to read file: {}", e))
            })?;
        
        // Apply offset and limit (lines are 1-indexed)
        let lines: Vec<&str> = content.lines().collect();
        let start = input.offset.saturating_sub(1).min(lines.len());
        let end = (start + input.limit).min(lines.len());
        let selected: String = lines[start..end].join("\n");
        
        tracing::info!(
            total_lines = %lines.len(),
            returned_lines = %(end - start),
            bytes = %selected.len(),
            "File read completed"
        );
        
        Ok(ToolOutput {
            success: true,
            output: Some(selected),
            error: None,
        })
    }
    
    /// Write content to a file
    async fn execute_write(&self, input: serde_json::Value) -> Result<ToolOutput, ToolError> {
        let span = tracing::info_span!("write_execute", cwd = %self.working_dir);
        let _guard = span.enter();
        
        let input: WriteInput = serde_json::from_value(input)
            .map_err(|e| {
                tracing::error!("Invalid write input: {}", e);
                ToolError::InvalidInput(e.to_string())
            })?;
        
        let path = Path::new(&self.working_dir).join(&input.path);
        tracing::debug!(path = ?path, bytes = %input.content.len(), "Writing file");
        
        // Create parent directories if needed
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await
                .map_err(|e| {
                    tracing::error!("Failed to create directory {:?}: {}", parent, e);
                    ToolError::ExecutionFailed(format!("Failed to create directory: {}", e))
                })?;
        }
        
        let mut file = tokio::fs::File::create(&path).await
            .map_err(|e| {
                tracing::error!("Failed to create file {:?}: {}", path, e);
                ToolError::ExecutionFailed(format!("Failed to create file: {}", e))
            })?;
        
        file.write_all(input.content.as_bytes()).await
            .map_err(|e| {
                tracing::error!("Failed to write to file {:?}: {}", path, e);
                ToolError::ExecutionFailed(format!("Failed to write file: {}", e))
            })?;
        
        tracing::info!(path = ?path, bytes_written = %input.content.len(), "File write completed");
        
        Ok(ToolOutput {
            success: true,
            output: Some(format!("Successfully wrote {} bytes to {}", input.content.len(), input.path)),
            error: None,
        })
    }
    
    /// Edit a file with targeted replacement
    async fn execute_edit(&self, input: serde_json::Value) -> Result<ToolOutput, ToolError> {
        let span = tracing::info_span!("edit_execute", cwd = %self.working_dir);
        let _guard = span.enter();
        
        let input: EditInput = serde_json::from_value(input)
            .map_err(|e| {
                tracing::error!("Invalid edit input: {}", e);
                ToolError::InvalidInput(e.to_string())
            })?;
        
        let path = Path::new(&self.working_dir).join(&input.path);
        tracing::debug!(
            path = ?path, 
            old_text_len = %input.old_text.len(), 
            new_text_len = %input.new_text.len(),
            "Editing file"
        );
        
        // Read the file
        let mut content = tokio::fs::read_to_string(&path).await
            .map_err(|e| {
                tracing::error!("Failed to read file {:?}: {}", path, e);
                ToolError::ExecutionFailed(format!("Failed to read file: {}", e))
            })?;
        
        // Find and replace
        if !content.contains(&input.old_text) {
            tracing::warn!(
                path = ?path,
                old_text_len = %input.old_text.len(),
                "old_text not found in file"
            );
            return Err(ToolError::InvalidInput(
                "old_text not found in file. Make sure to match the exact text including whitespace.".to_string()
            ));
        }
        
        content = content.replacen(&input.old_text, &input.new_text, 1);
        
        // Write back
        tokio::fs::write(&path, &content).await
            .map_err(|e| {
                tracing::error!("Failed to write file {:?}: {}", path, e);
                ToolError::ExecutionFailed(format!("Failed to write file: {}", e))
            })?;
        
        tracing::info!(path = ?path, "Edit applied successfully");
        
        Ok(ToolOutput {
            success: true,
            output: Some("Edit applied successfully".to_string()),
            error: None,
        })
    }
}

/// Execute a tool from an API request
#[allow(dead_code)]
pub async fn execute_tool(
    tool_executor: &ToolExecutor,
    request: &ToolInput,
) -> Result<ToolOutput, ToolError> {
    tool_executor.execute(&request.tool, request.input.clone()).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn temp_executor() -> (ToolExecutor, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let executor = ToolExecutor::new_with_nix(
            temp_dir.path().to_string_lossy().to_string(),
            false,
            None,
        );
        (executor, temp_dir)
    }

    #[tokio::test]
    async fn test_bash_simple_command() {
        let (executor, _temp) = temp_executor();
        
        let input = serde_json::json!({
            "command": "echo 'hello world'",
            "timeout_ms": 5000
        });
        
        let result = executor.execute("bash", input).await;
        
        assert!(result.is_ok());
        let output = result.unwrap();
        assert!(output.success);
        assert!(output.output.unwrap().contains("hello world"));
    }

    #[tokio::test]
    async fn test_bash_command_failure() {
        let (executor, _temp) = temp_executor();
        
        let input = serde_json::json!({
            "command": "exit 1",
            "timeout_ms": 5000
        });
        
        let result = executor.execute("bash", input).await;
        
        assert!(result.is_ok());
        let output = result.unwrap();
        assert!(!output.success);
    }

    #[tokio::test]
    async fn test_bash_timeout() {
        let (executor, _temp) = temp_executor();
        
        let input = serde_json::json!({
            "command": "sleep 10",
            "timeout_ms": 100
        });
        
        let result = executor.execute("bash", input).await;
        
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("timed out"));
    }

    #[tokio::test]
    async fn test_unknown_tool() {
        let (executor, _temp) = temp_executor();
        
        let input = serde_json::json!({});
        
        let result = executor.execute("nonexistent_tool", input).await;
        
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_write_and_read_file() {
        let (executor, temp) = temp_executor();
        
        // Write a file
        let write_input = serde_json::json!({
            "path": "test.txt",
            "content": "Hello, World!"
        });
        
        let write_result = executor.execute("write", write_input).await;
        assert!(write_result.is_ok());
        assert!(write_result.unwrap().success);
        
        // Read it back
        let read_input = serde_json::json!({
            "path": "test.txt"
        });
        
        let read_result = executor.execute("read", read_input).await;
        assert!(read_result.is_ok());
        assert!(read_result.unwrap().output.unwrap().contains("Hello, World!"));
    }

    #[tokio::test]
    async fn test_edit_file() {
        let (executor, temp) = temp_executor();
        
        // Create a file first
        let write_input = serde_json::json!({
            "path": "edit_test.txt",
            "content": "Hello, World!"
        });
        executor.execute("write", write_input).await.unwrap();
        
        // Edit it
        let edit_input = serde_json::json!({
            "path": "edit_test.txt",
            "old_text": "World",
            "new_text": "Rust"
        });
        
        let result = executor.execute("edit", edit_input).await;
        assert!(result.is_ok());
        
        // Verify the edit
        let read_input = serde_json::json!({"path": "edit_test.txt"});
        let read_result = executor.execute("read", read_input).await.unwrap();
        assert!(read_result.output.unwrap().contains("Hello, Rust!"));
    }

    #[tokio::test]
    async fn test_edit_file_not_found() {
        let (executor, _temp) = temp_executor();
        
        let edit_input = serde_json::json!({
            "path": "nonexistent.txt",
            "old_text": "something",
            "new_text": "replacement"
        });
        
        let result = executor.execute("edit", edit_input).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_read_nonexistent_file() {
        let (executor, _temp) = temp_executor();
        
        let read_input = serde_json::json!({
            "path": "nonexistent_file.txt"
        });
        
        let result = executor.execute("read", read_input).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_write_nested_directory() {
        let (executor, temp) = temp_executor();
        
        let write_input = serde_json::json!({
            "path": "nested/dir/test.txt",
            "content": "nested content"
        });
        
        let result = executor.execute("write", write_input).await;
        assert!(result.is_ok());
        
        // Verify file exists
        let file_path: PathBuf = temp.path().join("nested/dir/test.txt");
        assert!(file_path.exists());
    }
}
