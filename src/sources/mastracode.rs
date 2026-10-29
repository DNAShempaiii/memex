//! Mastra Code threads, stored as rows in one SQLite database.
//!
//! Mastra Code keeps every conversation in `<data dir>/mastra.db`
//! (`~/Library/Application Support/mastracode` on macOS). Threads live in
//! `mastra_threads` (`title`, plus a `metadata` column holding the project path)
//! and messages in `mastra_messages`, whose `content` is a JSON document
//! (`{"format":2,"parts":[...]}`) of typed parts.
//!
//! `mastra_threads.metadata` is SQLite JSONB (binary), which the SQLite bundled
//! with rusqlite cannot read through `json_extract`, so it is decoded here.
//!
//! Observational-memory scaffolding (`data-om-*` parts) and injected signals
//! (`system-reminder`, task lists, objectives) are not conversation and are
//! skipped. Rows are mutated in place by memory activation, so ingestion
//! replaces a whole thread at a time from a content fingerprint, like goose.
//!
//! Mastra Code does not record per-request token usage, so there is no usage
//! projection.

use super::{
    ConversationKind, IndexParseOutput, IndexParseState, ParseDiagnostics, ParserVersions,
    SourceFile,
};
use crate::types::{Record, RecordLinks, SourceKind};
use anyhow::{Context, Result, anyhow};
use rusqlite::types::Value as SqlValue;
use rusqlite::{Connection, OpenFlags};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub const VERSIONS: ParserVersions = ParserVersions {
    identity: 1,
    index: 1,
    usage: 1,
};

const DIRECTORY: &str = "mastracode";
const DATABASE: &str = "mastra.db";
const PREFIX: &str = "mastracode-thread-";

/// Messages that carry conversation: the assistant's replies and the user's
/// prompts, which Mastra Code stores as `user` signals.
const CONVERSATION_FILTER: &str =
    "(role = 'assistant' OR role = 'user' OR (role = 'signal' AND type = 'user'))";

pub fn matches_path(path: &str) -> bool {
    split_virtual_path(Path::new(path)).is_some() || is_database_path(Path::new(path))
}

fn is_database_path(path: &Path) -> bool {
    path.file_name().is_some_and(|name| name == DATABASE)
        && path
            .parent()
            .and_then(Path::file_name)
            .is_some_and(|name| name == DIRECTORY)
}

/// Encode thread ids into a single safe component.
pub fn virtual_path(database: &Path, thread_id: &str) -> PathBuf {
    let encoded: String = thread_id
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let directory = database.parent().unwrap_or(database);
    directory.join(format!("{PREFIX}{encoded}"))
}

pub fn split_virtual_path(path: &Path) -> Option<(PathBuf, String)> {
    let encoded = path.file_name()?.to_str()?.strip_prefix(PREFIX)?;
    let directory = path.parent()?;
    if directory.file_name()? != DIRECTORY || !encoded.len().is_multiple_of(2) {
        return None;
    }
    let bytes = encoded
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok())
        .collect::<Option<Vec<_>>>()?;
    Some((directory.join(DATABASE), String::from_utf8(bytes).ok()?))
}

/// The Mastra Code data directories to look in. `MASTRACODE_DATA_DIR` wins.
pub fn roots() -> Vec<PathBuf> {
    if let Some(root) = std::env::var_os("MASTRACODE_DATA_DIR")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
    {
        return vec![root];
    }
    let mut roots = vec![default_xdg_root(), native_data_root()];
    roots.retain(|root| !root.as_os_str().is_empty());
    roots.sort();
    roots.dedup();
    roots
}

fn default_xdg_root() -> PathBuf {
    std::env::var_os("XDG_DATA_HOME")
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| super::common::home().join(".local/share"))
        .join(DIRECTORY)
}

fn native_data_root() -> PathBuf {
    #[cfg(target_os = "macos")]
    {
        super::common::home().join("Library/Application Support/mastracode")
    }
    #[cfg(target_os = "windows")]
    {
        std::env::var_os("APPDATA")
            .map(PathBuf::from)
            .unwrap_or_else(|| super::common::home().join("AppData/Roaming"))
            .join(DIRECTORY)
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        default_xdg_root()
    }
}

pub fn db_paths() -> Vec<PathBuf> {
    db_paths_for(&roots())
}

fn db_paths_for(roots: &[PathBuf]) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = roots
        .iter()
        .map(|root| root.join(DATABASE))
        .filter(|path| path.is_file())
        .collect();
    paths.sort();
    paths.dedup();
    paths
}

pub fn discover() -> Vec<SourceFile> {
    discover_from_roots(&roots())
}

pub fn discover_from_roots(roots: &[PathBuf]) -> Vec<SourceFile> {
    db_paths_for(roots)
        .into_iter()
        .map(|path| SourceFile {
            source: SourceKind::Mastracode,
            path,
        })
        .collect()
}

/// Mastra Code records no usage ledger.
pub fn usage_files() -> Vec<PathBuf> {
    Vec::new()
}

/// Directories holding the thread databases, for the watcher.
pub fn db_dirs() -> Vec<PathBuf> {
    let mut dirs: Vec<PathBuf> = roots()
        .into_iter()
        .filter(|root| root.join(DATABASE).is_file())
        .collect();
    dirs.sort();
    dirs.dedup();
    dirs
}

fn open_readonly(path: &Path) -> Result<Connection> {
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("open mastracode database {}", path.display()))?;
    let _ = conn.busy_timeout(std::time::Duration::from_millis(500));
    Ok(conn)
}

fn has_tables(conn: &Connection) -> Result<bool> {
    let mut statement = conn.prepare("SELECT name FROM sqlite_master WHERE type='table'")?;
    let names: std::collections::HashSet<String> = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .filter_map(Result::ok)
        .collect();
    Ok(names.contains("mastra_threads") && names.contains("mastra_messages"))
}

fn text_of(value: SqlValue) -> String {
    match value {
        SqlValue::Text(text) => text,
        SqlValue::Blob(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
        SqlValue::Integer(number) => number.to_string(),
        SqlValue::Real(number) => number.to_string(),
        SqlValue::Null => String::new(),
    }
}

pub(crate) struct SessionFingerprint {
    pub id: String,
    pub fingerprint: String,
    /// Number of projected parts, so progress reports turns rather than pages.
    pub size: u64,
}

struct ThreadRow {
    id: String,
    title: String,
    project_path: Option<String>,
}

fn thread_row(id: String, title: Option<String>, metadata: SqlValue) -> ThreadRow {
    let project_path = match &metadata {
        SqlValue::Blob(bytes) => decode_metadata(bytes),
        SqlValue::Text(text) => serde_json::from_str(text).ok(),
        _ => None,
    }
    .and_then(|value| {
        value
            .get("projectPath")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|path| !path.is_empty())
            .map(str::to_string)
    });
    ThreadRow {
        id,
        title: title.unwrap_or_default(),
        project_path,
    }
}

fn read_threads(conn: &Connection, only: Option<&str>) -> Result<Vec<ThreadRow>> {
    let mut statement = conn.prepare(&format!(
        "SELECT id, title, metadata FROM mastra_threads \
         WHERE (?1 IS NULL OR id = ?1) \
           AND EXISTS (SELECT 1 FROM mastra_messages \
                       WHERE thread_id = mastra_threads.id AND {CONVERSATION_FILTER}) \
         ORDER BY createdAt, id"
    ))?;
    let rows = statement
        .query_map([only], |row| {
            Ok(thread_row(
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, SqlValue>(2)?,
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(rows)
}

struct MessageRow {
    role: String,
    content: String,
    created: String,
}

fn read_messages(conn: &Connection, thread_id: &str) -> Result<Vec<MessageRow>> {
    let mut statement = conn.prepare(&format!(
        "SELECT role, content, createdAt FROM mastra_messages \
         WHERE thread_id = ?1 AND {CONVERSATION_FILTER} ORDER BY createdAt, id"
    ))?;
    let rows = statement
        .query_map([thread_id], |row| {
            Ok(MessageRow {
                role: row.get::<_, SqlValue>(0).map(text_of)?,
                content: row.get::<_, SqlValue>(1).map(text_of)?,
                created: row.get::<_, SqlValue>(2).map(text_of)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// Hash the conversation content of every thread in one SQLite snapshot.
/// Memory activation and signal churn rewrite rows that are not conversation,
/// so only conversation rows feed the hash.
pub(crate) fn enumerate_sessions(database: &Path) -> Result<Vec<SessionFingerprint>> {
    let conn = open_readonly(database)?;
    let transaction = conn.unchecked_transaction()?;
    if !has_tables(&transaction)? {
        return Err(anyhow!(
            "mastracode database {} has no mastra_threads/mastra_messages tables",
            database.display()
        ));
    }
    let mut result = Vec::new();
    for thread in read_threads(&transaction, None)? {
        let mut hash = Sha256::new();
        hash.update(thread.title.as_bytes());
        hash.update([0xfe]);
        hash.update(thread.project_path.as_deref().unwrap_or("").as_bytes());
        let mut size = 0;
        for message in read_messages(&transaction, &thread.id)? {
            hash.update([0xff]);
            hash.update(message.role.as_bytes());
            hash.update(message.created.as_bytes());
            hash.update(message.content.as_bytes());
            size += message_parts(&message)
                .map(|parts| parts.iter().filter(|part| projects_part(part)).count() as u64)
                .unwrap_or_default();
        }
        result.push(SessionFingerprint {
            id: thread.id,
            fingerprint: format!("{:x}", hash.finalize()),
            size,
        });
    }
    transaction.commit()?;
    Ok(result)
}

fn message_parts(message: &MessageRow) -> Option<Vec<Value>> {
    let value: Value = serde_json::from_str(&message.content).ok()?;
    match value.get("parts") {
        Some(Value::Array(parts)) => Some(parts.clone()),
        _ => None,
    }
}

fn part_text(part: &Value) -> Option<&str> {
    part.get("text")
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        // Reminders injected by the harness are not something anyone typed.
        .filter(|text| !text.trim_start().starts_with("<system-reminder>"))
}

fn part_type(part: &Value) -> &str {
    part.get("type").and_then(Value::as_str).unwrap_or("")
}

/// Whether one message part becomes a Memex row. Enumeration counts with this
/// predicate so a thread's recorded size always equals the parser's offset.
fn projects_part(part: &Value) -> bool {
    match part_type(part) {
        "text" => part_text(part).is_some(),
        "reasoning" => part
            .get("reasoning")
            .and_then(Value::as_str)
            .is_some_and(|text| !text.trim().is_empty()),
        "tool-invocation" => part
            .pointer("/toolInvocation/toolName")
            .and_then(Value::as_str)
            .is_some(),
        "error" | "file" => true,
        _ => false,
    }
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(text) => Some(text.clone()),
        other => serde_json::to_string(other).ok(),
    }
}

fn message_ts(message: &MessageRow) -> u64 {
    chrono::DateTime::parse_from_rfc3339(&message.created)
        .map(|time| time.timestamp_millis().max(0) as u64)
        .unwrap_or_default()
}

pub(crate) fn parse_index_records(
    path: &Path,
    state: IndexParseState,
    include_reasoning: bool,
    next_doc_id: &AtomicU64,
    mut emit: impl FnMut(Record) -> Result<()>,
) -> Result<IndexParseOutput> {
    // Each changed thread is replaced, so state.offset is intentionally ignored.
    let source_path = path.to_string_lossy().to_string();
    let virtual_session = split_virtual_path(path);
    let database = virtual_session
        .as_ref()
        .map_or(path, |(database, _)| database.as_path());
    let selected_session = virtual_session.as_ref().map(|(_, id)| id.as_str());
    let mut diagnostics = ParseDiagnostics::default();
    let conn = open_readonly(database)?;
    let transaction = conn.unchecked_transaction()?;
    if !has_tables(&transaction)? {
        return Err(anyhow!(
            "mastracode database {} has no mastra_threads/mastra_messages tables",
            path.display()
        ));
    }
    let threads = read_threads(&transaction, selected_session)?;

    let mut row_count = 0;
    let mut turn_id = state.turn_id;
    let mut session_cwd: Option<String> = None;
    for thread in &threads {
        if session_cwd.is_none() {
            session_cwd = thread.project_path.clone();
        }
        let project = thread
            .project_path
            .as_deref()
            .map(super::common::project_from_path);
        let links = RecordLinks {
            conversation_kind: Some(ConversationKind::Main.as_str().to_string()),
            ..RecordLinks::default()
        };
        for message in read_messages(&transaction, &thread.id)? {
            let Some(parts) = message_parts(&message) else {
                diagnostics.malformed_json_lines += 1;
                continue;
            };
            let message_role = if message.role == "assistant" {
                "assistant"
            } else {
                "user"
            };
            let message_ts = message_ts(&message);
            for part in &parts {
                let ts = part
                    .get("createdAt")
                    .and_then(Value::as_u64)
                    .unwrap_or(message_ts);
                let base = |turn_id: u32| Record {
                    source: SourceKind::Mastracode,
                    doc_id: next_doc_id.fetch_add(1, Ordering::SeqCst),
                    ts,
                    project: project
                        .clone()
                        .unwrap_or_else(|| SourceKind::Mastracode.label().to_string()),
                    session_id: thread.id.clone(),
                    turn_id,
                    role: String::new(),
                    text: String::new(),
                    tool_name: None,
                    tool_input: None,
                    tool_output: None,
                    links: links.clone(),
                    source_path: source_path.clone(),
                };
                match part_type(part) {
                    "text" => {
                        let Some(text) = part_text(part) else {
                            continue;
                        };
                        row_count += 1;
                        emit(Record {
                            role: message_role.to_string(),
                            text: text.to_string(),
                            ..base(turn_id)
                        })?;
                        turn_id += 1;
                    }
                    "reasoning" => {
                        let reasoning = part
                            .get("reasoning")
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        if reasoning.trim().is_empty() {
                            continue;
                        }
                        row_count += 1;
                        if !include_reasoning {
                            continue;
                        }
                        emit(Record {
                            role: "reasoning".to_string(),
                            text: reasoning.to_string(),
                            ..base(turn_id)
                        })?;
                        turn_id += 1;
                    }
                    "tool-invocation" => {
                        let Some(invocation) = part.get("toolInvocation") else {
                            continue;
                        };
                        let Some(name) = invocation.get("toolName").and_then(Value::as_str) else {
                            diagnostics.increment_unknown_semantic("tool_without_name");
                            continue;
                        };
                        let call_id = invocation
                            .get("toolCallId")
                            .and_then(Value::as_str)
                            .filter(|id| !id.is_empty())
                            .map(str::to_string);
                        let arguments = invocation.get("args").and_then(value_to_string);
                        let mut tool_links = links.clone();
                        tool_links.event_id = call_id.clone();
                        row_count += 1;
                        emit(Record {
                            role: "tool_use".to_string(),
                            text: arguments.clone().unwrap_or_default(),
                            tool_name: Some(name.to_string()),
                            tool_input: arguments,
                            links: tool_links,
                            ..base(turn_id)
                        })?;
                        turn_id += 1;
                        let (output, is_error) = match invocation
                            .get("state")
                            .and_then(Value::as_str)
                        {
                            Some("result") => {
                                (invocation.get("result").and_then(value_to_string), false)
                            }
                            Some("output-error") => (
                                invocation
                                    .get("errorText")
                                    .and_then(value_to_string)
                                    .or_else(|| invocation.get("result").and_then(value_to_string)),
                                true,
                            ),
                            _ => (None, false),
                        };
                        if let Some(output) = output {
                            let mut result_links = links.clone();
                            if let Some(call_id) = &call_id {
                                result_links.event_id = Some(format!("{call_id}:result"));
                                result_links.parent_event_id = Some(call_id.clone());
                                result_links.parent_tool_use_id = Some(call_id.clone());
                            }
                            result_links.tool_result_is_error = Some(is_error);
                            emit(Record {
                                role: "tool_result".to_string(),
                                text: output.clone(),
                                tool_output: Some(output),
                                links: result_links,
                                ..base(turn_id)
                            })?;
                            turn_id += 1;
                        }
                    }
                    "file" => {
                        let mime = part
                            .get("mimeType")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown");
                        row_count += 1;
                        emit(Record {
                            role: message_role.to_string(),
                            text: format!("[file: {mime}]"),
                            ..base(turn_id)
                        })?;
                        turn_id += 1;
                    }
                    "error" => {
                        let text = part
                            .pointer("/error/message")
                            .and_then(Value::as_str)
                            .unwrap_or("error");
                        row_count += 1;
                        emit(Record {
                            role: "tool_result".to_string(),
                            text: text.to_string(),
                            links: RecordLinks {
                                tool_result_is_error: Some(true),
                                ..links.clone()
                            },
                            ..base(turn_id)
                        })?;
                        turn_id += 1;
                    }
                    // Observational-memory bookkeeping, sandbox events, web
                    // sources, and step markers are not conversation.
                    "step-start" | "source" => {}
                    other if other.starts_with("data-") => {}
                    other => diagnostics.increment_unknown_top_level(other),
                }
            }
        }
    }
    transaction.commit()?;
    Ok(IndexParseOutput {
        offset: row_count,
        turn_id,
        legacy_turn_id: None,
        pending_tool_calls: state.pending_tool_calls,
        session_id: selected_session.map(str::to_owned),
        diagnostics,
        session_cwd,
    })
}

fn database_of(path: &Path) -> PathBuf {
    split_virtual_path(path).map_or_else(|| path.to_path_buf(), |(database, _)| database)
}

/// Project directory of one thread, for analytics and session metadata.
pub fn session_cwd(path: &Path, session_id: &str) -> Option<PathBuf> {
    let conn = open_readonly(&database_of(path)).ok()?;
    read_threads(&conn, Some(session_id))
        .ok()?
        .into_iter()
        .next()?
        .project_path
        .map(PathBuf::from)
}

/// The title Mastra Code shows for a thread. Empty until it is named.
pub fn session_title(path: &Path, session_id: &str) -> Option<String> {
    let conn = open_readonly(&database_of(path)).ok()?;
    let title: Option<String> = conn
        .query_row(
            "SELECT title FROM mastra_threads WHERE id = ?1",
            [session_id],
            |row| row.get(0),
        )
        .ok()?;
    title
        .map(|title| title.trim().to_string())
        .filter(|title| !title.is_empty())
}

/// Decode a SQLite JSONB blob. The bundled SQLite predates JSONB, so
/// `json_extract` cannot read Mastra's `metadata` column. Plain JSON text
/// stored in a blob is accepted as well.
fn decode_metadata(bytes: &[u8]) -> Option<Value> {
    if bytes.first() == Some(&b'{') {
        return serde_json::from_slice(bytes).ok();
    }
    let (value, used) = decode_jsonb(bytes)?;
    (used == bytes.len()).then_some(value)
}

/// Returns the decoded element and the number of bytes it occupies.
fn decode_jsonb(bytes: &[u8]) -> Option<(Value, usize)> {
    let head = *bytes.first()?;
    let kind = head & 0x0f;
    let (payload_len, header_len): (usize, usize) = match head >> 4 {
        size @ 0..=11 => (usize::from(size), 1),
        12 => (usize::from(*bytes.get(1)?), 2),
        13 => (
            usize::from(u16::from_be_bytes(bytes.get(1..3)?.try_into().ok()?)),
            3,
        ),
        14 => (
            u32::from_be_bytes(bytes.get(1..5)?.try_into().ok()?) as usize,
            5,
        ),
        _ => (
            usize::try_from(u64::from_be_bytes(bytes.get(1..9)?.try_into().ok()?)).ok()?,
            9,
        ),
    };
    let end = header_len.checked_add(payload_len)?;
    let payload = bytes.get(header_len..end)?;
    let text = || std::str::from_utf8(payload).ok();
    let value = match kind {
        0 => Value::Null,
        1 => Value::Bool(true),
        2 => Value::Bool(false),
        3..=6 => {
            let number: serde_json::Number = text()?.parse().ok()?;
            Value::Number(number)
        }
        7 | 10 => Value::String(text()?.to_string()),
        8 | 9 => {
            // TEXTJ carries JSON escapes; TEXT5 is rare and decoded the same way.
            let quoted = format!("\"{}\"", text()?);
            Value::String(serde_json::from_str(&quoted).ok()?)
        }
        11 => {
            let mut items = Vec::new();
            let mut offset = 0;
            while offset < payload.len() {
                let (item, used) = decode_jsonb(&payload[offset..])?;
                items.push(item);
                offset += used;
            }
            Value::Array(items)
        }
        12 => {
            let mut map = serde_json::Map::new();
            let mut offset = 0;
            while offset < payload.len() {
                let (key, used) = decode_jsonb(&payload[offset..])?;
                offset += used;
                let (value, used) = decode_jsonb(payload.get(offset..)?)?;
                offset += used;
                map.insert(key.as_str()?.to_string(), value);
            }
            Value::Object(map)
        }
        _ => return None,
    };
    Some((value, end))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) fn fixture_db(path: &Path) {
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(include_str!(
            "../../fixtures/trajectory_parity/mastracode.sql"
        ))
        .unwrap();
    }

    fn fixture() -> (tempfile::TempDir, PathBuf) {
        let temp = tempfile::tempdir().unwrap();
        let database = temp.path().join(DIRECTORY).join(DATABASE);
        std::fs::create_dir_all(database.parent().unwrap()).unwrap();
        fixture_db(&database);
        (temp, database)
    }

    fn parse(path: &Path, include_reasoning: bool) -> Vec<Record> {
        let next_doc_id = AtomicU64::new(0);
        let mut records = Vec::new();
        parse_index_records(
            path,
            IndexParseState::default(),
            include_reasoning,
            &next_doc_id,
            |record| {
                records.push(record);
                Ok(())
            },
        )
        .unwrap();
        records
    }

    #[test]
    fn jsonb_objects_decode() {
        // {"projectPath":"/tmp/p","n":1,"ok":true,"l":[null]}
        let object = |entries: &[&[u8]]| {
            let payload: Vec<u8> = entries.concat();
            let mut bytes = vec![0xcc, payload.len() as u8];
            bytes.extend(payload);
            bytes
        };
        let key = |text: &str| {
            let mut bytes = vec![((text.len() as u8) << 4) | 0x07];
            bytes.extend(text.as_bytes());
            bytes
        };
        let bytes = object(&[
            &key("projectPath"),
            &key("/tmp/p"),
            &key("n"),
            &[0x13, b'1'],
            &key("ok"),
            &[0x01],
            &key("l"),
            &[0x1b, 0x00],
        ]);
        let value = decode_metadata(&bytes).unwrap();
        assert_eq!(value["projectPath"], "/tmp/p");
        assert_eq!(value["n"], 1);
        assert_eq!(value["ok"], true);
        assert_eq!(value["l"], serde_json::json!([null]));
        assert_eq!(
            decode_metadata(br#"{"projectPath":"/x"}"#).unwrap()["projectPath"],
            "/x"
        );
    }

    #[test]
    fn virtual_path_round_trips() {
        let database = PathBuf::from("/data/mastracode/mastra.db");
        let path = virtual_path(&database, "abc-123");
        assert_eq!(
            split_virtual_path(&path),
            Some((database.clone(), "abc-123".to_string()))
        );
        assert!(matches_path(&path.to_string_lossy()));
        assert!(matches_path(&database.to_string_lossy()));
        assert!(!matches_path("/data/other/mastra.db"));
    }

    #[test]
    fn thread_projects_messages_tools_and_skips_scaffolding() {
        let (_temp, database) = fixture();
        let path = virtual_path(&database, "thread_main");
        let records = parse(&path, true);
        let roles = records
            .iter()
            .map(|record| (record.role.as_str(), record.text.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            roles,
            vec![
                ("user", "List the files"),
                ("reasoning", "thinking about it"),
                ("tool_use", "{\"command\":\"ls\"}"),
                ("tool_result", "a.txt"),
                ("assistant", "One file"),
            ]
        );
        assert_eq!(records[2].tool_name.as_deref(), Some("shell"));
        assert_eq!(records[2].links.event_id.as_deref(), Some("call_1"));
        assert_eq!(
            records[3].links.parent_tool_use_id.as_deref(),
            Some("call_1")
        );
        assert_eq!(records[0].project, "proj");
        assert_eq!(
            session_cwd(&path, "thread_main"),
            Some(PathBuf::from("/work/proj"))
        );
        assert_eq!(
            session_title(&path, "thread_main").as_deref(),
            Some("Listing files")
        );
        let without = parse(&path, false);
        assert!(without.iter().all(|record| record.role != "reasoning"));
    }

    #[test]
    fn fingerprint_ignores_signals_but_tracks_conversation() {
        let (_temp, database) = fixture();
        let before = enumerate_sessions(&database).unwrap();
        assert_eq!(before.len(), 1);
        assert_eq!(before[0].id, "thread_main");
        assert_eq!(before[0].size, 4);
        let conn = Connection::open(&database).unwrap();
        conn.execute(
            "UPDATE mastra_messages SET content = '{\"format\":2,\"parts\":[]}' \
             WHERE role = 'signal' AND type = 'system-reminder'",
            [],
        )
        .unwrap();
        assert_eq!(
            enumerate_sessions(&database).unwrap()[0].fingerprint,
            before[0].fingerprint
        );
        conn.execute(
            "UPDATE mastra_messages SET content = replace(content, 'One file', 'Two files')",
            [],
        )
        .unwrap();
        assert_ne!(
            enumerate_sessions(&database).unwrap()[0].fingerprint,
            before[0].fingerprint
        );
    }
}
