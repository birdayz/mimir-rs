//! Memberlist status page (`GET /memberlist`).
//!
//! Ported from:
//! - github.com/grafana/dskit/kv/memberlist/http_status_handler.go (`HTTPStatusHandler`,
//!   `StatusPageData`, `computeStoreSizes`, `viewKey`, `viewMessage`, `formatValue`,
//!   `downloadKey`, `deleteMessages`)
//! - github.com/grafana/dskit/kv/memberlist/status.gohtml (dskit's default page template, used by
//!   `KVInitService.ServeHTTP`)
//! - github.com/grafana/dskit/kv/memberlist/http_status_handler_test.go
//!
//! The handler is framework-agnostic: it returns a [`StatusResponse`] that the HTTP server turns
//! into a response. Mimir renders its own template (`pkg/api/memberlist_status.gohtml`), which is
//! passed in as a [`PageTemplate`] exactly like `memberlist.NewHTTPStatusHandler(kvs, templ)`.

use std::collections::{BTreeMap, HashMap};
use std::time::SystemTime;

use chrono::{DateTime, FixedOffset, Local};
use mimir_config::server::ZoneAwareRoutingConfig;
use mimir_proto::memberlistpb::KeyValuePair;

use super::client::{HistoryMessage, MemberlistKv, NodeState, ValueDesc};
use super::gofmt::{
    format_go_time_string, format_time_millis, go_ip_marshal_text, go_ip_string, go_zero_time,
    join_host_port, GoJson,
};
use super::node_meta::EncodedNodeMetadata;
use super::wire::NodeStateType;
use crate::dns::go_quote;

/// dskit `StatusPageData`: the data passed to the status page template / JSON encoder.
///
/// `Memberlist *memberlist.Memberlist` has no exported fields (JSON `{}`); templates only call its
/// `GetHealthScore` and `NumMembers` methods, captured here as [`Self::health_score`] and
/// [`Self::num_members`].
#[derive(Clone, Debug)]
pub struct StatusPageData {
    pub now: DateTime<FixedOffset>,
    /// `Memberlist.GetHealthScore()`.
    pub health_score: i32,
    /// `Memberlist.NumMembers()`.
    pub num_members: usize,
    pub sorted_members: Vec<NodeState>,
    pub store: BTreeMap<String, StatusValueDesc>,
    pub store_sizes: BTreeMap<String, usize>,
    pub total_store_size: usize,
    pub message_history_buffer_bytes: i64,
    pub sent_messages: Vec<StatusMessage>,
    pub received_messages: Vec<StatusMessage>,
    pub zone_aware_routing: ZoneAwareRoutingConfig,
}

/// The exported fields of dskit `ValueDesc` (the decoded value itself is unexported).
#[derive(Clone, Debug)]
pub struct StatusValueDesc {
    pub version: usize,
    pub codec_id: String,
    pub deleted: bool,
    /// Go zero time (`0001-01-01T00:00:00Z`) when never set.
    pub update_time: DateTime<FixedOffset>,
}

/// dskit `Message`.
#[derive(Clone, Debug)]
pub struct StatusMessage {
    pub id: u64,
    pub time: DateTime<FixedOffset>,
    pub size: usize,
    pub pair: KeyValuePair,
    pub version: usize,
    pub changes: Vec<String>,
}

/// Go `time.Now()` / stored `time.Time` values are in the local time zone.
fn local_time(t: SystemTime) -> DateTime<FixedOffset> {
    DateTime::<Local>::from(t).fixed_offset()
}

impl StatusMessage {
    fn from_history(m: HistoryMessage) -> Self {
        Self {
            id: m.id,
            time: local_time(m.time),
            size: m.size,
            pair: m.pair,
            version: m.version,
            changes: m.changes,
        }
    }

    fn to_go_json(&self) -> GoJson {
        // KeyValuePair: all fields `omitempty`.
        let p = &self.pair;
        let mut pair: Vec<(String, GoJson)> = Vec::new();
        if !p.key.is_empty() {
            pair.push(("key".into(), GoJson::str(p.key.as_str())));
        }
        if !p.value.is_empty() {
            pair.push(("value".into(), GoJson::bytes(Some(&p.value))));
        }
        if !p.codec.is_empty() {
            pair.push(("codec".into(), GoJson::str(p.codec.as_str())));
        }
        if p.deleted {
            pair.push(("deleted".into(), GoJson::Bool(true)));
        }
        if p.update_time_millis != 0 {
            pair.push(("update_time_millis".into(), GoJson::int(p.update_time_millis)));
        }
        GoJson::Object(vec![
            ("ID".into(), GoJson::int(self.id)),
            ("Time".into(), GoJson::time(&self.time)),
            ("Size".into(), GoJson::int(self.size as u64)),
            ("Pair".into(), GoJson::Object(pair)),
            ("Version".into(), GoJson::int(self.version as u64)),
            ("Changes".into(), GoJson::strings(&self.changes)),
        ])
    }
}

/// Go `memberlist.Node` JSON (`Addr` is a `net.IP`, `Meta` a `[]byte`, `State` an int).
fn node_to_go_json(n: &NodeState) -> Result<GoJson, String> {
    let addr = go_ip_marshal_text(&n.addr)
        .map_err(|e| format!("json: error calling MarshalText for type net.IP: {e}"))?;
    let vsn = |i: usize| GoJson::int(n.vsn.get(i).copied().unwrap_or(0));
    Ok(GoJson::Object(vec![
        ("Name".into(), GoJson::str(n.name.as_str())),
        ("Addr".into(), GoJson::Str(addr)),
        ("Port".into(), GoJson::int(n.port)),
        ("Meta".into(), GoJson::bytes(n.meta.as_deref().filter(|m| !m.is_empty()))),
        ("State".into(), GoJson::int(n.state as u8)),
        ("PMin".into(), vsn(0)),
        ("PMax".into(), vsn(1)),
        ("PCur".into(), vsn(2)),
        ("DMin".into(), vsn(3)),
        ("DMax".into(), vsn(4)),
        ("DCur".into(), vsn(5)),
    ]))
}

/// Go `(*memberlist.Node).Address()`.
pub fn node_address(n: &NodeState) -> String {
    join_host_port(&go_ip_string(&n.addr), n.port)
}

impl StatusPageData {
    /// The `StatusPageData` that dskit's `ServeHTTP` builds from the KV at `now`.
    pub fn from_kv(kv: &MemberlistKv, now: SystemTime) -> Self {
        let mut members = kv.members();
        members.sort_by(|a, b| a.name.cmp(&b.name));

        let (sent, received) = kv.sent_and_received_messages();

        let store = kv.store_copy();
        let (store_sizes, total_store_size) = compute_store_sizes(kv, &store);

        let cfg = kv.config();
        Self {
            now: local_time(now),
            health_score: kv.health_score(),
            num_members: kv.num_members(),
            sorted_members: members,
            store: store
                .into_iter()
                .map(|(k, v)| {
                    (
                        k,
                        StatusValueDesc {
                            version: v.version,
                            codec_id: v.codec_id,
                            deleted: v.deleted,
                            update_time: v.update_time.map(local_time).unwrap_or_else(go_zero_time),
                        },
                    )
                })
                .collect(),
            store_sizes,
            total_store_size,
            message_history_buffer_bytes: cfg.message_history_buffer_bytes,
            sent_messages: sent.into_iter().map(StatusMessage::from_history).collect(),
            received_messages: received.into_iter().map(StatusMessage::from_history).collect(),
            zone_aware_routing: cfg.zone_aware_routing.clone(),
        }
    }

    /// `json.NewEncoder(w).Encode(v)` of dskit's `StatusPageData`.
    pub fn to_go_json(&self) -> Result<String, String> {
        let members = self
            .sorted_members
            .iter()
            .map(node_to_go_json)
            .collect::<Result<Vec<_>, _>>()?;
        let messages = |m: &[StatusMessage]| {
            if m.is_empty() {
                GoJson::Null
            } else {
                GoJson::Array(m.iter().map(StatusMessage::to_go_json).collect())
            }
        };
        let store = self
            .store
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    GoJson::Object(vec![
                        ("Version".into(), GoJson::int(v.version as u64)),
                        ("CodecID".into(), GoJson::str(v.codec_id.as_str())),
                        ("Deleted".into(), GoJson::Bool(v.deleted)),
                        ("UpdateTime".into(), GoJson::time(&v.update_time)),
                    ]),
                )
            })
            .collect();
        let store_sizes = self
            .store_sizes
            .iter()
            .map(|(k, v)| (k.clone(), GoJson::int(*v as u64)))
            .collect();
        let zar = &self.zone_aware_routing;
        let v = GoJson::Object(vec![
            ("Now".into(), GoJson::time(&self.now)),
            ("Memberlist".into(), GoJson::Object(Vec::new())),
            // `memberlist.Members()` returns a non-nil slice.
            ("SortedMembers".into(), GoJson::Array(members)),
            ("Store".into(), GoJson::Object(store)),
            ("StoreSizes".into(), GoJson::Object(store_sizes)),
            ("TotalStoreSize".into(), GoJson::int(self.total_store_size as u64)),
            ("MessageHistoryBufferBytes".into(), GoJson::int(self.message_history_buffer_bytes)),
            ("SentMessages".into(), messages(&self.sent_messages)),
            ("ReceivedMessages".into(), messages(&self.received_messages)),
            (
                "ZoneAwareRouting".into(),
                GoJson::Object(vec![
                    ("Enabled".into(), GoJson::Bool(zar.enabled)),
                    ("Zone".into(), GoJson::str(zar.instance_availability_zone.as_str())),
                    ("Role".into(), GoJson::str(zar.role.as_str())),
                ]),
            ),
        ]);
        Ok(v.encode())
    }
}

/// dskit `computeStoreSizes`: encoded size of every stored value (entries whose codec is unknown
/// or fails to encode are skipped).
fn compute_store_sizes(
    kv: &MemberlistKv,
    store: &HashMap<String, ValueDesc>,
) -> (BTreeMap<String, usize>, usize) {
    let mut sizes = BTreeMap::new();
    let mut total = 0;
    for (key, val) in store {
        let Some(c) = kv.get_codec(&val.codec_id) else {
            continue;
        };
        let Ok(encoded) = c.encode_mergeable(val.value.as_ref()) else {
            continue;
        };
        sizes.insert(key.clone(), encoded.len());
        total += encoded.len();
    }
    (sizes, total)
}

/// A template rendering [`StatusPageData`] to HTML (Go `*template.Template`).
pub type PageTemplate = fn(&StatusPageData) -> String;

/// An HTTP response produced by [`serve_http`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatusResponse {
    pub status: u16,
    pub headers: Vec<(&'static str, String)>,
    pub body: Vec<u8>,
}

impl StatusResponse {
    fn new(status: u16, content_type: &str, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status,
            headers: vec![("content-type", content_type.to_string())],
            body: body.into(),
        }
    }

    /// Go `http.Error(w, msg, code)`.
    fn error(status: u16, msg: &str) -> Self {
        Self {
            status,
            headers: vec![
                ("content-type", "text/plain; charset=utf-8".to_string()),
                ("x-content-type-options", "nosniff".to_string()),
            ],
            body: format!("{msg}\n").into_bytes(),
        }
    }

    /// Header value by (lower-case) name.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

const DOWNLOAD_KEY_PARAM: &str = "downloadKey";
const VIEW_KEY_PARAM: &str = "viewKey";
const VIEW_MSG_PARAM: &str = "viewMsg";
const DELETE_MESSAGES_PARAM: &str = "deleteMessages";
const VIEW_FORMAT_PARAM: &str = "format";

/// dskit `HTTPStatusHandler.ServeHTTP`.
///
/// `kv` is `KVInitService.getKV()` (`None` when this instance doesn't use memberlist),
/// `raw_query` the request's URL query, `accept` its `Accept` header.
pub fn serve_http(
    kv: Option<&MemberlistKv>,
    raw_query: Option<&str>,
    accept: &str,
    tpl: PageTemplate,
) -> StatusResponse {
    let Some(kv) = kv else {
        return StatusResponse::new(200, "text/plain", "This instance doesn't use memberlist.");
    };

    if let Ok(form) = parse_query(raw_query.unwrap_or("")) {
        let first = |name: &str| form.get(name).and_then(|v| v.first()).map(String::as_str);
        let format = first(VIEW_FORMAT_PARAM).unwrap_or("");

        if let Some(key) = first(DOWNLOAD_KEY_PARAM) {
            return download_key(kv, &kv.store_copy(), key);
        }

        if let Some(key) = first(VIEW_KEY_PARAM) {
            return view_key(&kv.store_copy(), key, format);
        }

        if let Some(id) = first(VIEW_MSG_PARAM) {
            let msg_id = match go_atoi(id) {
                Ok(v) => v,
                Err(e) => return StatusResponse::error(400, &e),
            };
            let (sent, received) = kv.sent_and_received_messages();
            for m in sent.iter().chain(received.iter()) {
                if i128::from(m.id) == i128::from(msg_id) {
                    return view_message(kv, m, format);
                }
            }
            return StatusResponse::error(404, "message not found");
        }

        if first(DELETE_MESSAGES_PARAM) == Some("true") {
            kv.delete_sent_received_messages();
            // Redirect back.
            return StatusResponse {
                status: 302,
                headers: vec![("location", format!("?{DELETE_MESSAGES_PARAM}=false"))],
                body: Vec::new(),
            };
        }
    }

    let v = StatusPageData::from_kv(kv, SystemTime::now());

    if accept.contains("application/json") {
        return match v.to_go_json() {
            Ok(body) => StatusResponse::new(200, "application/json", body),
            Err(e) => StatusResponse::error(500, &e),
        };
    }

    StatusResponse::new(200, "text/html", tpl(&v))
}

/// dskit `viewMessage`.
fn view_message(kv: &MemberlistKv, msg: &HistoryMessage, format: &str) -> StatusResponse {
    let Some(c) = kv.get_codec(&msg.pair.codec) else {
        return StatusResponse::error(404, "codec not found");
    };
    match c.decode_mergeable(&msg.pair.value) {
        Ok(val) => format_value(val.as_ref(), format),
        Err(e) => StatusResponse::error(500, &format!("failed to decode: {e}")),
    }
}

/// dskit `viewKey`.
fn view_key(store: &HashMap<String, ValueDesc>, key: &str, format: &str) -> StatusResponse {
    match store.get(key) {
        Some(v) => format_value(v.value.as_ref(), format),
        None => StatusResponse::error(404, "value not found"),
    }
}

/// dskit `formatValue`. Go calls `w.WriteHeader(200)` before setting `content-type`, so the
/// header is dropped and net/http sniffs the (text) body: `text/plain; charset=utf-8`.
fn format_value(val: &dyn super::Mergeable, format: &str) -> StatusResponse {
    let body = match format {
        "json" => val.to_go_json().encode(),
        "json-pretty" => val.to_go_json().encode_indent("    "),
        _ => val.go_string(),
    };
    StatusResponse::new(200, "text/plain; charset=utf-8", body)
}

/// dskit `downloadKey`.
fn download_key(kv: &MemberlistKv, store: &HashMap<String, ValueDesc>, key: &str) -> StatusResponse {
    let Some(val) = store.get(key) else {
        return StatusResponse::error(404, "value not found");
    };
    let Some(c) = kv.get_codec(&val.codec_id) else {
        return StatusResponse::error(404, "codec not found");
    };
    let encoded = match c.encode_mergeable(val.value.as_ref()) {
        Ok(e) => e,
        Err(e) => return StatusResponse::error(500, &format!("failed to encode: {e}")),
    };
    StatusResponse {
        status: 200,
        headers: vec![
            ("content-type", "application/octet-stream".to_string()),
            // Set content-length so that client knows whether it has received full response or not.
            ("content-length", encoded.len().to_string()),
            ("content-disposition", format!("attachment; filename={}-{key}", val.version)),
        ],
        body: encoded,
    }
}

/// Go `strconv.Atoi` (64-bit `int`), including its error messages.
fn go_atoi(s: &str) -> Result<i64, String> {
    use std::num::IntErrorKind;
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!("strconv.Atoi: parsing {}: invalid syntax", go_quote(s)));
    }
    s.parse::<i64>().map_err(|e| match e.kind() {
        IntErrorKind::PosOverflow | IntErrorKind::NegOverflow => {
            format!("strconv.Atoi: parsing {}: value out of range", go_quote(s))
        }
        _ => format!("strconv.Atoi: parsing {}: invalid syntax", go_quote(s)),
    })
}

/// Go `url.ParseQuery` (as used by `Request.ParseForm` for a GET request). Any error makes
/// `ParseForm` fail, in which case dskit ignores all parameters.
fn parse_query(query: &str) -> Result<HashMap<String, Vec<String>>, ()> {
    let mut m: HashMap<String, Vec<String>> = HashMap::new();
    let mut failed = false;
    for part in query.split('&') {
        if part.contains(';') {
            failed = true;
            continue;
        }
        if part.is_empty() {
            continue;
        }
        let (k, v) = part.split_once('=').unwrap_or((part, ""));
        let (Some(k), Some(v)) = (query_unescape(k), query_unescape(v)) else {
            failed = true;
            continue;
        };
        m.entry(k).or_default().push(v);
    }
    if failed {
        Err(())
    } else {
        Ok(m)
    }
}

/// Go `url.QueryUnescape`.
fn query_unescape(s: &str) -> Option<String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' => {
                let hex = |c: u8| (c as char).to_digit(16);
                let hi = b.get(i + 1).copied().and_then(hex)?;
                let lo = b.get(i + 2).copied().and_then(hex)?;
                out.push((hi * 16 + lo) as u8);
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    Some(String::from_utf8_lossy(&out).into_owned())
}

/// Go `html/template` escaping of a value in HTML text / quoted attribute context
/// (`htmlEscaper`).
pub fn html_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\0' => out.push('\u{FFFD}'),
            '"' => out.push_str("&#34;"),
            '&' => out.push_str("&amp;"),
            '\'' => out.push_str("&#39;"),
            '+' => out.push_str("&#43;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            c => out.push(c),
        }
    }
    out
}

/// Go `html/template` escaping of a value in a URL query part inside an attribute
/// (`urlEscaper`: everything but RFC 3986 unreserved characters is percent-encoded, lower-case hex).
pub fn url_query_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02x}"));
        }
    }
    out
}

/// `GetZoneFromMeta` template function.
pub fn zone_from_meta(meta: Option<&[u8]>) -> String {
    EncodedNodeMetadata(meta.unwrap_or_default()).zone().to_string()
}

/// `GetRoleFromMeta` template function.
pub fn role_from_meta(meta: Option<&[u8]>) -> String {
    EncodedNodeMetadata(meta.unwrap_or_default()).role().to_string()
}

/// `{{ .State }}` of a `memberlist.Node` (`NodeStateType` has no `String` method).
pub fn node_state_int(s: NodeStateType) -> u8 {
    s as u8
}

/// dskit's default page template (`status.gohtml`, `defaultPageTemplate`), as rendered by
/// `KVInitService.ServeHTTP`. `{{ .Now }}` / `{{ $v.UpdateTime }}` print Go's `time.Time.String()`
/// without the monotonic clock reading (`m=+...`) Go appends for `time.Now()` values.
pub fn render_default_page(d: &StatusPageData) -> String {
    let mut out = String::with_capacity(4096);
    out.push_str(
        r#"<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Memberlist Status</title>
</head>
<body>
<h1>Memberlist Status</h1>
<p>Current time: "#,
    );
    out.push_str(&html_escape(&format_go_time_string(&d.now)));
    out.push_str(
        r#"</p>

<ul>
    <li>Health Score: "#,
    );
    out.push_str(&d.health_score.to_string());
    out.push_str(
        r#" (lower = better, 0 = healthy)</li>
    <li>Members: "#,
    );
    out.push_str(&d.num_members.to_string());
    out.push_str(
        r#"</li>
    <li>Zone-aware routing: "#,
    );
    out.push_str(if d.zone_aware_routing.enabled { "enabled" } else { "disabled" });
    out.push_str(
        r#"</li>
</ul>

<h2>KV Store</h2>

<table width="100%" border="1">
    <thead>
    <tr>
        <th>Key</th>
        <th>Codec</th>
        <th>Version</th>
        <th>Deleted</th>
        <th>Update Time</th>
        <th>Size</th>
        <th>Actions</th>
    </tr>
    </thead>

    <tbody>
    "#,
    );
    for (k, v) in &d.store {
        let k_html = html_escape(k);
        let k_url = url_query_escape(k);
        out.push_str(
            r#"
        <tr>
            <td>"#,
        );
        out.push_str(&k_html);
        out.push_str(
            r#"</td>
            <td>"#,
        );
        out.push_str(&html_escape(&v.codec_id));
        out.push_str(
            r#"</td>
            <td>"#,
        );
        out.push_str(&v.version.to_string());
        out.push_str(
            r#"</td>
            <td>"#,
        );
        out.push_str(if v.deleted { "true" } else { "false" });
        out.push_str(
            r#"</td>
            <td>"#,
        );
        out.push_str(&html_escape(&format_go_time_string(&v.update_time)));
        out.push_str(
            r#"</td>
            <td>"#,
        );
        out.push_str(&d.store_sizes.get(k).copied().unwrap_or(0).to_string());
        out.push_str(
            r#"</td>
            <td>
                <a href="?viewKey="#,
        );
        out.push_str(&k_url);
        out.push_str(
            r#"&format=json">json</a>
                | <a href="?viewKey="#,
        );
        out.push_str(&k_url);
        out.push_str(
            r#"&format=json-pretty">json-pretty</a>
                | <a href="?viewKey="#,
        );
        out.push_str(&k_url);
        out.push_str(
            r#"&format=struct">struct</a>
                | <a href="?downloadKey="#,
        );
        out.push_str(&k_url);
        out.push_str(
            r#"">download</a>
            </td>
        </tr>
    "#,
        );
    }
    out.push_str(
        r#"
    </tbody>

    <tfoot>
        <tr>
            <td colspan="5" style="text-align: right"><strong>Total:</strong></td>
            <td>"#,
    );
    out.push_str(&d.total_store_size.to_string());
    out.push_str(
        r#"</td>
            <td></td>
        </tr>
    </tfoot>
</table>

<p>Note that value "version" is node-specific. It starts with 0 (on restart), and increases on each received update.
    "Size" shows the encoded size in bytes.</p>

<h2>Memberlist Cluster Members</h2>

<table width="100%" border="1">
    <thead>
    <tr>
        <th>Name</th>
        <th>Address</th>
        <th>State</th>
        "#,
    );
    if d.zone_aware_routing.enabled {
        out.push_str(
            r#"
            <th>Zone</th>
            <th>Role</th>
        "#,
        );
    }
    out.push_str(
        r#"
    </tr>
    </thead>

    <tbody>
    "#,
    );
    for m in &d.sorted_members {
        out.push_str(
            r#"
        <tr>
            <td>"#,
        );
        out.push_str(&html_escape(&m.name));
        out.push_str(
            r#"</td>
            <td>"#,
        );
        out.push_str(&html_escape(&node_address(m)));
        out.push_str(
            r#"</td>
            <td>"#,
        );
        out.push_str(&node_state_int(m.state).to_string());
        out.push_str(
            r#"</td>
            "#,
        );
        if d.zone_aware_routing.enabled {
            out.push_str(
                r#"
                <td>"#,
            );
            out.push_str(&html_escape(&zone_from_meta(m.meta.as_deref())));
            out.push_str(
                r#"</td>
                <td>"#,
            );
            out.push_str(&html_escape(&role_from_meta(m.meta.as_deref())));
            out.push_str(
                r#"</td>
            "#,
            );
        }
        out.push_str(
            r#"
        </tr>
    "#,
        );
    }
    out.push_str(
        r#"
    </tbody>
</table>

<p>State: 0 = Alive, 1 = Suspect, 2 = Dead, 3 = Left</p>

<h2>Message History</h2>

"#,
    );
    if d.message_history_buffer_bytes != 0 {
        out.push_str(
            r#"

    <h3>Received Messages</h3>

    <a href="?deleteMessages=true">Delete All Messages (received and sent)</a>

    <table width="100%" border="1">
        <thead>
        <tr>
            <th>ID</th>
            <th>Time</th>
            <th>Key</th>
            <th>Value in the Message</th>
            <th>Version After Update (0 = no change)</th>
            <th>Changes</th>
            <th>Actions</th>
        </tr>
        </thead>

        <tbody>
        "#,
        );
        default_page_messages(&mut out, &d.received_messages);
        out.push_str(
            r#"
        </tbody>
    </table>

    <h3>Sent Messages</h3>

    <a href="?deleteMessages=true">Delete All Messages (received and sent)</a>

    <table width="100%" border="1">
        <thead>
        <tr>
            <th>ID</th>
            <th>Time</th>
            <th>Key</th>
            <th>Value</th>
            <th>Version</th>
            <th>Changes</th>
            <th>Actions</th>
        </tr>
        </thead>

        <tbody>
        "#,
        );
        default_page_messages(&mut out, &d.sent_messages);
        out.push_str(
            r#"
        </tbody>
    </table>
"#,
        );
    } else {
        out.push_str(
            r#"
    <p><i>Message history buffer is disabled, refer to the configuration to enable it in order to troubleshoot the message history.</i></p>
"#,
        );
    }
    out.push_str(
        r#"
</body>
</html>"#,
    );
    out
}

fn default_page_messages(out: &mut String, msgs: &[StatusMessage]) {
    for m in msgs {
        let id = m.id.to_string();
        out.push_str(
            r#"
            <tr>
                <td>"#,
        );
        out.push_str(&id);
        out.push_str(
            r#"</td>
                <td>"#,
        );
        out.push_str(&format_time_millis(&m.time));
        out.push_str(
            r#"</td>
                <td>"#,
        );
        out.push_str(&html_escape(&m.pair.key));
        out.push_str(
            r#"</td>
                <td>size: "#,
        );
        out.push_str(&m.pair.value.len().to_string());
        out.push_str(", codec: ");
        out.push_str(&html_escape(&m.pair.codec));
        out.push_str(
            r#"</td>
                <td>"#,
        );
        out.push_str(&m.version.to_string());
        out.push_str(
            r#"</td>
                <td>"#,
        );
        out.push_str(&html_escape(&m.changes.join(", ")));
        out.push_str(
            r#"</td>
                <td>
                    <a href="?viewMsg="#,
        );
        out.push_str(&id);
        out.push_str(
            r#"&format=json">json</a>
                    | <a href="?viewMsg="#,
        );
        out.push_str(&id);
        out.push_str(
            r#"&format=json-pretty">json-pretty</a>
                    | <a href="?viewMsg="#,
        );
        out.push_str(&id);
        out.push_str(
            r#"&format=struct">struct</a>
                </td>
            </tr>
        "#,
        );
    }
}

#[cfg(test)]
#[path = "http_status_tests.rs"]
mod tests;
