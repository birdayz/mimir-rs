//! Ported from github.com/grafana/dskit/kv/memberlist/http_status_handler_test.go (`TestPage`),
//! plus handler cases for the query parameters of http_status_handler.go.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use mimir_config::duration::GoDuration;
use mimir_config::server::MemberlistConfig;
use mimir_proto::ringpb::{Desc, InstanceDesc, InstanceState};
use regex::Regex;

use super::*;
use crate::kv::{CasOutcome, KvClient};
use crate::ring::model::ring_desc_codec;

fn test_cfg(name: &str, history_bytes: i64) -> MemberlistConfig {
    MemberlistConfig {
        bind_addr: vec!["127.0.0.1".to_string()],
        bind_port: 0,
        node_name: name.to_string(),
        randomize_node_name: false,
        gossip_interval: GoDuration::from_millis(100),
        message_history_buffer_bytes: history_bytes,
        ..Default::default()
    }
}

async fn kv_with_test_key(history_bytes: i64) -> Arc<MemberlistKv> {
    let kv = MemberlistKv::start(test_cfg("status", history_bytes), vec![ring_desc_codec()], "", None)
        .await
        .unwrap();
    let client = super::super::MemberlistKvClient::new(Arc::clone(&kv), ring_desc_codec()).unwrap();
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
    client
        .cas(
            "test-key",
            Box::new(move |_| {
                let mut d = Desc::default();
                d.ingesters.insert(
                    "member1".to_string(),
                    InstanceDesc {
                        addr: "1.2.3.4:9095".to_string(),
                        timestamp: now,
                        state: InstanceState::Active as i32,
                        tokens: vec![1, 2, 3],
                        ..Default::default()
                    },
                );
                Ok(CasOutcome::Update(Box::new(d)))
            }),
        )
        .await
        .unwrap();
    kv
}

fn get(kv: &MemberlistKv, query: Option<&str>, accept: &str) -> StatusResponse {
    serve_http(Some(kv), query, accept, render_default_page)
}

fn body(r: &StatusResponse) -> String {
    String::from_utf8(r.body.clone()).unwrap()
}

// Upstream: github.com/grafana/dskit/kv/memberlist/http_status_handler_test.go::TestPage
#[tokio::test]
async fn test_page() {
    let kv = kv_with_test_key(0).await;

    let rec = get(&kv, None, "");

    assert_eq!(rec.status, 200);
    assert_eq!(rec.header("content-type"), Some("text/html"));
    let b = body(&rec);

    // Verify Size column header
    assert!(Regex::new(r"(?m)<th>\s*Size\s*</th>").unwrap().is_match(&b), "{b}");

    // Verify size value appears in a table cell (non-zero)
    assert!(Regex::new(r"<td>\d+</td>").unwrap().is_match(&b), "{b}");

    // Verify Total row exists
    assert!(Regex::new(r"(?m)<strong>\s*Total:\s*</strong>").unwrap().is_match(&b), "{b}");

    kv.shutdown().await;
}

#[tokio::test]
async fn test_json_is_dskit_status_page_data() {
    let kv = kv_with_test_key(0).await;

    let rec = get(&kv, None, "text/html, application/json");
    assert_eq!(rec.status, 200);
    assert_eq!(rec.header("content-type"), Some("application/json"));
    let b = body(&rec);
    assert!(b.ends_with("}\n"), "json.Encoder appends a newline");
    let v: serde_json::Value = serde_json::from_str(&b).unwrap();

    let keys: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
    let mut want = vec![
        "Now",
        "Memberlist",
        "SortedMembers",
        "Store",
        "StoreSizes",
        "TotalStoreSize",
        "MessageHistoryBufferBytes",
        "SentMessages",
        "ReceivedMessages",
        "ZoneAwareRouting",
    ];
    let mut got = keys.clone();
    got.sort_unstable();
    want.sort_unstable();
    assert_eq!(got, want);
    // Go struct field order.
    assert!(b.starts_with("{\"Now\":\""), "{b}");
    assert_eq!(v["Memberlist"], serde_json::json!({}));

    let m = &v["SortedMembers"][0];
    assert_eq!(m["Name"], "status");
    assert_eq!(m["Addr"], "127.0.0.1");
    assert_eq!(m["Port"].as_u64().unwrap(), u64::from(kv.listening_port()));
    assert_eq!(m["Meta"], serde_json::Value::Null);
    assert_eq!(m["State"], 0);
    assert_eq!((m["PMin"].as_u64(), m["PMax"].as_u64(), m["PCur"].as_u64()), (Some(1), Some(5), Some(2)));

    let s = &v["Store"]["test-key"];
    assert_eq!(s["CodecID"], "ringDesc");
    assert_eq!(s["Deleted"], false);
    assert!(s["Version"].as_u64().unwrap() >= 1);
    assert!(s["UpdateTime"].as_str().unwrap().len() >= 20);
    let size = v["StoreSizes"]["test-key"].as_u64().unwrap();
    assert!(size > 0);
    assert_eq!(v["TotalStoreSize"].as_u64().unwrap(), size);
    assert_eq!(v["MessageHistoryBufferBytes"], 0);
    assert_eq!(v["SentMessages"], serde_json::Value::Null);
    assert_eq!(v["ReceivedMessages"], serde_json::Value::Null);
    assert_eq!(
        v["ZoneAwareRouting"],
        serde_json::json!({"Enabled": false, "Zone": "", "Role": "member"})
    );

    kv.shutdown().await;
}

#[tokio::test]
async fn test_view_and_download_key() {
    let kv = kv_with_test_key(0).await;

    let r = get(&kv, Some("viewKey=test-key&format=json"), "");
    assert_eq!(r.status, 200);
    assert_eq!(r.header("content-type"), Some("text/plain; charset=utf-8"));
    let b = body(&r);
    assert!(
        b.starts_with(r#"{"ingesters":{"member1":{"addr":"1.2.3.4:9095","timestamp":"#),
        "{b}"
    );
    assert!(b.ends_with("\"tokens\":[1,2,3]}}}\n"), "{b}");

    let b = body(&get(&kv, Some("viewKey=test-key&format=json-pretty"), ""));
    assert!(b.starts_with("{\n    \"ingesters\": {\n        \"member1\": {\n"), "{b}");

    let b = body(&get(&kv, Some("viewKey=test-key&format=struct"), ""));
    assert!(
        b.starts_with("&ring.Desc{Ingesters: map[string]InstanceDesc{\"member1\": ring.InstanceDesc{Addr:\"1.2.3.4:9095\", Timestamp:"),
        "{b}"
    );
    assert!(b.ends_with(", Tokens:[]uint32{0x1, 0x2, 0x3}, Zone:\"\", RegisteredTimestamp:0, Id:\"\", ReadOnlyUpdatedTimestamp:0, ReadOnly:false, Versions:map[uint64]uint64(nil)},},\n}"), "{b}");

    let r = get(&kv, Some("viewKey=missing"), "");
    assert_eq!((r.status, body(&r).as_str()), (404, "value not found\n"));
    assert_eq!(r.header("x-content-type-options"), Some("nosniff"));

    let r = get(&kv, Some("downloadKey=test-key"), "");
    assert_eq!(r.status, 200);
    assert_eq!(r.header("content-type"), Some("application/octet-stream"));
    assert_eq!(r.header("content-length"), Some(r.body.len().to_string().as_str()));
    let version = kv.store_copy()["test-key"].version;
    assert_eq!(
        r.header("content-disposition"),
        Some(format!("attachment; filename={version}-test-key").as_str())
    );
    let decoded = ring_desc_codec().decode_mergeable(&r.body).unwrap();
    let d = decoded.as_any().downcast_ref::<Desc>().unwrap();
    assert_eq!(d.ingesters["member1"].tokens, vec![1, 2, 3]);

    let r = get(&kv, Some("downloadKey=missing"), "");
    assert_eq!((r.status, body(&r).as_str()), (404, "value not found\n"));

    kv.shutdown().await;
}

#[tokio::test]
async fn test_message_history_view_and_delete() {
    let kv = kv_with_test_key(1 << 20).await;

    let (sent, _) = kv.sent_and_received_messages();
    assert!(!sent.is_empty(), "CAS records a sent message");
    let id = sent[0].id;

    let v: serde_json::Value = serde_json::from_slice(&get(&kv, None, "application/json").body).unwrap();
    assert_eq!(v["MessageHistoryBufferBytes"], 1 << 20);
    let msg = &v["SentMessages"][0];
    assert_eq!(msg["ID"].as_u64(), Some(id));
    assert_eq!(msg["Pair"]["key"], "test-key");
    assert_eq!(msg["Pair"]["codec"], "ringDesc");
    assert!(msg["Pair"]["value"].is_string());
    assert_eq!(msg["Changes"], serde_json::json!(["member1"]));

    let html = body(&get(&kv, None, ""));
    assert!(html.contains("<h3>Sent Messages</h3>"), "{html}");
    assert!(html.contains(&format!("<a href=\"?viewMsg={id}&format=json\">json</a>")), "{html}");

    let r = get(&kv, Some(&format!("viewMsg={id}&format=json")), "");
    assert_eq!(r.status, 200);
    assert!(body(&r).starts_with(r#"{"ingesters":{"member1":"#), "{}", body(&r));

    let r = get(&kv, Some("viewMsg=abc"), "");
    assert_eq!(
        (r.status, body(&r).as_str()),
        (400, "strconv.Atoi: parsing \"abc\": invalid syntax\n")
    );
    let r = get(&kv, Some("viewMsg=99999999999999999999"), "");
    assert_eq!(
        (r.status, body(&r).as_str()),
        (400, "strconv.Atoi: parsing \"99999999999999999999\": value out of range\n")
    );
    let r = get(&kv, Some("viewMsg=123456"), "");
    assert_eq!((r.status, body(&r).as_str()), (404, "message not found\n"));

    // deleteMessages=false is ignored.
    assert_eq!(get(&kv, Some("deleteMessages=false"), "").status, 200);
    assert!(!kv.sent_and_received_messages().0.is_empty());

    let r = get(&kv, Some("deleteMessages=true"), "");
    assert_eq!(r.status, 302);
    assert_eq!(r.header("location"), Some("?deleteMessages=false"));
    assert!(r.body.is_empty());
    let (sent, received) = kv.sent_and_received_messages();
    assert!(sent.is_empty() && received.is_empty());

    kv.shutdown().await;
}

#[tokio::test]
async fn test_invalid_query_is_ignored_like_parse_form() {
    let kv = kv_with_test_key(0).await;
    for q in ["viewKey=%zz", "viewKey=test-key;format=json", "a=%"] {
        let r = get(&kv, Some(q), "");
        assert_eq!(r.status, 200, "{q}");
        assert_eq!(r.header("content-type"), Some("text/html"), "{q}");
    }
    // Decoded keys (`+` = space, `%2F`).
    let r = get(&kv, Some("viewKey=test%2Dkey&format=json"), "");
    assert_eq!(r.status, 200);
    kv.shutdown().await;
}

#[test]
fn test_no_memberlist_kv() {
    let r = serve_http(None, Some("viewKey=x"), "application/json", render_default_page);
    assert_eq!(r.status, 200);
    assert_eq!(r.header("content-type"), Some("text/plain"));
    assert_eq!(body(&r), "This instance doesn't use memberlist.");
}

#[test]
fn test_go_atoi_and_query() {
    assert_eq!(go_atoi("+12"), Ok(12));
    assert_eq!(go_atoi("-3"), Ok(-3));
    assert!(go_atoi("").unwrap_err().ends_with("parsing \"\": invalid syntax"));
    assert!(go_atoi("1_0").is_err());
    let q = parse_query("a=1&a=2&b=x+y%2Fz&&c").unwrap();
    assert_eq!(q["a"], vec!["1", "2"]);
    assert_eq!(q["b"], vec!["x y/z"]);
    assert_eq!(q["c"], vec![""]);
    assert!(parse_query("a=%G0").is_err());
}

#[test]
fn test_template_escapers() {
    assert_eq!(html_escape("<a href='x'>\"&+\0"), "&lt;a href=&#39;x&#39;&gt;&#34;&amp;&#43;\u{FFFD}");
    assert_eq!(url_query_escape("collectors/ring a+b~._-"), "collectors%2fring%20a%2bb~._-");
}

#[test]
fn test_status_page_data_json_is_deterministic() {
    let tz = FixedOffset::east_opt(0).unwrap();
    let now = DateTime::from_timestamp(1_700_000_000, 5_000).unwrap().with_timezone(&tz);
    let d = StatusPageData {
        now,
        health_score: 0,
        num_members: 1,
        sorted_members: vec![NodeState {
            name: "n1".into(),
            addr: std::net::Ipv4Addr::new(10, 0, 0, 1).to_ipv6_mapped().octets().to_vec(),
            port: 7946,
            meta: None,
            incarnation: 1,
            state: NodeStateType::Alive,
            state_change: SystemTime::now(),
            vsn: vec![1, 5, 2, 0, 0, 0],
        }],
        store: BTreeMap::new(),
        store_sizes: BTreeMap::new(),
        total_store_size: 0,
        message_history_buffer_bytes: 0,
        sent_messages: vec![],
        received_messages: vec![],
        zone_aware_routing: ZoneAwareRoutingConfig::default(),
    };
    assert_eq!(
        d.to_go_json().unwrap(),
        "{\"Now\":\"2023-11-14T22:13:20.000005Z\",\"Memberlist\":{},\"SortedMembers\":[{\"Name\":\"n1\",\"Addr\":\"10.0.0.1\",\"Port\":7946,\"Meta\":null,\"State\":0,\"PMin\":1,\"PMax\":5,\"PCur\":2,\"DMin\":0,\"DMax\":0,\"DCur\":0}],\"Store\":{},\"StoreSizes\":{},\"TotalStoreSize\":0,\"MessageHistoryBufferBytes\":0,\"SentMessages\":null,\"ReceivedMessages\":null,\"ZoneAwareRouting\":{\"Enabled\":false,\"Zone\":\"\",\"Role\":\"member\"}}\n"
    );
}
