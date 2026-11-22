//! Ported from:
//! - github.com/grafana/dskit/ring/shard/shard_test.go
//! - github.com/grafana/dskit/dns/miekgdns/resolver_test.go

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::net::{IpAddr, Ipv4Addr};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::FutureExt;
use hickory_resolver::proto::op::{Message, MessageType, ResponseCode};
use hickory_resolver::proto::rr::rdata::{A, CNAME, SRV};
use hickory_resolver::proto::rr::{DNSClass, Name, RData, Record, RecordType};
use hickory_resolver::proto::serialize::binary::{BinDecodable, BinEncodable};
use mimir_ring::dns::miekgdns::{
    new_dns_question, ClientConfig, DnsClient, Pool, PoolingClient, Resolver as MiekgResolver,
    DEFAULT_MAX_CONNS_PER_HOST, DEFAULT_RESOLV_CONF_PATH,
};
use mimir_ring::dns::{LookupError, Srv};
use mimir_ring::prng::{
    shuffle_shard_expected_instances, shuffle_shard_expected_instances_per_zone, yolo_buf,
};
use parking_lot::Mutex;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

// ============================================================================
// github.com/grafana/dskit/ring/shard/shard_test.go
// ============================================================================

// Upstream: github.com/grafana/dskit/ring/shard/shard_test.go::TestShuffleShardExpectedInstancesPerZone
#[test]
fn test_shuffle_shard_expected_instances_per_zone() {
    let cases = [
        (1usize, 1usize, 1usize),
        (1, 3, 1),
        (3, 3, 1),
        (4, 3, 2),
        (6, 3, 2),
        (usize::MAX, 3, usize::MAX),
    ];
    for (shard_size, num_zones, expected) in cases {
        assert_eq!(
            expected,
            shuffle_shard_expected_instances_per_zone(shard_size, num_zones),
            "shard_size={shard_size}, num_zones={num_zones}"
        );
    }
}

// Upstream: github.com/grafana/dskit/ring/shard/shard_test.go::TestShuffleShardExpectedInstances
#[test]
fn test_shuffle_shard_expected_instances() {
    let cases = [
        (1usize, 1usize, 1usize),
        (1, 3, 3),
        (3, 3, 3),
        (4, 3, 6),
        (6, 3, 6),
    ];
    for (shard_size, num_zones, expected) in cases {
        assert_eq!(
            expected,
            shuffle_shard_expected_instances(shard_size, num_zones),
            "shard_size={shard_size}, num_zones={num_zones}"
        );
    }
}

// Upstream: github.com/grafana/dskit/ring/shard/shard_test.go::TestYoloBuf
#[test]
fn test_yolo_buf() {
    let s = yolo_buf("hello world");
    assert_eq!(b"hello world", s);
}

// ============================================================================
// github.com/grafana/dskit/dns/miekgdns/resolver_test.go
// ============================================================================

const RESOLV_CONF_CONTENTS: &str = "
nameserver 127.0.0.53
nameserver 127.0.0.54
options attempts:2
";

#[derive(Debug)]
struct TestStrErr(String);

impl fmt::Display for TestStrErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TestStrErr {}

struct MockDnsClient {
    err: Mutex<HashMap<String, String>>,
    res: Mutex<HashMap<String, VecDeque<Message>>>,
    cleaned: AtomicU64,
}

impl MockDnsClient {
    fn new() -> Self {
        Self {
            err: Mutex::new(HashMap::new()),
            res: Mutex::new(HashMap::new()),
            cleaned: AtomicU64::new(0),
        }
    }

    fn set_err(&self, server: &str, msg: &str) {
        self.err.lock().insert(server.to_string(), msg.to_string());
    }

    fn push_res(&self, server: &str, msgs: Vec<Message>) {
        self.res
            .lock()
            .entry(server.to_string())
            .or_default()
            .extend(msgs);
    }
}

impl DnsClient for MockDnsClient {
    fn exchange<'a>(
        &'a self,
        _msg: &'a Message,
        server: &'a str,
    ) -> BoxFuture<'a, Result<(Message, Duration), LookupError>> {
        async move {
            if let Some(e) = self.err.lock().get(server).cloned() {
                return Err(Box::new(TestStrErr(e)) as LookupError);
            }
            let mut map = self.res.lock();
            let q = map
                .get_mut(server)
                .expect("mock client had no responses for server");
            let resp = q.pop_front().expect("mock client response queue empty");
            Ok((resp, Duration::ZERO))
        }
        .boxed()
    }

    fn clean(&self, _known: &[String]) {
        self.cleaned.fetch_add(1, Ordering::Relaxed);
    }
}

fn write_resolv_conf(dir: &Path, contents: &str) -> String {
    let p = dir.join("resolv.conf");
    std::fs::write(&p, contents).unwrap();
    p.to_string_lossy().into_owned()
}

fn fqdn(s: &str) -> Name {
    let mut n = Name::from_str_relaxed(s).unwrap();
    n.set_fqdn(true);
    n
}

fn new_srv_dns_response(host: &str, target: &str) -> Message {
    let mut msg = Message::new();
    msg.set_id(1);
    msg.set_message_type(MessageType::Response);
    msg.set_response_code(ResponseCode::NoError);
    let q = hickory_resolver::proto::op::Query::query(fqdn(host), RecordType::SRV);
    msg.add_query(q);
    let rec = Record::from_rdata(
        fqdn(host),
        30,
        RData::SRV(SRV::new(10, 100, 11211, fqdn(target))),
    );
    msg.add_answer(rec);
    msg
}

fn new_ip_dns_response(host: &str, addr: Ipv4Addr) -> Message {
    let mut msg = Message::new();
    msg.set_id(1);
    msg.set_message_type(MessageType::Response);
    msg.set_response_code(ResponseCode::NoError);
    let q = hickory_resolver::proto::op::Query::query(fqdn(host), RecordType::A);
    msg.add_query(q);
    let rec = Record::from_rdata(fqdn(host), 30, RData::A(A(addr)));
    msg.add_answer(rec);
    msg
}

fn new_cname_dns_response(host: &str, target: &str) -> Message {
    let mut msg = Message::new();
    msg.set_id(1);
    msg.set_message_type(MessageType::Response);
    msg.set_response_code(ResponseCode::NoError);
    let q = hickory_resolver::proto::op::Query::query(fqdn(host), RecordType::CNAME);
    msg.add_query(q);
    let rec = Record::from_rdata(fqdn(host), 30, RData::CNAME(CNAME(fqdn(target))));
    msg.add_answer(rec);
    msg
}

async fn spawn_local_tcp_dns_server() -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let handle = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                loop {
                    let mut len_buf = [0u8; 2];
                    if stream.read_exact(&mut len_buf).await.is_err() {
                        break;
                    }
                    let len = u16::from_be_bytes(len_buf) as usize;
                    let mut req_buf = vec![0u8; len];
                    if stream.read_exact(&mut req_buf).await.is_err() {
                        break;
                    }
                    let Ok(req) = Message::from_bytes(&req_buf) else {
                        break;
                    };
                    let mut resp = Message::new();
                    resp.set_id(req.id());
                    resp.set_message_type(MessageType::Response);
                    resp.set_response_code(ResponseCode::NoError);
                    for q in req.queries() {
                        resp.add_query(q.clone());
                        if q.query_type() == RecordType::A {
                            let mut rec = Record::from_rdata(
                                q.name().clone(),
                                30,
                                RData::A(A(Ipv4Addr::LOCALHOST)),
                            );
                            rec.set_dns_class(DNSClass::IN);
                            resp.add_answer(rec);
                        }
                    }
                    let bytes = resp.to_bytes().unwrap();
                    let resp_len = (bytes.len() as u16).to_be_bytes();
                    if stream.write_all(&resp_len).await.is_err() {
                        break;
                    }
                    if stream.write_all(&bytes).await.is_err() {
                        break;
                    }
                    let _ = stream.flush().await;
                }
            });
        }
    });
    (addr, handle)
}

// Upstream: github.com/grafana/dskit/dns/miekgdns/resolver_test.go::TestResolver_ConfigParsing
#[test]
fn test_resolver_config_parsing() {
    let period = Duration::from_secs(3600);
    let tmp = TempDir::new().unwrap();

    // missing resolv.conf
    {
        let cfg_path = tmp.path().join("resolv.conf");
        let client = Arc::new(MockDnsClient::new());
        let resolver = MiekgResolver::new_with_client(
            cfg_path.to_string_lossy().as_ref(),
            period,
            client,
        );
        let conf = resolver.get_config();
        assert_eq!(vec!["127.0.0.1".to_string()], conf.servers);
        assert_eq!(2, conf.attempts);
        resolver.stop();
    }

    // attempts not included
    {
        let cfg_path = write_resolv_conf(tmp.path(), "nameserver 127.0.0.53\n");
        let client = Arc::new(MockDnsClient::new());
        let resolver = MiekgResolver::new_with_client(cfg_path, period, client);
        let conf = resolver.get_config();
        assert_eq!(vec!["127.0.0.53".to_string()], conf.servers);
        assert_eq!(2, conf.attempts);
        resolver.stop();
    }

    // attempts overridden
    {
        let cfg_path = write_resolv_conf(
            tmp.path(),
            "nameserver 127.0.0.53\noptions attempts:3\n",
        );
        let client = Arc::new(MockDnsClient::new());
        let resolver = MiekgResolver::new_with_client(cfg_path, period, client);
        let conf = resolver.get_config();
        assert_eq!(vec!["127.0.0.53".to_string()], conf.servers);
        assert_eq!(3, conf.attempts);
        resolver.stop();
    }

    // unknown settings ignored
    {
        let cfg_path = write_resolv_conf(
            tmp.path(),
            "nameserver 127.0.0.53\noptions attempts:3\nfoo bar\noptions whatever:4\noptions single-request\n",
        );
        let client = Arc::new(MockDnsClient::new());
        let resolver = MiekgResolver::new_with_client(cfg_path, period, client);
        let conf = resolver.get_config();
        assert_eq!(vec!["127.0.0.53".to_string()], conf.servers);
        assert_eq!(3, conf.attempts);
        resolver.stop();
    }
}

// Upstream: github.com/grafana/dskit/dns/miekgdns/resolver_test.go::TestResolver_LookupSRV
#[tokio::test]
async fn test_resolver_lookup_srv() {
    let period = Duration::from_secs(3600);
    let tmp = TempDir::new().unwrap();

    // multiple timeouts
    {
        let cfg_path = write_resolv_conf(tmp.path(), RESOLV_CONF_CONTENTS);
        let client = Arc::new(MockDnsClient::new());
        client.set_err("127.0.0.53:53", "timeout 1 in test");
        client.set_err("127.0.0.54:53", "timeout 2 in test");

        let resolver = MiekgResolver::new_with_client(cfg_path, period, client);
        let err = resolver
            .lookup_srv("cache", "tcp", "example.com")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("timeout"), "got: {err}");
        resolver.stop();
    }

    // one timeout and one success
    {
        let cfg_path = write_resolv_conf(tmp.path(), RESOLV_CONF_CONTENTS);
        let client = Arc::new(MockDnsClient::new());
        client.set_err("127.0.0.53:53", "timeout 1 in test");
        client.push_res(
            "127.0.0.54:53",
            vec![new_srv_dns_response(
                "_cache._tcp.example.com.",
                "cache01.example.com.",
            )],
        );

        let resolver = MiekgResolver::new_with_client(cfg_path, period, client);
        let (_, res) = resolver
            .lookup_srv("cache", "tcp", "example.com")
            .await
            .unwrap();
        assert_eq!(
            vec![Srv {
                target: "cache01.example.com.".to_string(),
                port: 11211,
                priority: 10,
                weight: 100,
            }],
            res
        );
        resolver.stop();
    }

    // name error
    {
        let mut response = Message::new();
        response.set_message_type(MessageType::Response);
        response.set_response_code(ResponseCode::NXDomain);
        response.add_query(hickory_resolver::proto::op::Query::query(
            fqdn("_cache._tcp.example.com."),
            RecordType::SRV,
        ));

        let cfg_path = write_resolv_conf(tmp.path(), RESOLV_CONF_CONTENTS);
        let client = Arc::new(MockDnsClient::new());
        client.set_err("127.0.0.53:53", "timeout 1 in test");
        client.push_res("127.0.0.54:53", vec![response]);

        let resolver = MiekgResolver::new_with_client(cfg_path, period, client);
        let (_, res) = resolver
            .lookup_srv("cache", "tcp", "example.com")
            .await
            .unwrap();
        assert!(res.is_empty());
        resolver.stop();
    }

    // non-SRV answer
    {
        let mut response =
            new_srv_dns_response("_cache._tcp.example.com.", "cache01.example.com.");
        response.add_answer(Record::from_rdata(
            fqdn("cache01.example.com."),
            30,
            RData::A(A(Ipv4Addr::new(127, 0, 0, 1))),
        ));

        let cfg_path = write_resolv_conf(tmp.path(), RESOLV_CONF_CONTENTS);
        let client = Arc::new(MockDnsClient::new());
        client.push_res("127.0.0.53:53", vec![response]);

        let resolver = MiekgResolver::new_with_client(cfg_path, period, client);
        let (_, res) = resolver
            .lookup_srv("cache", "tcp", "example.com")
            .await
            .unwrap();
        assert_eq!(
            vec![Srv {
                target: "cache01.example.com.".to_string(),
                port: 11211,
                priority: 10,
                weight: 100,
            }],
            res
        );
        resolver.stop();
    }

    // truncated
    {
        let mut response =
            new_srv_dns_response("_cache._tcp.example.com.", "cache01.example.com.");
        response.set_truncated(true);

        let cfg_path = write_resolv_conf(tmp.path(), RESOLV_CONF_CONTENTS);
        let client = Arc::new(MockDnsClient::new());
        client.push_res("127.0.0.53:53", vec![response.clone(), response]);
        client.set_err("127.0.0.54:53", "timeout 2 in test");

        let resolver = MiekgResolver::new_with_client(cfg_path, period, client);
        let err = resolver
            .lookup_srv("cache", "tcp", "example.com")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("truncated"), "got: {err}");
        resolver.stop();
    }

    // resolv.conf changes
    {
        let client = Arc::new(MockDnsClient::new());
        client.push_res(
            "127.0.0.11:53",
            vec![new_srv_dns_response(
                "_cache._tcp.example.com.",
                "cache01.example.com.",
            )],
        );
        client.push_res(
            "127.0.0.22:53",
            vec![new_srv_dns_response(
                "_cache._tcp.example.com.",
                "cache02.example.com.",
            )],
        );

        let cfg_path = write_resolv_conf(tmp.path(), "nameserver 127.0.0.11\n");
        let resolver = MiekgResolver::new_with_client(cfg_path, period, client);

        let (_, res) = resolver
            .lookup_srv("cache", "tcp", "example.com")
            .await
            .unwrap();
        assert_eq!(
            vec![Srv {
                target: "cache01.example.com.".to_string(),
                port: 11211,
                priority: 10,
                weight: 100,
            }],
            res
        );

        let _ = write_resolv_conf(tmp.path(), "nameserver 127.0.0.22\n");
        resolver.load_config().unwrap();

        let (_, res) = resolver
            .lookup_srv("cache", "tcp", "example.com")
            .await
            .unwrap();
        assert_eq!(
            vec![Srv {
                target: "cache02.example.com.".to_string(),
                port: 11211,
                priority: 10,
                weight: 100,
            }],
            res
        );
        resolver.stop();
    }
}

// Upstream: github.com/grafana/dskit/dns/miekgdns/resolver_test.go::TestResolver_LookupIP
#[tokio::test]
async fn test_resolver_lookup_ip() {
    let period = Duration::from_secs(3600);
    let tmp = TempDir::new().unwrap();

    // multiple timeouts
    {
        let cfg_path = write_resolv_conf(tmp.path(), RESOLV_CONF_CONTENTS);
        let client = Arc::new(MockDnsClient::new());
        client.set_err("127.0.0.53:53", "timeout 1 in test");
        client.set_err("127.0.0.54:53", "timeout 2 in test");

        let resolver = MiekgResolver::new_with_client(cfg_path, period, client);
        let err = resolver
            .lookup_ip_addr("cache01.example.com.")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("timeout"), "got: {err}");
        resolver.stop();
    }

    // one timeout and one success
    {
        let cfg_path = write_resolv_conf(tmp.path(), RESOLV_CONF_CONTENTS);
        let client = Arc::new(MockDnsClient::new());
        client.set_err("127.0.0.53:53", "timeout 1 in test");
        client.push_res(
            "127.0.0.54:53",
            vec![new_ip_dns_response(
                "cache01.example.com.",
                Ipv4Addr::new(10, 0, 0, 1),
            )],
        );

        let resolver = MiekgResolver::new_with_client(cfg_path, period, client);
        let res = resolver.lookup_ip_addr("cache01.example.com").await.unwrap();
        assert_eq!(vec![IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))], res);
        resolver.stop();
    }

    // name error
    {
        let mut response1 = Message::new();
        response1.set_message_type(MessageType::Response);
        response1.set_response_code(ResponseCode::NXDomain);
        response1.add_query(hickory_resolver::proto::op::Query::query(
            fqdn("cache01.example.com."),
            RecordType::AAAA,
        ));

        let mut response2 = Message::new();
        response2.set_message_type(MessageType::Response);
        response2.set_response_code(ResponseCode::NXDomain);
        response2.add_query(hickory_resolver::proto::op::Query::query(
            fqdn("cache01.example.com."),
            RecordType::A,
        ));

        let cfg_path = write_resolv_conf(tmp.path(), RESOLV_CONF_CONTENTS);
        let client = Arc::new(MockDnsClient::new());
        client.set_err("127.0.0.53:53", "timeout 1 in test");
        client.push_res("127.0.0.54:53", vec![response1, response2]);

        let resolver = MiekgResolver::new_with_client(cfg_path, period, client);
        let res = resolver.lookup_ip_addr("cache01.example.com").await.unwrap();
        assert!(res.is_empty());
        resolver.stop();
    }

    // one level of CNAME
    {
        let cfg_path = write_resolv_conf(tmp.path(), RESOLV_CONF_CONTENTS);
        let client = Arc::new(MockDnsClient::new());
        client.set_err("127.0.0.53:53", "timeout 1 in test");
        client.push_res(
            "127.0.0.54:53",
            vec![
                new_cname_dns_response("cache01.example.com.", "cache01.east.example.com."),
                new_ip_dns_response("cache01.east.example.com.", Ipv4Addr::new(10, 0, 0, 1)),
            ],
        );

        let resolver = MiekgResolver::new_with_client(cfg_path, period, client);
        let res = resolver.lookup_ip_addr("cache01.example.com").await.unwrap();
        assert_eq!(vec![IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))], res);
        resolver.stop();
    }
}

// Upstream: github.com/grafana/dskit/dns/miekgdns/resolver_test.go::TestPoolingClient
#[tokio::test]
async fn test_pooling_client() {
    if Path::new(DEFAULT_RESOLV_CONF_PATH).exists() {
        let conf = ClientConfig::from_file(DEFAULT_RESOLV_CONF_PATH).unwrap();
        assert!(!conf.servers.is_empty());
    }
    let (server, srv_handle) = spawn_local_tcp_dns_server().await;
    let msg = new_dns_question("localhost", RecordType::A);
    let client = PoolingClient::new(DEFAULT_MAX_CONNS_PER_HOST);

    let (resp, _) = client.exchange(&msg, &server).await.unwrap();
    assert_eq!(ResponseCode::NoError, resp.response_code());
    assert_eq!(1, client.pool_count());

    client.clean(&[]);
    assert_eq!(0, client.pool_count());

    srv_handle.abort();
}

// Upstream: github.com/grafana/dskit/dns/miekgdns/resolver_test.go::TestPool
#[tokio::test]
async fn test_pool() {
    let (server, srv_handle) = spawn_local_tcp_dns_server().await;

    for (name, num_conns) in [("pooling disabled", 0usize), ("pooling enabled", 1usize)] {
        let pool = Pool::new(num_conns);

        // get connection
        let conn = pool
            .get("tcp", &server)
            .await
            .unwrap_or_else(|e| panic!("{name}: get connection failed: {e}"));

        // put connection
        pool.put(conn)
            .unwrap_or_else(|e| panic!("{name}: put connection failed: {e}"));
        assert_eq!(
            num_conns,
            pool.idle_len(),
            "{name}: unexpected idle connection count"
        );

        // close
        pool.close();
        assert!(
            pool.get("tcp", &server).await.is_err(),
            "{name}: expected error after close"
        );
    }

    srv_handle.abort();
}
