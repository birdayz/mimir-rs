#[path = "upstream_storage_part2_helpers.rs"]
mod helpers;

use helpers::{create_test_kafka_config, ClusterSasl, FakeKafkaCluster};
use mimir_config::types::SASLMechanism;
use mimir_storage::ingest::new_kafka_reader_client;

// Upstream: pkg/storage/ingest/reader_client_test.go::TestNewKafkaReaderClient
#[tokio::test]
async fn test_new_kafka_reader_client() {
    const TOPIC: &str = "test";
    const USERNAME: &str = "mimir";
    const PASSWORD: &str = "supersecret";

    // "should support SASL plain authentication"
    {
        let cluster = FakeKafkaCluster::start_with_opts(
            1,
            TOPIC,
            ClusterSasl::Plain {
                username: USERNAME.to_string(),
                password: PASSWORD.to_string(),
            },
            true,
        )
        .await;

        // wrong auth fails
        let mut bad_cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        bad_cfg.sasl_mechanism = SASLMechanism::Plain;
        bad_cfg.sasl_username = USERNAME.to_string();
        bad_cfg.sasl_password = "wrong".to_string();
        let bad_client = new_kafka_reader_client(&bad_cfg, None).unwrap();
        assert!(bad_client.ping().await.is_err());
        bad_client.close().await;

        // good auth succeeds
        let mut good_cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        good_cfg.sasl_mechanism = SASLMechanism::Plain;
        good_cfg.sasl_username = USERNAME.to_string();
        good_cfg.sasl_password = PASSWORD.to_string();
        let good_client = new_kafka_reader_client(&good_cfg, None).unwrap();
        good_client.ping().await.unwrap();
        good_client.close().await;

        cluster.close();
    }

    // "should support SASL SCRAM-SHA-256 authentication"
    {
        let cluster = FakeKafkaCluster::start_with_opts(
            1,
            TOPIC,
            ClusterSasl::ScramSha256 {
                username: USERNAME.to_string(),
                password: PASSWORD.to_string(),
            },
            true,
        )
        .await;

        // wrong auth fails
        let mut bad_cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        bad_cfg.sasl_mechanism = SASLMechanism::ScramSha256;
        bad_cfg.sasl_username = USERNAME.to_string();
        bad_cfg.sasl_password = "wrong".to_string();
        let bad_client = new_kafka_reader_client(&bad_cfg, None).unwrap();
        assert!(bad_client.ping().await.is_err());
        bad_client.close().await;

        // good auth succeeds
        let mut good_cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        good_cfg.sasl_mechanism = SASLMechanism::ScramSha256;
        good_cfg.sasl_username = USERNAME.to_string();
        good_cfg.sasl_password = PASSWORD.to_string();
        let good_client = new_kafka_reader_client(&good_cfg, None).unwrap();
        good_client.ping().await.unwrap();
        good_client.close().await;

        cluster.close();
    }

    // "should support SASL SCRAM-SHA-512 authentication"
    {
        let cluster = FakeKafkaCluster::start_with_opts(
            1,
            TOPIC,
            ClusterSasl::ScramSha512 {
                username: USERNAME.to_string(),
                password: PASSWORD.to_string(),
            },
            true,
        )
        .await;

        // wrong auth fails
        let mut bad_cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        bad_cfg.sasl_mechanism = SASLMechanism::ScramSha512;
        bad_cfg.sasl_username = USERNAME.to_string();
        bad_cfg.sasl_password = "wrong".to_string();
        let bad_client = new_kafka_reader_client(&bad_cfg, None).unwrap();
        assert!(bad_client.ping().await.is_err());
        bad_client.close().await;

        // good auth succeeds
        let mut good_cfg = create_test_kafka_config(&cluster.addr, TOPIC);
        good_cfg.sasl_mechanism = SASLMechanism::ScramSha512;
        good_cfg.sasl_username = USERNAME.to_string();
        good_cfg.sasl_password = PASSWORD.to_string();
        let good_client = new_kafka_reader_client(&good_cfg, None).unwrap();
        good_client.ping().await.unwrap();
        good_client.close().await;

        cluster.close();
    }
}
