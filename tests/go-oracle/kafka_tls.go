// SPDX-License-Identifier: AGPL-3.0-only
// Kafka TLS / SASL helpers for the testcontainers (Redpanda) conformance suite (OBSERV-1374).
//
// `kafka-produce` deliberately builds its client through upstream Mimir's own
// `ingest.KafkaConfig` (YAML-decoded with KnownFields, flag defaults, Validate) and
// `ingest.NewKafkaWriterClient`, so the Go side of every scenario exercises exactly the
// TLS / SASL code path a Go Mimir distributor uses in production.
package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.yaml.in/yaml/v3"
)

// --- gen-test-pki ---

type genTestPKIIn struct {
	Dir string `json:"dir"`
}

type genTestPKIOut struct {
	CACert     string `json:"ca_cert"`
	ServerCert string `json:"server_cert"`
	ServerKey  string `json:"server_key"`
	SNICert    string `json:"sni_cert"`
	SNIKey     string `json:"sni_key"`
	SNIName    string `json:"sni_name"`
	ClientCert string `json:"client_cert"`
	ClientKey  string `json:"client_key"`
}

const testPKISNIName = "kafka.mimir-rs.test"

func writePEM(path, typ string, der []byte) error {
	return os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o644)
}

func issueLeaf(dir, name string, ca *x509.Certificate, caKey *rsa.PrivateKey, serial int64, tmpl x509.Certificate) (string, string, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", "", err
	}
	tmpl.SerialNumber = big.NewInt(serial)
	tmpl.NotBefore = time.Now().Add(-time.Hour)
	tmpl.NotAfter = time.Now().Add(48 * time.Hour)
	tmpl.KeyUsage = x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		return "", "", err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return "", "", err
	}
	certPath := filepath.Join(dir, name+".crt")
	keyPath := filepath.Join(dir, name+".key")
	if err := writePEM(certPath, "CERTIFICATE", der); err != nil {
		return "", "", err
	}
	if err := writePEM(keyPath, "PRIVATE KEY", keyDER); err != nil {
		return "", "", err
	}
	return certPath, keyPath, nil
}

// cmdGenTestPKI writes a throwaway CA plus server (localhost/127.0.0.1), SNI-only
// (testPKISNIName, no IP SAN) and client (mTLS) certificates.
func cmdGenTestPKI(input []byte) (*genTestPKIOut, error) {
	var in genTestPKIIn
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, fmt.Errorf("decode input: %w", err)
	}
	if in.Dir == "" {
		return nil, fmt.Errorf("dir is required")
	}
	if err := os.MkdirAll(in.Dir, 0o755); err != nil {
		return nil, err
	}

	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "mimir-rs conformance test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(48 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, err
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, err
	}
	caPath := filepath.Join(in.Dir, "ca.crt")
	if err := writePEM(caPath, "CERTIFICATE", caDER); err != nil {
		return nil, err
	}

	out := &genTestPKIOut{CACert: caPath, SNIName: testPKISNIName}
	out.ServerCert, out.ServerKey, err = issueLeaf(in.Dir, "server", ca, caKey, 2, x509.Certificate{
		Subject:     pkix.Name{CommonName: "localhost"},
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	if err != nil {
		return nil, fmt.Errorf("server cert: %w", err)
	}
	out.SNICert, out.SNIKey, err = issueLeaf(in.Dir, "sni", ca, caKey, 3, x509.Certificate{
		Subject:     pkix.Name{CommonName: testPKISNIName},
		DNSNames:    []string{testPKISNIName},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	if err != nil {
		return nil, fmt.Errorf("sni cert: %w", err)
	}
	out.ClientCert, out.ClientKey, err = issueLeaf(in.Dir, "client", ca, caKey, 4, x509.Certificate{
		Subject:     pkix.Name{CommonName: "mimir-ingester"},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})
	if err != nil {
		return nil, fmt.Errorf("client cert: %w", err)
	}
	return out, nil
}

// --- kafka-produce ---

type kafkaProduceIn struct {
	// KafkaYAML is the body of an `ingest_storage.kafka:` block (upstream YAML keys).
	KafkaYAML string          `json:"kafka_yaml"`
	Records   []kfakeRecordIn `json:"records"`
	// ConsumerGroups to fetch committed offsets for (partition 0 of the configured topic).
	ConsumerGroups []string `json:"consumer_groups,omitempty"`
}

type kafkaProduceOut struct {
	Topic      string           `json:"topic"`
	EndOffsets map[int32]int64  `json:"end_offsets"`
	Committed  map[string]int64 `json:"committed"`
}

func loadKafkaConfig(kafkaYAML string) (ingest.KafkaConfig, error) {
	var cfg ingest.KafkaConfig
	fs := flag.NewFlagSet("go-oracle-kafka", flag.ContinueOnError)
	cfg.RegisterFlagsWithPrefix("ingest-storage.kafka.", fs)
	dec := yaml.NewDecoder(strings.NewReader(kafkaYAML))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return cfg, fmt.Errorf("decode kafka YAML: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return cfg, fmt.Errorf("validate kafka config: %w", err)
	}
	return cfg, nil
}

func cmdKafkaProduce(input []byte) (*kafkaProduceOut, error) {
	var in kafkaProduceIn
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, fmt.Errorf("decode input: %w", err)
	}
	cfg, err := loadKafkaConfig(in.KafkaYAML)
	if err != nil {
		return nil, err
	}

	cl, err := ingest.NewKafkaWriterClient(cfg, 20, log.NewNopLogger(), prometheus.NewRegistry())
	if err != nil {
		return nil, fmt.Errorf("NewKafkaWriterClient: %w", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var recs []*kgo.Record
	for i, recIn := range in.Records {
		if err := ingest.ValidateRecordVersion(recIn.Version); err != nil {
			return nil, fmt.Errorf("record %d: %w", i, err)
		}
		wr, err := buildWriteRequest(recIn.WriteRequest)
		if err != nil {
			return nil, fmt.Errorf("record %d: %w", i, err)
		}
		rs, _, err := ingest.RecordSerializerFromVersion(recIn.Version).ToRecords(cfg.Topic, recIn.Partition, recIn.Tenant, wr, cfg.ProducerMaxRecordSizeBytes)
		if err != nil {
			return nil, fmt.Errorf("record %d serialize: %w", i, err)
		}
		recs = append(recs, rs...)
	}
	if len(recs) > 0 {
		if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
			return nil, fmt.Errorf("produce: %w", err)
		}
	}

	adm := kadm.NewClient(cl)
	ends, err := adm.ListEndOffsets(ctx, cfg.Topic)
	if err != nil {
		return nil, fmt.Errorf("ListEndOffsets: %w", err)
	}
	out := &kafkaProduceOut{Topic: cfg.Topic, EndOffsets: map[int32]int64{}, Committed: map[string]int64{}}
	var endErr error
	ends.Each(func(o kadm.ListedOffset) {
		if o.Err != nil {
			endErr = o.Err
			return
		}
		out.EndOffsets[o.Partition] = o.Offset
	})
	if endErr != nil {
		return nil, fmt.Errorf("ListEndOffsets: %w", endErr)
	}

	for _, group := range in.ConsumerGroups {
		resp, err := adm.FetchOffsets(ctx, group)
		if err != nil {
			return nil, fmt.Errorf("FetchOffsets(%s): %w", group, err)
		}
		if err := resp.Error(); err != nil {
			return nil, fmt.Errorf("FetchOffsets(%s): %w", group, err)
		}
		off := int64(-1)
		if r, ok := resp.Lookup(cfg.Topic, 0); ok {
			if r.Err != nil {
				return nil, fmt.Errorf("FetchOffsets(%s) partition 0: %w", group, r.Err)
			}
			off = r.At
		}
		out.Committed[group] = off
	}
	return out, nil
}
