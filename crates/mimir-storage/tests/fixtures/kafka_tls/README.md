Test-only PKI for `tests/kafka_transport_tls_sasl.rs` and `mimir_common::tls` unit tests
(OBSERV-1374). Never use outside tests. Valid for 100 years; generated with `openssl`:

- `ca.crt`: test CA (key discarded). `other-ca.crt`: unrelated CA (untrusted-CA cases).
- `server.crt/.key`: `localhost` + IP SAN `127.0.0.1` (serverAuth), PKCS#8 RSA key.
- `sni.crt/.key`: only `kafka.mimir-rs.test` (needs `tls_server_name`).
- `client.crt` + `client.key` (PKCS#8 RSA) / `client.pkcs1.key` (PKCS#1 RSA): clientAuth.
- `client-ec.crt` + `client-ec.key` (PKCS#8 P-256) / `client-ec.sec1.key` (SEC1 P-256).
