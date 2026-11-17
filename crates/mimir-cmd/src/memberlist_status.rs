//! Mimir's memberlist status page template.
//!
//! Ported from:
//! - github.com/grafana/mimir/pkg/api/handlers.go (`memberlistStatusHandler`: template functions
//!   `StringsJoin`, `GetZoneFromMeta`, `GetRoleFromMeta`, `FormatBytes` = `humanize.IBytes`)
//! - github.com/grafana/mimir/pkg/api/memberlist_status.gohtml (rendered byte-for-byte, including
//!   the whitespace `html/template` leaves around actions)
//!
//! The request handling itself (`?viewKey=`, `?downloadKey=`, `?viewMsg=`, `?deleteMessages=`,
//! `Accept: application/json`) is dskit's and lives in
//! [`mimir_ring::kv::memberlist::http_status`].

use mimir_ring::kv::memberlist::gofmt::{format_date_time_millis, format_time_millis, humanize_ibytes};
use mimir_ring::kv::memberlist::http_status::{
    html_escape, node_address, role_from_meta, url_query_escape, zone_from_meta,
    StatusMessage, StatusPageData,
};
use mimir_ring::kv::memberlist::wire::NodeStateType;

/// `FormatBytes` template function.
fn format_bytes(n: usize) -> String {
    humanize_ibytes(n as u64)
}

/// Renders `pkg/api/memberlist_status.gohtml` (a [`mimir_ring::kv::memberlist::http_status::PageTemplate`]).
pub fn render_memberlist_status_page(d: &StatusPageData) -> String {
    let mut out = String::with_capacity(16 * 1024);
    out.push_str(
        r##"<!DOCTYPE html>
<html class="h-100">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1">

    <title>Memberlist: Grafana Mimir</title>

    <link rel="stylesheet" href="static/bootstrap-5.1.3.min.css">
    <link rel="stylesheet" href="static/bootstrap-icons-1.8.1.css">
    <link rel="stylesheet" href="static/mimir-styles.css">
    <script src="static/bootstrap-5.1.3.bundle.min.js"></script>
</head>
<body class="d-flex flex-column h-100">
<main class="flex-shrink-0">
    <div class="container">
        <div class="header row border-bottom py-3 flex-column-reverse flex-sm-row">
            <div class="col-12 col-sm-9 text-center text-sm-start">
                <h1>Memberlist: Grafana Mimir</h1>
            </div>
            <div class="col-12 col-sm-3 text-center text-sm-end mb-3 mb-sm-0">
                <img alt="Mimir logo" class="mimir-brand" src="static/mimir-icon.png">
            </div>
        </div>
        <div class="row my-3">
            <div class="col-12">
                
                "##,
    );
    if d.health_score == 0 {
        out.push_str(
            r##"
                    <div class="alert alert-success" role="alert">
                        Memberlist cluster has <strong>"##,
        );
        out.push_str(&d.num_members.to_string());
        out.push_str(
            r##"</strong> members and it is <strong>healthy</strong>.
                    </div>
                "##,
        );
    } else {
        out.push_str(
            r##"
                    <div class="alert alert-warning" role="alert">
                        Memberlist cluster has <strong>"##,
        );
        out.push_str(&d.num_members.to_string());
        out.push_str(
            r##"</strong> members but health score
                        is "##,
        );
        out.push_str(&d.health_score.to_string());
        out.push_str(
            r##" (lower is better, 0 = healthy).
                    </div>
                "##,
        );
    }
    out.push_str(
        r##"
            </div>

            <h2>KV Store</h2>
            <div class="table-responsive">
                <table class="table table-bordered table-hover table-striped">
                    <thead>
                    <tr>
                        <th>Key</th>
                        <th class="fit-width">Codec</th>
                        <th class="fit-width">Size</th>
                        <th class="fit-width">
                            <span class="text-nowrap">
                                Version
                                <i class="bi bi-info-circle"
                                   data-bs-toggle="tooltip" data-bs-placement="top"
                                   title="Note that value 'version' is node-specific. It starts with 0 (on restart), and increases on each received update."></i>
                            </span>
                        </th>
                        <th class="fit-width">Actions</th>
                    </tr>
                    </thead>

                    <tbody>
                    "##,
    );
    for (k, v) in &d.store {
        let k_url = url_query_escape(k);
        out.push_str(
            r##"
                        <tr>
                            <td class="align-middle font-monospace small">"##,
        );
        out.push_str(&html_escape(k));
        out.push_str(
            r##"</td>
                            <td class="align-middle font-monospace small fit-width">"##,
        );
        out.push_str(&html_escape(&v.codec_id));
        out.push_str(
            r##"</td>
                            <td class="align-middle font-monospace small fit-width text-nowrap">"##,
        );
        out.push_str(&html_escape(&format_bytes(d.store_sizes.get(k).copied().unwrap_or(0))));
        out.push_str(
            r##"</td>
                            <td class="align-middle font-monospace small fit-width">"##,
        );
        out.push_str(&v.version.to_string());
        out.push_str(
            r##"</td>
                            <td class="fit-width">
                                <span class="text-nowrap">
                                    <a href="?viewKey="##,
        );
        out.push_str(&k_url);
        out.push_str(
            r##"&format=json-pretty" title="JSON pretty" class="text-decoration-none">
                                        <i class="bi bi-filetype-json text-success"></i>
                                    </a>
                                    <a href="?viewKey="##,
        );
        out.push_str(&k_url);
        out.push_str(
            r##"&format=json" title="JSON" class="text-decoration-none">
                                        <i class="bi bi-filetype-json"></i>
                                    </a>
                                    <a href="?viewKey="##,
        );
        out.push_str(&k_url);
        out.push_str(
            r##"&format=struct" title="Struct" class="text-decoration-none">
                                        <i class="bi bi-file-earmark-code"></i>
                                    </a>
                                    <a href="?downloadKey="##,
        );
        out.push_str(&k_url);
        out.push_str(
            r##"" title="Download" class="text-decoration-none">
                                        <i class="bi bi-file-earmark-arrow-down"></i>
                                    </a>
                                </span>
                            </td>
                        </tr>
                    "##,
        );
    }
    out.push_str(
        r##"
                    </tbody>
                    <tfoot>
                    <tr>
                        <td colspan="2"></td>
                        <td class="align-middle font-monospace small fit-width text-nowrap">"##,
    );
    out.push_str(&html_escape(&format_bytes(d.total_store_size)));
    out.push_str(
        r##"</td>
                        <td colspan="2"></td>
                    </tr>
                    </tfoot>
                </table>
            </div>

            <h2>Memberlist Cluster Members</h2>
            <div class="table-responsive">
                <table class="table table-bordered table-hover table-striped">
                    <thead>
                    <tr>
                        <th>Name</th>
                        <th>Address</th>
                        <th class="fit-width">
                            <span class="text-nowrap">
                                State
                                <i class="bi bi-info-circle"
                                   data-bs-toggle="tooltip" data-bs-placement="left"
                                   title="State: 0 = Alive, 1 = Suspect, 2 = Dead, 3 = Left"></i>
                            </span>
                        </th>
                        "##,
    );
    if d.zone_aware_routing.enabled {
        out.push_str(
            r##"
                            <th>Zone</th>
                            <th>Role</th>
                        "##,
        );
    }
    out.push_str(
        r##"
                    </tr>
                    </thead>

                    <tbody>
                    "##,
    );
    for m in &d.sorted_members {
        out.push_str(
            r##"
                        <tr>
                            <td class="align-middle font-monospace small">"##,
        );
        out.push_str(&html_escape(&m.name));
        out.push_str(
            r##"</td>
                            <td class="align-middle font-monospace small">"##,
        );
        out.push_str(&html_escape(&node_address(m)));
        out.push_str(
            r##"</td>
                            <td class="fit-width text-center py-1">
                                "##,
        );
        match m.state {
            NodeStateType::Alive => out.push_str(
                r##"
                                    <span class="badge bg-success">Alive</span>
                                "##,
            ),
            NodeStateType::Suspect => out.push_str(
                r##"
                                    <span class="badge bg-warning text-dark">Suspect</span>
                                "##,
            ),
            NodeStateType::Dead => out.push_str(
                r##"
                                    <span class="badge bg-danger">Dead</span>
                                "##,
            ),
            NodeStateType::Left => {
                // Upstream bug: `{{ else if eq.State 3}}` calls `eq` without arguments, so Go's
                // template execution fails here. HTTPStatusHandler has already streamed the page so
                // far (status 200) and `http.Error` appends the error text. Unreachable in practice:
                // `Memberlist.Members()` never returns dead or left nodes.
                out.push_str(
                    "template: memberlist_status:132:43: executing \"memberlist_status\" at <eq>: \
                     wrong number of args for eq: want at least 1 got 0\n",
                );
                return out;
            }
        }
        // `{{ else }} Unknown: {{ .State }}` is unreachable: NodeStateType only has 0..=3.
        out.push_str(
            r##"
                            </td>
                            "##,
        );
        if d.zone_aware_routing.enabled {
            out.push_str(
                r##"
                                <td class="align-middle font-monospace small">"##,
            );
            out.push_str(&html_escape(&zone_from_meta(m.meta.as_deref())));
            out.push_str(
                r##"</td>
                                <td class="align-middle font-monospace small">"##,
            );
            out.push_str(&html_escape(&role_from_meta(m.meta.as_deref())));
            out.push_str(
                r##"</td>
                            "##,
            );
        }
        out.push_str(
            r##"
                        </tr>
                    "##,
        );
    }
    out.push_str(
        r##"
                    </tbody>
                </table>
            </div>

            <h2>Message History
                "##,
    );
    if d.message_history_buffer_bytes != 0 {
        out.push_str(
            r##"
                    <a class="btn btn-outline-warning" href="?deleteMessages=true" data-bs-toggle="tooltip"
                       data-bs-placement="right" title="Delete sent and received messages buffer">Flush</a>
                "##,
        );
    }
    out.push_str(
        r##"
            </h2>

            "##,
    );
    if d.message_history_buffer_bytes != 0 {
        out.push_str(
            r##"
                <div class="accordion">
                    <div class="accordion-item">
                        <h3 class="accordion-header" id="heading-received-messages">
                            <button class="accordion-button collapsed" type="button" data-bs-toggle="collapse"
                                    data-bs-target="#collapse-received-messages" aria-expanded="false"
                                    aria-controls="collapse-received-messages">
                                Received Messages
                            </button>
                        </h3>
                        <div id="collapse-received-messages" class="accordion-collapse collapse"
                             aria-labelledby="heading-received-messages">
                            <div class="accordion-body p-0 table-responsive">
                                "##,
        );
        if d.received_messages.is_empty() {
            out.push_str(
                r##"
                                    <span><i>There are no received messages.</i></span>
                                "##,
            );
        } else {
            out.push_str(
                r##"
                                    <table class="table table-hover table-striped">
                                        <thead>
                                        <tr>
                                            <th class="fit-width">ID</th>
                                            <th>Time</th>
                                            <th>Key</th>
                                            <th class="fit-width">Size (B)</th>
                                            <th class="fit-width">Codec</th>
                                            <th class="fit-width">
                                                <span class="text-nowrap">
                                                    Version
                                                    <i class="bi bi-info-circle"
                                                       data-bs-toggle="tooltip" data-bs-placement="top"
                                                       title="Version after update. 0 = No change."></i>
                                                </span>
                                            </th>
                                            <th>Changes</th>
                                            <th class="fit-width">Actions</th>
                                        </tr>
                                        </thead>

                                        <tbody class="font-monospace small">
                                        "##,
            );
            for m in &d.received_messages {
                message_row(&mut out, m, " fit-width", r##"<td class="fit-width">"##);
            }
            out.push_str(
                r##"
                                        </tbody>

                                    </table>
                                "##,
            );
        }
        out.push_str(
            r##"
                            </div>
                        </div>
                    </div>
                    <div class="accordion-item">
                        <h3 class="accordion-header" id="heading-sent-messages">
                            <button class="accordion-button collapsed" type="button" data-bs-toggle="collapse"
                                    data-bs-target="#collapse-sent-messages" aria-expanded="false"
                                    aria-controls="collapse-sent-messages">
                                Sent Messages
                            </button>
                        </h3>
                        <div id="collapse-sent-messages" class="accordion-collapse collapse"
                             aria-labelledby="heading-sent-messages">
                            <div class="accordion-body p-0 table-responsive">

                                "##,
        );
        if d.sent_messages.is_empty() {
            out.push_str(
                r##"
                                    <span><i>There are no sent messages.</i></span>
                                "##,
            );
        } else {
            out.push_str(
                r##"
                                    <table class="table table-hover table-striped">
                                        <thead>
                                        <tr>
                                            <th>ID</th>
                                            <th>Time</th>
                                            <th>Key</th>
                                            <th>Size</th>
                                            <th>Codec</th>
                                            <th>Version</th>
                                            <th>Changes</th>
                                            <th>Actions</th>
                                        </tr>
                                        </thead>

                                        <tbody>
                                        "##,
            );
            for m in &d.sent_messages {
                message_row(&mut out, m, "", "<td>");
            }
            out.push_str(
                r##"
                                        </tbody>
                                    </table>

                                "##,
            );
        }
        out.push_str(
            r##"
                            </div>
                        </div>
                    </div>
                </div>
            "##,
        );
    } else {
        out.push_str(
            r##"

                <div class="col-12">
                    <div class="alert alert-info" role="alert">
                        Message history buffer is disabled.
                        <br />Enable it by setting the <code>-memberlist.message-history-buffer-bytes</code> flag or the corresponding config key.
                    </div>
                </div>
            "##,
        );
    }
    out.push_str(
        r##"
        </div>
    </div>
</main>
<footer class="footer mt-auto py-3 bg-light">
    <div class="container">
        <small class="text-muted">Status @ "##,
    );
    out.push_str(&format_date_time_millis(&d.now));
    out.push_str(
        r##"</small>
    </div>
</footer>
<script type="text/javascript">
    var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'))
    var tooltipList = tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl)
    })
</script>
</body>
</html>"##,
    );
    out
}

/// One row of the received (`fit = " fit-width"`, actions cell `<td class="fit-width">`) or sent
/// (`fit = ""`, actions cell `<td>`) message table.
fn message_row(out: &mut String, m: &StatusMessage, fit: &str, actions_td: &str) {
    let id = m.id.to_string();
    out.push_str(
        r##"
                                            <tr>
                                                <td class="font-monospace small"##,
    );
    out.push_str(fit);
    out.push_str(r#"">"#);
    out.push_str(&id);
    out.push_str(
        r##"</td>
                                                <td class="font-monospace small">"##,
    );
    out.push_str(&format_time_millis(&m.time));
    out.push_str(
        r##"</td>
                                                <td class="font-monospace small">"##,
    );
    out.push_str(&html_escape(&m.pair.key));
    out.push_str(
        r##"</td>
                                                <td class="font-monospace small"##,
    );
    out.push_str(fit);
    out.push_str(r#"">"#);
    out.push_str(&m.pair.value.len().to_string());
    out.push_str(
        r##"</td>
                                                <td class="font-monospace small"##,
    );
    out.push_str(fit);
    out.push_str(r#"">"#);
    out.push_str(&html_escape(&m.pair.codec));
    out.push_str(
        r##"</td>
                                                <td class="font-monospace small"##,
    );
    out.push_str(fit);
    out.push_str(r#"">"#);
    out.push_str(&m.version.to_string());
    out.push_str(
        r##"</td>
                                                <td class="font-monospace small">"##,
    );
    out.push_str(&html_escape(&m.changes.join(", ")));
    out.push_str(
        r##"</td>
                                                "##,
    );
    out.push_str(actions_td);
    out.push_str(
        r##"
                                                    <span class="text-nowrap">
                                                        <a href="?viewMsg="##,
    );
    out.push_str(&id);
    out.push_str(
        r##"&format=json-pretty" class="text-decoration-none">
                                                            <i class="bi bi-filetype-json text-success"></i>
                                                        </a>
                                                        <a href="?viewMsg="##,
    );
    out.push_str(&id);
    out.push_str(
        r##"&format=json" class="text-decoration-none">
                                                            <i class="bi bi-filetype-json"></i>
                                                        </a>
                                                        <a href="?viewMsg="##,
    );
    out.push_str(&id);
    out.push_str(
        r##"&format=struct" class="text-decoration-none">
                                                            <i class="bi bi-file-earmark-code"></i>
                                                        </a>
                                                    </span>
                                                </td>
                                            </tr>
                                        "##,
    );
}
