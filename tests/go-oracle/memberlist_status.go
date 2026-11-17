// SPDX-License-Identifier: AGPL-3.0-only
// Oracle for the memberlist status page (`GET /memberlist`):
// - dskit kv/memberlist/http_status_handler.go: `json.NewEncoder(w).Encode(StatusPageData)` and
//   `formatValue` (json / json-pretty / %#v of a decoded KV value)
// - Mimir pkg/api/memberlist_status.gohtml (+ the template funcs of pkg/api/handlers.go) and
//   dskit kv/memberlist/status.gohtml rendered with html/template.
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html/template"
	"os"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/ring"
	hmemberlist "github.com/hashicorp/memberlist"

	"github.com/grafana/mimir/pkg/distributor"
)

type mlTime struct {
	UnixNano   int64 `json:"unix_nano"`
	OffsetSecs int   `json:"offset_secs"`
}

func (t *mlTime) toTime() time.Time {
	if t == nil {
		return time.Time{}
	}
	loc := time.UTC
	if t.OffsetSecs != 0 {
		loc = time.FixedZone("", t.OffsetSecs)
	}
	return time.Unix(0, t.UnixNano).In(loc)
}

type mlMember struct {
	Name    string  `json:"name"`
	AddrB64 string  `json:"addr_b64"`
	Port    uint16  `json:"port"`
	MetaB64 *string `json:"meta_b64"`
	State   int     `json:"state"`
	Vsn     []uint8 `json:"vsn"`
}

type mlValueDesc struct {
	Version    uint    `json:"version"`
	CodecID    string  `json:"codec_id"`
	Deleted    bool    `json:"deleted"`
	UpdateTime *mlTime `json:"update_time"`
}

type mlMessage struct {
	ID       int      `json:"id"`
	Time     mlTime   `json:"time"`
	Size     int      `json:"size"`
	Key      string   `json:"key"`
	ValueB64 string   `json:"value_b64"`
	Codec    string   `json:"codec"`
	Version  uint     `json:"version"`
	Changes  []string `json:"changes"`
}

type mlStatusPageRequest struct {
	Now                       mlTime                 `json:"now"`
	HealthScore               int                    `json:"health_score"`
	NumMembers                int                    `json:"num_members"`
	Members                   []mlMember             `json:"members"`
	Store                     map[string]mlValueDesc `json:"store"`
	StoreSizes                map[string]int         `json:"store_sizes"`
	TotalStoreSize            int                    `json:"total_store_size"`
	MessageHistoryBufferBytes int                    `json:"message_history_buffer_bytes"`
	Sent                      []mlMessage            `json:"sent"`
	Received                  []mlMessage            `json:"received"`
	ZoneAwareEnabled          bool                   `json:"zone_aware_enabled"`
	ZoneAwareZone             string                 `json:"zone_aware_zone"`
	ZoneAwareRole             string                 `json:"zone_aware_role"`
	MimirTemplatePath         string                 `json:"mimir_template_path"`
	DskitTemplatePath         string                 `json:"dskit_template_path"`
}

type mlStatusPageResponse struct {
	JSON      string `json:"json"`
	MimirHTML string `json:"mimir_html"`
	DskitHTML string `json:"dskit_html"`
}

// fakeMemberlist provides the two *memberlist.Memberlist methods the templates call.
type fakeMemberlist struct{ health, num int }

func (f *fakeMemberlist) GetHealthScore() int { return f.health }
func (f *fakeMemberlist) NumMembers() int     { return f.num }

// templateData mirrors memberlist.StatusPageData field-for-field (templates resolve by name).
type templateData struct {
	Now                       time.Time
	Memberlist                *fakeMemberlist
	SortedMembers             []*hmemberlist.Node
	Store                     map[string]memberlist.ValueDesc
	StoreSizes                map[string]int
	TotalStoreSize            int
	MessageHistoryBufferBytes int
	SentMessages              []memberlist.Message
	ReceivedMessages          []memberlist.Message
	ZoneAwareRouting          memberlist.ZoneAwareRoutingConfig
}

func mlMessages(in []mlMessage) ([]memberlist.Message, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]memberlist.Message, 0, len(in))
	for _, m := range in {
		v, err := base64.StdEncoding.DecodeString(m.ValueB64)
		if err != nil {
			return nil, err
		}
		if len(v) == 0 {
			v = nil
		}
		out = append(out, memberlist.Message{
			ID:      m.ID,
			Time:    m.Time.toTime(),
			Size:    m.Size,
			Pair:    memberlist.KeyValuePair{Key: m.Key, Value: v, Codec: m.Codec},
			Version: m.Version,
			Changes: m.Changes,
		})
	}
	return out, nil
}

func cmdMemberlistStatusPage(input []byte) (any, error) {
	var req mlStatusPageRequest
	if err := json.Unmarshal(input, &req); err != nil {
		return nil, err
	}

	members := make([]*hmemberlist.Node, 0, len(req.Members))
	for _, m := range req.Members {
		addr, err := base64.StdEncoding.DecodeString(m.AddrB64)
		if err != nil {
			return nil, err
		}
		n := &hmemberlist.Node{Name: m.Name, Addr: addr, Port: m.Port, State: hmemberlist.NodeStateType(m.State)}
		if m.MetaB64 != nil {
			if n.Meta, err = base64.StdEncoding.DecodeString(*m.MetaB64); err != nil {
				return nil, err
			}
		}
		if len(m.Vsn) == 6 {
			n.PMin, n.PMax, n.PCur, n.DMin, n.DMax, n.DCur = m.Vsn[0], m.Vsn[1], m.Vsn[2], m.Vsn[3], m.Vsn[4], m.Vsn[5]
		}
		members = append(members, n)
	}
	store := make(map[string]memberlist.ValueDesc, len(req.Store))
	for k, v := range req.Store {
		store[k] = memberlist.ValueDesc{Version: v.Version, CodecID: v.CodecID, Deleted: v.Deleted, UpdateTime: v.UpdateTime.toTime()}
	}
	storeSizes := req.StoreSizes
	if storeSizes == nil {
		storeSizes = map[string]int{}
	}
	sent, err := mlMessages(req.Sent)
	if err != nil {
		return nil, err
	}
	received, err := mlMessages(req.Received)
	if err != nil {
		return nil, err
	}
	zar := memberlist.ZoneAwareRoutingConfig{Enabled: req.ZoneAwareEnabled, Zone: req.ZoneAwareZone, Role: req.ZoneAwareRole}

	// JSON exactly as dskit's HTTPStatusHandler encodes it.
	v := memberlist.StatusPageData{
		Now:                       req.Now.toTime(),
		Memberlist:                &hmemberlist.Memberlist{},
		SortedMembers:             members,
		Store:                     store,
		StoreSizes:                storeSizes,
		TotalStoreSize:            req.TotalStoreSize,
		MessageHistoryBufferBytes: req.MessageHistoryBufferBytes,
		SentMessages:              sent,
		ReceivedMessages:          received,
		ZoneAwareRouting:          zar,
	}
	var jsonBuf bytes.Buffer
	if err := json.NewEncoder(&jsonBuf).Encode(v); err != nil {
		return nil, err
	}

	data := templateData{
		Now:                       v.Now,
		Memberlist:                &fakeMemberlist{health: req.HealthScore, num: req.NumMembers},
		SortedMembers:             members,
		Store:                     store,
		StoreSizes:                storeSizes,
		TotalStoreSize:            req.TotalStoreSize,
		MessageHistoryBufferBytes: req.MessageHistoryBufferBytes,
		SentMessages:              sent,
		ReceivedMessages:          received,
		ZoneAwareRouting:          zar,
	}

	// pkg/api/handlers.go memberlistStatusHandler.
	mimirSrc, err := os.ReadFile(req.MimirTemplatePath)
	if err != nil {
		return nil, err
	}
	mimirTpl := template.New("memberlist_status")
	mimirTpl.Funcs(map[string]interface{}{
		"StringsJoin": strings.Join,
		"GetZoneFromMeta": func(meta []byte) string {
			return memberlist.EncodedNodeMetadata(meta).Zone()
		},
		"GetRoleFromMeta": func(meta []byte) string {
			return memberlist.EncodedNodeMetadata(meta).Role().String()
		},
		"FormatBytes": func(bytes int) string {
			return humanize.IBytes(uint64(bytes))
		},
	})
	template.Must(mimirTpl.Parse(string(mimirSrc)))
	var mimirBuf bytes.Buffer
	if err := mimirTpl.Execute(&mimirBuf, data); err != nil {
		// HTTPStatusHandler: the partial page is already written, then http.Error appends the error.
		fmt.Fprintln(&mimirBuf, err.Error())
	}

	// dskit defaultPageTemplate.
	dskitSrc, err := os.ReadFile(req.DskitTemplatePath)
	if err != nil {
		return nil, err
	}
	dskitTpl := template.Must(template.New("webpage").Funcs(template.FuncMap{
		"StringsJoin": strings.Join,
		"GetZoneFromMeta": func(meta []byte) string {
			return memberlist.EncodedNodeMetadata(meta).Zone()
		},
		"GetRoleFromMeta": func(meta []byte) string {
			return memberlist.EncodedNodeMetadata(meta).Role().String()
		},
	}).Parse(string(dskitSrc)))
	var dskitBuf bytes.Buffer
	if err := dskitTpl.Execute(&dskitBuf, data); err != nil {
		fmt.Fprintln(&dskitBuf, err.Error())
	}

	return mlStatusPageResponse{JSON: jsonBuf.String(), MimirHTML: mimirBuf.String(), DskitHTML: dskitBuf.String()}, nil
}

type mlFormatValueRequest struct {
	Codec    string `json:"codec"`
	ValueB64 string `json:"value_b64"`
	Format   string `json:"format"`
}

// cmdMemberlistFormatValue decodes a KV value with Mimir's memberlist codecs and formats it like
// dskit's (unexported) formatValue.
func cmdMemberlistFormatValue(input []byte) (any, error) {
	var req mlFormatValueRequest
	if err := json.Unmarshal(input, &req); err != nil {
		return nil, err
	}
	var c codec.Codec
	for _, cc := range []codec.Codec{ring.GetCodec(), ring.GetPartitionRingCodec(), distributor.GetReplicaDescCodec()} {
		if cc.CodecID() == req.Codec {
			c = cc
		}
	}
	if c == nil {
		return nil, fmt.Errorf("unknown codec %q", req.Codec)
	}
	raw, err := base64.StdEncoding.DecodeString(req.ValueB64)
	if err != nil {
		return nil, err
	}
	val, err := c.Decode(raw)
	if err != nil {
		return nil, err
	}

	var w bytes.Buffer
	switch req.Format {
	case "json", "json-pretty":
		enc := json.NewEncoder(&w)
		if req.Format == "json-pretty" {
			enc.SetIndent("", "    ")
		}
		if err := enc.Encode(val); err != nil {
			return nil, err
		}
	default:
		_, _ = fmt.Fprintf(&w, "%#v", val)
	}
	return map[string]string{"body": w.String()}, nil
}
