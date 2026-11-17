// Command yamltypes dumps the go.yaml.in/yaml/v3 decoding view of mimir.Config as JSON:
// every reachable type with its reflect kind/name, which yaml unmarshaler interface it
// implements, and for structs the flattened yaml field map exactly as yaml.v3's
// getStructInfo builds it. The Rust port uses it to reproduce yaml.v3 decode errors
// ("field X not found in type T", "cannot unmarshal !!str `x` into int", ...).
//
// Build/run (see crates/mimir-config/tools/gen-yaml-types.sh).
package main

import (
	"encoding"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"

	"go.yaml.in/yaml/v3"

	"github.com/grafana/mimir/pkg/mimir"
)

type field struct {
	Key       string `json:"key"`
	Type      int    `json:"type"`
	OmitEmpty bool   `json:"omitempty,omitempty"`
}

type typeInfo struct {
	Name      string  `json:"name"`
	Kind      string  `json:"kind"`
	Bits      int     `json:"bits,omitempty"`
	Unmarsh   string  `json:"unmarshaler,omitempty"`
	Elem      *int    `json:"elem,omitempty"`
	Key       *int    `json:"key,omitempty"`
	Fields    []field `json:"fields,omitempty"`
	InlineMap *int    `json:"inline_map,omitempty"`
	InlineUnm bool    `json:"inline_unmarshaler,omitempty"`
	// For custom unmarshalers: the type name yaml.v3 reports when the unmarshaler decodes a
	// mapping into its plain struct form (probed), e.g. "validation.plain".
	Delegate string `json:"delegate,omitempty"`
}

var (
	unmarshalerType     = reflect.TypeOf((*yaml.Unmarshaler)(nil)).Elem()
	obsoleteType        = reflect.TypeOf((*interface{ UnmarshalYAML(func(interface{}) error) error })(nil)).Elem()
	textUnmarshalerType = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()
)

type gen struct {
	ids   map[reflect.Type]int
	types []*typeInfo
}

func (g *gen) id(t reflect.Type) int {
	if id, ok := g.ids[t]; ok {
		return id
	}
	id := len(g.types)
	g.ids[t] = id
	ti := &typeInfo{Name: t.String(), Kind: t.Kind().String()}
	g.types = append(g.types, ti)
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64:
		ti.Bits = t.Bits()
	}
	pt := reflect.PointerTo(t)
	switch {
	case pt.Implements(unmarshalerType):
		ti.Unmarsh = "node"
	case pt.Implements(obsoleteType):
		ti.Unmarsh = "obsolete"
	case pt.Implements(textUnmarshalerType):
		ti.Unmarsh = "text"
	}
	if ti.Unmarsh == "node" || ti.Unmarsh == "obsolete" {
		ti.Delegate = probeDelegate(t)
	}
	switch t.Kind() {
	case reflect.Ptr, reflect.Slice, reflect.Array:
		e := g.id(t.Elem())
		ti.Elem = &e
	case reflect.Map:
		k := g.id(t.Key())
		e := g.id(t.Elem())
		ti.Key, ti.Elem = &k, &e
	case reflect.Struct:
		g.structFields(t, ti)
	}
	return id
}

// structFields mirrors go.yaml.in/yaml/v3 getStructInfo.
func (g *gen) structFields(st reflect.Type, ti *typeInfo) {
	for i := 0; i < st.NumField(); i++ {
		f := st.Field(i)
		if f.PkgPath != "" && !f.Anonymous {
			continue
		}
		tag := f.Tag.Get("yaml")
		if tag == "" && !strings.Contains(string(f.Tag), ":") {
			tag = string(f.Tag)
		}
		if tag == "-" {
			continue
		}
		inline, omitEmpty := false, false
		parts := strings.Split(tag, ",")
		for _, fl := range parts[1:] {
			switch fl {
			case "inline":
				inline = true
			case "omitempty":
				omitEmpty = true
			}
		}
		tag = parts[0]
		if inline {
			switch f.Type.Kind() {
			case reflect.Map:
				e := g.id(f.Type.Elem())
				ti.InlineMap = &e
			default:
				ft := f.Type
				for ft.Kind() == reflect.Ptr {
					ft = ft.Elem()
				}
				if reflect.PointerTo(ft).Implements(unmarshalerType) {
					ti.InlineUnm = true
					continue
				}
				sub := &typeInfo{}
				g.structFields(ft, sub)
				ti.Fields = append(ti.Fields, sub.Fields...)
				if sub.InlineUnm {
					ti.InlineUnm = true
				}
			}
			continue
		}
		key := tag
		if key == "" {
			key = strings.ToLower(f.Name)
		}
		ti.Fields = append(ti.Fields, field{Key: key, Type: g.id(f.Type), OmitEmpty: omitEmpty})
	}
}

const probeKey = "zz_mimir_rs_probe_unknown_field"

// probeDelegate decodes a mapping with an unknown key into a fresh value using the type's
// custom unmarshaler, and returns the struct type name reported by yaml.v3, if any.
func probeDelegate(t reflect.Type) (name string) {
	defer func() { _ = recover() }()
	var node yaml.Node
	if err := yaml.Unmarshal([]byte(probeKey+": 1\n"), &node); err != nil {
		return ""
	}
	v := reflect.New(t)
	err := node.Content[0].DecodeWithOptions(v.Interface(), yaml.DecodeOptions{KnownFields: true})
	if err == nil {
		return ""
	}
	msg := err.Error()
	marker := "field " + probeKey + " not found in type "
	if idx := strings.Index(msg, marker); idx >= 0 {
		rest := msg[idx+len(marker):]
		if nl := strings.IndexByte(rest, '\n'); nl >= 0 {
			rest = rest[:nl]
		}
		return rest
	}
	return ""
}

func main() {
	g := &gen{ids: map[reflect.Type]int{}}
	root := g.id(reflect.TypeOf(mimir.Config{}))
	out := struct {
		Root  int         `json:"root"`
		Types []*typeInfo `json:"types"`
	}{root, g.types}
	enc := json.NewEncoder(os.Stdout)
	if err := enc.Encode(out); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
