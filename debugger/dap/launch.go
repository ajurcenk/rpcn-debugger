package dap

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// LaunchConfig is the launch contract (AD-12): the keys a launch.json entry
// of type redpanda-connect may set. It is the only source of the schema,
// the defaults and the constraints. vscode/package.json's
// configurationAttributes.launch and initialConfigurations are generated
// from it by cmd/gen-launch-schema (task gen:schema), and task ci fails when
// they drift.
//
// The tags read are:
//   - json: the key (camelCase);
//   - desc: the schema description;
//   - default: the default value, applied when the key is absent;
//   - min: the minimum of an integer, or the minimum length of a string;
//   - max: the maximum of an integer;
//   - enum: the comma-separated values a string map's values must be one of.
type LaunchConfig struct {
	Pipeline string `json:"pipeline" min:"1" desc:"The pipeline YAML file to debug. A relative path resolves against cwd. When unset, the file in the active editor is debugged."`

	Cwd string `json:"cwd" desc:"The directory that relative paths (pipeline, envFile) resolve against. A relative cwd resolves against the debugger's working directory. When unset, the workspace folder is used."`

	MaxMessages int `json:"maxMessages" default:"10" min:"1" max:"1024" desc:"The most messages of the captured batch to step through, at most 1024. The rest of the batch is dropped, never acknowledged, and the Debug Console says how many messages were kept."`

	Env map[string]string `json:"env" desc:"Environment variables for the pipeline's ${VAR} interpolations. Values accept ${env:NAME}, which VS Code substitutes from its own environment: reference secrets that way and never put literal secrets in launch.json. Applied last: env overrides envFile, which overrides the debugger's own environment."`

	EnvFile string `json:"envFile" min:"1" desc:"A dotenv file of NAME=value environment variables for the pipeline's ${VAR} interpolations: # comments, blank lines, an optional export prefix and quoted values are allowed. A relative path resolves against cwd, and the file must exist. Keep secrets in this file, not in launch.json. Applied after the debugger's own environment and before env, so env overrides it."`

	KeepConsumerGroup bool `json:"keepConsumerGroup" default:"false" desc:"Keep the consumer groups of the pipeline's inputs for the capture. By default the capture replaces each Kafka-family consumer_group (kafka, kafka_franz, redpanda, redpanda_common) and each nats queue with its own debug group rpcn-debug-<user>-<k>, so the production group never rebalances. A kept group joins the production group, whose members rebalance, so the launch then also needs allowSideEffects."`

	AllowSideEffects bool `json:"allowSideEffects" default:"false" desc:"Allow capturing from inputs whose capture has side effects on the system they read from, such as a queue (aws_sqs, amqp_0_9, redis_list, …), a consumer group that is kept, or an input the debugger doesn't know. Each such input gets a warning naming its effect. Without it, the launch fails with SIDE_EFFECTS_NOT_ALLOWED. A rejected input (stdin, http_server, sockets) can't be debugged either way."`

	InputClasses map[string]string `json:"inputClasses" enum:"replay-safe,side-effecting,rejected" desc:"Overrides the debugger's class of an input component, by component name, e.g. {\"aws_sqs\": \"replay-safe\"}: replay-safe (captured freely), side-effecting (needs allowSideEffects) or rejected (never captured). A composite input such as broker takes its worst child's class after the override. Marking an input safer than the debugger thinks it is gets a warning, because the debugger can't prove it."`
}

// vsCodeKeys are the keys VS Code itself adds to launch configs. They are
// accepted and ignored, as is any key starting with "__" (such as
// __sessionId or __configurationTarget).
var vsCodeKeys = map[string]bool{
	"type":                           true,
	"request":                        true,
	"name":                           true,
	"presentation":                   true,
	"preLaunchTask":                  true,
	"postDebugTask":                  true,
	"internalConsoleOptions":         true,
	"debugServer":                    true,
	"noDebug":                        true,
	"suppressMultipleSessionWarning": true,
}

func isVSCodeKey(key string) bool {
	return vsCodeKeys[key] || strings.HasPrefix(key, "__")
}

// launchField is one LaunchConfig key, read from its struct tags.
type launchField struct {
	key   string
	index int
	typ   reflect.Type
	desc  string
	def   any // nil: no default
	min   *int
	max   *int     // integers only
	enum  []string // string maps only: the allowed values
}

// launchFields returns the LaunchConfig keys in struct order. The tags are
// fixed at compile time, so a malformed one is a programming error.
func launchFields() []launchField {
	t := reflect.TypeFor[LaunchConfig]()
	out := make([]launchField, 0, t.NumField())
	for i := range t.NumField() {
		sf := t.Field(i)
		key, _, _ := strings.Cut(sf.Tag.Get("json"), ",")
		f := launchField{key: key, index: i, typ: sf.Type, desc: sf.Tag.Get("desc")}
		if d, ok := sf.Tag.Lookup("default"); ok {
			v := reflect.New(sf.Type)
			src := d
			if sf.Type.Kind() == reflect.String {
				src = strconv.Quote(d)
			}
			if err := json.Unmarshal([]byte(src), v.Interface()); err != nil {
				panic(fmt.Sprintf("LaunchConfig.%s: bad default tag %q: %v", sf.Name, d, err))
			}
			f.def = v.Elem().Interface()
		}
		if m, ok := sf.Tag.Lookup("min"); ok {
			n, err := strconv.Atoi(m)
			if err != nil {
				panic(fmt.Sprintf("LaunchConfig.%s: bad min tag %q: %v", sf.Name, m, err))
			}
			f.min = &n
		}
		if m, ok := sf.Tag.Lookup("max"); ok {
			n, err := strconv.Atoi(m)
			if err != nil || sf.Type.Kind() != reflect.Int {
				panic(fmt.Sprintf("LaunchConfig.%s: bad max tag %q (integers only): %v", sf.Name, m, err))
			}
			f.max = &n
		}
		if e, ok := sf.Tag.Lookup("enum"); ok {
			if sf.Type.Kind() != reflect.Map || e == "" {
				panic(fmt.Sprintf("LaunchConfig.%s: bad enum tag %q (string maps only)", sf.Name, e))
			}
			f.enum = strings.Split(e, ",")
		}
		out = append(out, f)
	}
	return out
}

// kind is the JSON type a field's values must have, as said in errors.
func (f launchField) kind() string {
	switch f.typ.Kind() {
	case reflect.String:
		return "a string"
	case reflect.Int:
		return "an integer"
	case reflect.Bool:
		return "a boolean"
	case reflect.Map:
		return "an object of string values"
	default:
		panic(fmt.Sprintf("LaunchConfig key %q: unsupported type %s", f.key, f.typ))
	}
}

func invalidLaunch(format string, args ...any) error {
	return core.NewError(core.CodeLaunchInvalid, "invalid launch configuration: "+format, args...)
}

// decodeLaunch decodes the launch arguments strictly into a LaunchConfig
// with its defaults applied. An unknown key or a value of the wrong type or
// out of range is LAUNCH_INVALID, naming the key. The keys VS Code adds are
// ignored. Paths are left as given: the Session resolves them.
func decodeLaunch(raw json.RawMessage) (LaunchConfig, error) {
	var cfg LaunchConfig
	fields := launchFields()
	v := reflect.ValueOf(&cfg).Elem()
	for _, f := range fields {
		if f.def != nil {
			v.Field(f.index).Set(reflect.ValueOf(f.def))
		}
	}

	var args map[string]json.RawMessage
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &args); err != nil {
			return LaunchConfig{}, invalidLaunch("the launch arguments must be a JSON object: %v", err)
		}
	}

	known := map[string]launchField{}
	names := make([]string, 0, len(fields))
	for _, f := range fields {
		known[f.key] = f
		names = append(names, f.key)
	}
	var unknown []string
	for k := range args {
		if _, ok := known[k]; !ok && !isVSCodeKey(k) {
			unknown = append(unknown, strconv.Quote(k))
		}
	}
	if len(unknown) > 0 {
		slices.Sort(unknown)
		noun := "key"
		if len(unknown) > 1 {
			noun = "keys"
		}
		return LaunchConfig{}, invalidLaunch("unknown %s %s; the launch keys are %s",
			noun, strings.Join(unknown, ", "), strings.Join(names, ", "))
	}

	for _, f := range fields {
		val, ok := args[f.key]
		if !ok {
			continue
		}
		if string(bytes.TrimSpace(val)) == "null" {
			return LaunchConfig{}, invalidLaunch("%q must be %s, not null", f.key, f.kind())
		}
		// Errors never echo a value: env values may be secrets VS Code
		// substituted from ${env:NAME} (AD-22).
		if f.typ.Kind() == reflect.Map {
			if err := checkStringMap(f.key, val, f.enum); err != nil {
				return LaunchConfig{}, err
			}
		}
		ptr := reflect.New(f.typ)
		if err := json.Unmarshal(val, ptr.Interface()); err != nil {
			got := jsonKind(val)
			if got == "a number" && f.typ.Kind() == reflect.Int {
				got = "a non-integer or out-of-range number"
			}
			return LaunchConfig{}, invalidLaunch("%q must be %s, not %s", f.key, f.kind(), got)
		}
		if f.min != nil {
			switch f.typ.Kind() {
			case reflect.Int:
				if n := ptr.Elem().Int(); n < int64(*f.min) {
					return LaunchConfig{}, invalidLaunch("%q must be at least %d, not %d", f.key, *f.min, n)
				}
			case reflect.String:
				if len(ptr.Elem().String()) < *f.min {
					if *f.min == 1 {
						return LaunchConfig{}, invalidLaunch("%q must be a non-empty string", f.key)
					}
					return LaunchConfig{}, invalidLaunch("%q must be at least %d characters long", f.key, *f.min)
				}
			}
		}
		if f.max != nil {
			if n := ptr.Elem().Int(); n > int64(*f.max) {
				return LaunchConfig{}, invalidLaunch("%q must be at most %d, not %d", f.key, *f.max, n)
			}
		}
		v.Field(f.index).Set(ptr.Elem())
	}
	return cfg, nil
}

// checkStringMap checks that val is an object of string values, each one of
// enum when it is set, naming the first offending entry (in key order) by
// its key only.
func checkStringMap(key string, val json.RawMessage, enum []string) error {
	var entries map[string]json.RawMessage
	if err := json.Unmarshal(val, &entries); err != nil {
		return invalidLaunch("%q must be an object of string values, not %s", key, jsonKind(val))
	}
	names := make([]string, 0, len(entries))
	for k := range entries {
		names = append(names, k)
	}
	slices.Sort(names)
	for _, k := range names {
		if got := jsonKind(entries[k]); got != "a string" {
			return invalidLaunch("%q must be a string, not %s", key+"."+k, got)
		}
		var v string
		if err := json.Unmarshal(entries[k], &v); err != nil {
			return invalidLaunch("%q must be a string: %v", key+"."+k, err)
		}
		if enum != nil && !slices.Contains(enum, v) {
			return invalidLaunch("%q must be one of %s", key+"."+k, strings.Join(enum, ", "))
		}
	}
	return nil
}

// jsonKind names the JSON type of a valid JSON value, for errors.
func jsonKind(val json.RawMessage) string {
	v := bytes.TrimSpace(val)
	if len(v) == 0 {
		return "empty"
	}
	switch v[0] {
	case '{':
		return "an object"
	case '[':
		return "an array"
	case '"':
		return "a string"
	case 't', 'f':
		return "a boolean"
	case 'n':
		return "null"
	default:
		return "a number"
	}
}

// Schema returns the JSON schema of the launch request: the value of
// configurationAttributes.launch in vscode/package.json, derived from
// LaunchConfig's tags.
func Schema() map[string]any {
	props := map[string]any{}
	for _, f := range launchFields() {
		p := map[string]any{"description": f.desc}
		switch f.typ.Kind() {
		case reflect.String:
			p["type"] = "string"
			if f.min != nil {
				p["minLength"] = *f.min
			}
		case reflect.Int:
			p["type"] = "integer"
			if f.min != nil {
				p["minimum"] = *f.min
			}
			if f.max != nil {
				p["maximum"] = *f.max
			}
		case reflect.Bool:
			p["type"] = "boolean"
		case reflect.Map:
			p["type"] = "object"
			values := map[string]any{"type": "string"}
			if f.enum != nil {
				values["enum"] = f.enum
			}
			p["additionalProperties"] = values
		default:
			panic(fmt.Sprintf("LaunchConfig key %q: unsupported type %s", f.key, f.typ))
		}
		if f.def != nil {
			p["default"] = f.def
		}
		props[f.key] = p
	}
	return map[string]any{"properties": props}
}

// InitialConfigurations returns the launch.json entries VS Code writes when
// the user creates a launch.json: one entry debugging the active file.
func InitialConfigurations() []any {
	return []any{map[string]any{
		"type":     "redpanda-connect",
		"request":  "launch",
		"name":     "Debug pipeline",
		"pipeline": "${file}",
	}}
}
