// Command genwsevent regenerates the event plumbing that neither oapi-codegen
// nor protoc-gen-go produces.
//
// Two event families arrive at the SDK and both are generated here, from their
// own source of truth, so that the constraints and every switch over them cannot
// drift apart:
//
// Coordinator events. The generated models expose them as VideoEvent, an opaque
// JSON union with a ValueByDiscriminator method and no Go methods on the
// concrete event structs. The SDK instead wants a WebsocketEvent interface it can
// type switch on, plus a Handler with one callback per event. Both are
// mechanical derivations of the VideoEvent union, and the event list changes
// every time the OpenAPI spec does.
//
// SFU signal events. The wire type is the SfuEvent protobuf, whose event_payload
// oneof means the concrete Go type reaching the SDK is always an SfuEvent_*
// wrapper, never the inner message. The set of wrappers is read out of the
// generated events.pb.go, so signal.Events cannot list a type the event store is
// unable to match.
//
// Run it through ./generate.sh, or directly:
//
//	go run ./internal/cmd/genwsevent
package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	modelsFile     = "coordinator/models/models.gen.go"
	unionType      = "VideoEvent"
	callCidField   = "CallCid"
	modelsPkgAlias = "models"

	// The SfuEvent oneof wrappers live in the pre-generated protobuf package,
	// each marked by the unexported interface method below.
	sfuEventPkgPath  = "github.com/GetStream/protocol/protobuf/video/sfu/event"
	sfuEventFile     = "events.pb.go"
	sfuEventMarker   = "isSfuEvent_EventPayload"
	sfuEventPkgAlias = "sfu_events"

	// signalPkgAlias is the alias signal/events.go already uses, kept so the
	// generated union reads the same as the hand-written code beside it.
	signalEventPkgAlias = "sfuevent"
)

type event struct {
	// Type is the Go struct name, e.g. CallCreatedEvent.
	Type string
	// Discriminator is the wire value of the "type" field, e.g. call.created.
	Discriminator string
	// HasCallCid reports whether the event carries a call_cid, which is what
	// makes it addressable per call.
	HasCallCid bool
}

// inputs is everything the generated files are derived from.
type inputs struct {
	// coordinator holds the VideoEvent union members.
	coordinator []event
	// signal holds the SfuEvent oneof wrapper type names.
	signal []string
}

func (in inputs) callScoped() []event {
	var out []event
	for _, e := range in.coordinator {
		if e.HasCallCid {
			out = append(out, e)
		}
	}
	return out
}

func main() {
	log.SetFlags(0)

	root, err := moduleRoot()
	if err != nil {
		log.Fatal(err)
	}

	in, err := loadInputs(root)
	if err != nil {
		log.Fatal(err)
	}

	files, err := render(in)
	if err != nil {
		log.Fatal(err)
	}
	for name, src := range files {
		if err := os.WriteFile(filepath.Join(root, name), src, 0o644); err != nil {
			log.Fatal(err)
		}
	}

	fmt.Printf("genwsevent: %d coordinator events (%d call-scoped), %d signal events\n",
		len(in.coordinator), len(in.callScoped()), len(in.signal))
}

// loadInputs reads both event families out of the generated code that defines
// them.
func loadInputs(root string) (inputs, error) {
	coordinatorEvents, err := parseEvents(filepath.Join(root, modelsFile))
	if err != nil {
		return inputs{}, err
	}
	if len(coordinatorEvents) == 0 {
		return inputs{}, fmt.Errorf("no events found in %s: has the union type %s been renamed?", modelsFile, unionType)
	}

	signalEvents, err := parseSignalEvents()
	if err != nil {
		return inputs{}, err
	}
	if len(signalEvents) == 0 {
		return inputs{}, fmt.Errorf("no %s implementations found in %s: has the SfuEvent oneof been renamed?", sfuEventMarker, sfuEventFile)
	}

	return inputs{coordinator: coordinatorEvents, signal: signalEvents}, nil
}

// render produces every generated file, keyed by its module-relative path. It is
// separate from main so a test can compare the committed files against what the
// current inputs produce.
func render(in inputs) (map[string][]byte, error) {
	sources := map[string][]byte{
		"coordinator/models/wsevent_types.gen.go": renderEventTypes(in.coordinator),
		"coordinator/events.gen.go":               renderEvents(in.coordinator),
		"coordinator/handler.gen.go":              renderHandler(in.coordinator),
		"signal/events.gen.go":                    renderSignalEvents(in.signal),
		"events_dispatch.gen.go":                  renderCallEventDispatch(in),
		"events_dispatch_cases.gen_test.go":       renderCallEventDispatchCases(in),
	}

	out := make(map[string][]byte, len(sources))
	for name, src := range sources {
		formatted, err := format.Source(src)
		if err != nil {
			return nil, fmt.Errorf("formatting %s: %w", name, err)
		}
		out[name] = formatted
	}
	return out, nil
}

// parseSignalEvents returns the SfuEvent oneof wrapper type names. Those are the
// only concrete types SfuEvent.GetEventPayload can return, and therefore the only
// types signal's event store and HandleEvent can ever match.
func parseSignalEvents() ([]string, error) {
	dir, err := packageDir(sfuEventPkgPath)
	if err != nil {
		return nil, err
	}

	path := filepath.Join(dir, sfuEventFile)
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		return nil, err
	}

	var types []string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != sfuEventMarker {
			continue
		}
		if recv := receiverType(fn); recv != "" {
			types = append(types, recv)
		}
	}
	sort.Strings(types)
	return types, nil
}

func packageDir(pkgPath string) (string, error) {
	out, err := exec.Command("go", "list", "-f", "{{.Dir}}", pkgPath).Output()
	if err != nil {
		return "", fmt.Errorf("locate package %s: %w", pkgPath, err)
	}
	dir := strings.TrimSpace(string(out))
	if dir == "" {
		return "", fmt.Errorf("locate package %s: no directory reported", pkgPath)
	}
	return dir, nil
}

// moduleRoot walks up from the working directory until it finds go.mod, so the
// generator can be run from anywhere in the module.
func moduleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no go.mod found above %s", dir)
		}
		dir = parent
	}
}

func parseEvents(path string) ([]event, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		return nil, err
	}

	withCallCid := structsWithField(file, callCidField)

	var events []event
	for discriminator, typeName := range unionMembers(file) {
		events = append(events, event{
			Type:          typeName,
			Discriminator: discriminator,
			HasCallCid:    withCallCid[typeName],
		})
	}
	sort.Slice(events, func(i, j int) bool { return events[i].Type < events[j].Type })
	return events, nil
}

// unionMembers reads the discriminator -> struct mapping out of
// (VideoEvent).ValueByDiscriminator, which is the spec's own view of which
// events can arrive on the websocket.
func unionMembers(file *ast.File) map[string]string {
	members := map[string]string{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "ValueByDiscriminator" || receiverType(fn) != unionType {
			continue
		}
		ast.Inspect(fn, func(n ast.Node) bool {
			clause, ok := n.(*ast.CaseClause)
			if !ok || len(clause.List) != 1 || len(clause.Body) != 1 {
				return true
			}
			lit, ok := clause.List[0].(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			ret, ok := clause.Body[0].(*ast.ReturnStmt)
			if !ok || len(ret.Results) != 1 {
				return true
			}
			call, ok := ret.Results[0].(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !strings.HasPrefix(sel.Sel.Name, "As") {
				return true
			}
			discriminator, err := strconv.Unquote(lit.Value)
			if err != nil {
				return true
			}
			members[discriminator] = strings.TrimPrefix(sel.Sel.Name, "As")
			return true
		})
	}
	return members
}

func receiverType(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return ""
	}
	switch t := fn.Recv.List[0].Type.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.StarExpr:
		if ident, ok := t.X.(*ast.Ident); ok {
			return ident.Name
		}
	}
	return ""
}

func structsWithField(file *ast.File, field string) map[string]bool {
	found := map[string]bool{}
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.TYPE {
			continue
		}
		for _, spec := range gen.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok {
				continue
			}
			structType, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				continue
			}
			for _, f := range structType.Fields.List {
				for _, name := range f.Names {
					if name.Name == field {
						found[typeSpec.Name.Name] = true
					}
				}
			}
		}
	}
	return found
}

const header = "// Code generated by internal/cmd/genwsevent. DO NOT EDIT.\n\n"

func renderEventTypes(events []event) []byte {
	var b bytes.Buffer
	b.WriteString(header)
	b.WriteString("package models\n\n")
	b.WriteString("// GetEventType implementations, one per websocket event. They are declared\n")
	b.WriteString("// on the value type so that both T and *T satisfy WebsocketEvent.\n\n")
	for _, e := range events {
		fmt.Fprintf(&b, "func (%s) GetEventType() string { return %q }\n\n", e.Type, e.Discriminator)
	}
	return b.Bytes()
}

func renderEvents(events []event) []byte {
	var b bytes.Buffer
	b.WriteString(header)
	b.WriteString("package coordinator\n\n")
	fmt.Fprintf(&b, "import %s %q\n\n", modelsPkgAlias, "github.com/GetStream/getstream-go-webrtc/coordinator/models")

	b.WriteString("// Events constrains the event types HandleEvent and AwaitEvent accept.\n")
	writeUnion(&b, "Events", events, func(event) bool { return true })

	b.WriteString("// CallEvents constrains the events that belong to a specific call, i.e.\n")
	b.WriteString("// those carrying a call_cid.\n")
	writeUnion(&b, "CallEvents", events, func(e event) bool { return e.HasCallCid })

	b.WriteString("// getCallCid returns the call a call-scoped event belongs to.\n")
	b.WriteString("func getCallCid[T CallEvents](e T) string {\n\tswitch e := any(e).(type) {\n")
	for _, e := range events {
		if !e.HasCallCid {
			continue
		}
		fmt.Fprintf(&b, "\tcase *%s.%s:\n\t\treturn e.CallCid\n", modelsPkgAlias, e.Type)
	}
	b.WriteString("\tdefault:\n\t\treturn \"\"\n\t}\n}\n\n")

	b.WriteString("// dispatch routes an event to the matching Handler callback. It reports\n")
	b.WriteString("// whether the event was recognised.\n")
	b.WriteString("func dispatch(h Handler, e models.WebsocketEvent) bool {\n\tswitch e := e.(type) {\n")
	for _, ev := range events {
		fmt.Fprintf(&b, "\tcase *%s.%s:\n\t\th.On%s(e)\n", modelsPkgAlias, ev.Type, ev.Type)
	}
	b.WriteString("\tdefault:\n\t\treturn false\n\t}\n\treturn true\n}\n")
	return b.Bytes()
}

func writeUnion(b *bytes.Buffer, name string, events []event, include func(event) bool) {
	fmt.Fprintf(b, "type %s interface {\n", name)
	first := true
	for _, e := range events {
		if !include(e) {
			continue
		}
		if first {
			fmt.Fprintf(b, "\t*%s.%s", modelsPkgAlias, e.Type)
			first = false
			continue
		}
		fmt.Fprintf(b, " |\n\t\t*%s.%s", modelsPkgAlias, e.Type)
	}
	b.WriteString("\n}\n\n")
}

func renderSignalEvents(types []string) []byte {
	var b bytes.Buffer
	b.WriteString(header)
	b.WriteString("package signal\n\n")
	fmt.Fprintf(&b, "import %s %q\n\n", signalEventPkgAlias, sfuEventPkgPath)

	b.WriteString("// Events constrains the event types HandleEvent and AwaitEvent accept.\n")
	b.WriteString("//\n")
	b.WriteString("// Every member is an SfuEvent_* oneof wrapper, because that is what\n")
	b.WriteString("// SfuEvent.GetEventPayload returns and therefore what both HandleEvent and the\n")
	b.WriteString("// event store match on. Listing an inner message type here would compile and\n")
	b.WriteString("// never match, which is why the list is generated from the oneof itself.\n")
	b.WriteString("type Events interface {\n")
	for i, t := range types {
		if i == 0 {
			fmt.Fprintf(&b, "\t*%s.%s", signalEventPkgAlias, t)
			continue
		}
		fmt.Fprintf(&b, " |\n\t\t*%s.%s", signalEventPkgAlias, t)
	}
	b.WriteString("\n}\n")
	return b.Bytes()
}

// renderCallEventDispatch emits the switch behind rtc.HandleCallEvent. It
// covers both families from the same inputs as the CallEvents constraint, so the
// two cannot drift: the hand-written version this replaced had 19 of the 78
// admitted types missing and panicked on each of them.
func renderCallEventDispatch(in inputs) []byte {
	var b bytes.Buffer
	b.WriteString(header)
	b.WriteString("package rtc\n\n")
	fmt.Fprintf(&b, "import (\n\t%s %q\n\n", sfuEventPkgAlias, sfuEventPkgPath)
	b.WriteString("\t\"github.com/GetStream/getstream-go-webrtc/coordinator\"\n")
	b.WriteString("\t\"github.com/GetStream/getstream-go-webrtc/coordinator/models\"\n")
	b.WriteString("\t\"github.com/GetStream/getstream-go-webrtc/signal\"\n)\n\n")

	b.WriteString("// dispatchCallEvent registers onEvent with the store that carries T: the\n")
	b.WriteString("// coordinator event store for a coordinator.CallEvents member, the SFU signal\n")
	b.WriteString("// store for a signal.Events member. It reports whether T was recognised, which\n")
	b.WriteString("// is always true while this file and both constraints are generated together.\n")
	b.WriteString("func dispatchCallEvent[T CallEvents](call *Call, onEvent func(T)) (func(), bool) {\n")
	b.WriteString("\tvar zero T\n\tswitch any(zero).(type) {\n")

	b.WriteString("\t// coordinator.CallEvents\n")
	for _, e := range in.callScoped() {
		fmt.Fprintf(&b, "\tcase *%s.%s:\n", modelsPkgAlias, e.Type)
		fmt.Fprintf(&b, "\t\treturn coordinator.HandleCallEvent(call.cc.CoordinatorClientInterface, call.CID(), castEventHandlerFunc[T, *%s.%s](onEvent)), true\n",
			modelsPkgAlias, e.Type)
	}

	b.WriteString("\n\t// signal.Events\n")
	for _, t := range in.signal {
		fmt.Fprintf(&b, "\tcase *%s.%s:\n", sfuEventPkgAlias, t)
		fmt.Fprintf(&b, "\t\treturn signal.HandleEvent(call.Client(), castEventHandlerFunc[T, *%s.%s](onEvent)), true\n",
			sfuEventPkgAlias, t)
	}

	b.WriteString("\tdefault:\n\t\treturn nil, false\n\t}\n}\n")
	return b.Bytes()
}

// renderCallEventDispatchCases emits one registration per type CallEvents admits,
// for the exhaustiveness test in events_dispatch_test.go to walk.
func renderCallEventDispatchCases(in inputs) []byte {
	var b bytes.Buffer
	b.WriteString(header)
	b.WriteString("package rtc\n\n")
	fmt.Fprintf(&b, "import (\n\t%s %q\n\n", sfuEventPkgAlias, sfuEventPkgPath)
	b.WriteString("\t\"github.com/GetStream/getstream-go-webrtc/coordinator/models\"\n)\n\n")

	b.WriteString("// callEventDispatchCase registers a handler for one member of CallEvents.\n")
	b.WriteString("type callEventDispatchCase struct {\n")
	b.WriteString("\t// Type is the constraint member, for failure messages.\n\tType string\n")
	b.WriteString("\t// Register calls dispatchCallEvent with that member as T.\n")
	b.WriteString("\tRegister func(*Call) (func(), bool)\n}\n\n")

	b.WriteString("// callEventDispatchCases covers every type CallEvents admits.\n")
	b.WriteString("var callEventDispatchCases = []callEventDispatchCase{\n")
	for _, e := range in.callScoped() {
		writeDispatchCase(&b, modelsPkgAlias, e.Type)
	}
	for _, t := range in.signal {
		writeDispatchCase(&b, sfuEventPkgAlias, t)
	}
	b.WriteString("}\n")
	return b.Bytes()
}

func writeDispatchCase(b *bytes.Buffer, pkg, typeName string) {
	fmt.Fprintf(b, "\t{\n\t\tType: %q,\n", "*"+pkg+"."+typeName)
	fmt.Fprintf(b, "\t\tRegister: func(call *Call) (func(), bool) {\n")
	fmt.Fprintf(b, "\t\t\treturn dispatchCallEvent(call, func(*%s.%s) {})\n\t\t},\n\t},\n", pkg, typeName)
}

func renderHandler(events []event) []byte {
	var b bytes.Buffer
	b.WriteString(header)
	b.WriteString("package coordinator\n\n")
	fmt.Fprintf(&b, "import %s %q\n\n", modelsPkgAlias, "github.com/GetStream/getstream-go-webrtc/coordinator/models")

	b.WriteString("// Handler receives every event the coordinator websocket delivers.\n")
	b.WriteString("// Embed NoopHandler to implement only the callbacks you care about.\n")
	b.WriteString("type Handler interface {\n")
	for _, e := range events {
		fmt.Fprintf(&b, "\tOn%s(*%s.%s)\n", e.Type, modelsPkgAlias, e.Type)
	}
	b.WriteString("}\n\n")

	b.WriteString("// NoopHandler ignores every event.\n")
	b.WriteString("type NoopHandler struct{}\n\n")
	b.WriteString("var _ Handler = NoopHandler{}\n\n")
	for _, e := range events {
		fmt.Fprintf(&b, "func (NoopHandler) On%s(*%s.%s) {}\n", e.Type, modelsPkgAlias, e.Type)
	}
	return b.Bytes()
}
