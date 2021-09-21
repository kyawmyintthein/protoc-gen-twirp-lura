// Copyright 2018 Twitch Interactive, Inc.  All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"). You may not
// use this file except in compliance with the License. A copy of the License is
// located at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// or in the "license" file accompanying this file. This file is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"go/parser"
	"go/printer"
	"go/token"
	"path"
	"sort"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"
	descriptor "google.golang.org/protobuf/types/descriptorpb"
	plugin "google.golang.org/protobuf/types/pluginpb"

	"github.com/kyawmyintthein/protoc-gen-twirplura/internal/gen"
	"github.com/kyawmyintthein/protoc-gen-twirplura/internal/gen/stringutils"
	"github.com/kyawmyintthein/protoc-gen-twirplura/internal/gen/typemap"
	"github.com/pkg/errors"
)

type twirp struct {
	filesHandled int

	reg *typemap.Registry

	// Map to record whether we've built each package
	pkgs          map[string]string
	pkgNamesInUse map[string]bool

	importPrefix string            // String to prefix to imported package file names.
	importMap    map[string]string // Mapping from .proto file name to import path.

	// Package output:
	sourceRelativePaths bool // instruction on where to write output files
	modulePrefix        string

	// Package naming:
	genPkgName          string // Name of the package that we're generating
	fileToGoPackageName map[*descriptor.FileDescriptorProto]string

	// List of files that were inputs to the generator. We need to hold this in
	// the struct so we can write a header for the file that lists its inputs.
	genFiles []*descriptor.FileDescriptorProto

	// Output buffer that holds the bytes we want to write out for a single file.
	// Gets reset after working on a file.
	output *bytes.Buffer
}

func newGenerator() *twirp {
	t := &twirp{
		pkgs:                make(map[string]string),
		pkgNamesInUse:       make(map[string]bool),
		importMap:           make(map[string]string),
		fileToGoPackageName: make(map[*descriptor.FileDescriptorProto]string),
		output:              bytes.NewBuffer(nil),
	}

	return t
}

func (t *twirp) Generate(in *plugin.CodeGeneratorRequest) *plugin.CodeGeneratorResponse {
	params, err := parseCommandLineParams(in.GetParameter())
	if err != nil {
		gen.Fail("could not parse parameters passed to --twirp_out", err.Error())
	}
	t.importPrefix = params.importPrefix
	t.importMap = params.importMap
	t.sourceRelativePaths = params.paths == "source_relative"
	t.modulePrefix = params.module

	t.genFiles = gen.FilesToGenerate(in)

	// Collect information on types.
	t.reg = typemap.New(in.ProtoFile)

	// Register names of packages that we import.
	t.registerPackageName("bytes")
	t.registerPackageName("strings")
	t.registerPackageName("path")
	t.registerPackageName("ctxsetters")
	t.registerPackageName("context")
	t.registerPackageName("http")
	t.registerPackageName("io")
	t.registerPackageName("ioutil")
	t.registerPackageName("json")
	t.registerPackageName("protojson")
	t.registerPackageName("proto")
	t.registerPackageName("strconv")
	t.registerPackageName("twirp")
	t.registerPackageName("url")
	t.registerPackageName("fmt")
	t.registerPackageName("errors")

	// Time to figure out package names of objects defined in protobuf. First,
	// we'll figure out the name for the package we're generating.
	genPkgName, err := deduceGenPkgName(t.genFiles)
	if err != nil {
		gen.Fail(err.Error())
	}
	t.genPkgName = genPkgName

	// We also need to figure out the fully import path of the package we're
	// generating. It's possible to import proto definitions from different .proto
	// files which will be generated into the same Go package, which we need to
	// detect (and can only detect if files use fully-specified go_package
	// options).
	genPkgImportPath, _, _ := goPackageOption(t.genFiles[0])

	// Next, we need to pick names for all the files that are dependencies.
	for _, f := range in.ProtoFile {
		// Is this is a file we are generating? If yes, it gets the shared package name.
		if fileDescSliceContains(t.genFiles, f) {
			t.fileToGoPackageName[f] = t.genPkgName
			continue
		}

		// Is this is an imported .proto file which has the same fully-specified
		// go_package as the targeted file for generation? If yes, it gets the
		// shared package name too.
		if genPkgImportPath != "" {
			importPath, _, _ := goPackageOption(f)
			if importPath == genPkgImportPath {
				t.fileToGoPackageName[f] = t.genPkgName
				continue
			}
		}

		// This is a dependency from a different go_package. Use its package name.
		name := f.GetPackage()
		if name == "" {
			name = stringutils.BaseName(f.GetName())
		}
		name = stringutils.CleanIdentifier(name)
		alias := t.registerPackageName(name)
		t.fileToGoPackageName[f] = alias
	}

	// Showtime! Generate the response.
	resp := new(plugin.CodeGeneratorResponse)
	resp.SupportedFeatures = proto.Uint64(uint64(plugin.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL))
	for _, f := range t.genFiles {
		respFile := t.generate(f)
		if respFile != nil {
			resp.File = append(resp.File, respFile)
		}
	}
	return resp
}

func (t *twirp) registerPackageName(name string) (alias string) {
	alias = name
	i := 1
	for t.pkgNamesInUse[alias] {
		alias = name + strconv.Itoa(i)
		i++
	}
	t.pkgNamesInUse[alias] = true
	t.pkgs[name] = alias
	return alias
}

// deduceGenPkgName figures out the go package name to use for generated code.
// Will try to use the explicit go_package setting in a file (if set, must be
// consistent in all files). If no files have go_package set, then use the
// protobuf package name (must be consistent in all files)
func deduceGenPkgName(genFiles []*descriptor.FileDescriptorProto) (string, error) {
	var genPkgName string
	for _, f := range genFiles {
		name, explicit := goPackageName(f)
		if explicit {
			name = stringutils.CleanIdentifier(name)
			if genPkgName != "" && genPkgName != name {
				// Make sure they're all set consistently.
				return "", errors.Errorf("files have conflicting go_package settings, must be the same: %q and %q", genPkgName, name)
			}
			genPkgName = name
		}
	}
	if genPkgName != "" {
		return genPkgName, nil
	}

	// If there is no explicit setting, then check the implicit package name
	// (derived from the protobuf package name) of the files and make sure it's
	// consistent.
	for _, f := range genFiles {
		name, _ := goPackageName(f)
		name = stringutils.CleanIdentifier(name)
		if genPkgName != "" && genPkgName != name {
			return "", errors.Errorf("files have conflicting package names, must be the same or overridden with go_package: %q and %q", genPkgName, name)
		}
		genPkgName = name
	}

	// All the files have the same name, so we're good.
	return genPkgName, nil
}

func (t *twirp) generate(file *descriptor.FileDescriptorProto) *plugin.CodeGeneratorResponse_File {
	resp := new(plugin.CodeGeneratorResponse_File)
	if len(file.Service) == 0 {
		return nil
	}

	t.generateFileHeader(file)

	t.generateImports(file)

	t.generateVersionCheck(file)

	// For each service, generate client stubs and server
	for i, service := range file.Service {
		t.generateService(file, service, i)
	}

	// t.generateClientStructAndMethodConsts(file)

	// t.generateClientConstructor(file)

	// t.generateUtils()

	// t.generateInvokeFunction()

	// t.generateIdentifierFunction()

	// t.generateEncodeFunction()

	// t.generateDecodeFunction()

	resp.Name = proto.String(t.goFileName(file))
	resp.Content = proto.String(t.formattedOutput())
	t.output.Reset()

	t.filesHandled++
	return resp
}

func (t *twirp) generateVersionCheck(file *descriptor.FileDescriptorProto) {
	t.P(`// Version compatibility assertion.`)
	t.P(`// If the constant is not defined in the package, that likely means`)
	t.P(`// the package needs to be updated to work with this generated code.`)
	t.P(`// See https://twitchtv.github.io/twirp/docs/version_matrix.html`)
	t.P(`const _ = `, t.pkgs["twirp"], `.TwirpPackageMinVersion_8_1_0`)
}

func (t *twirp) generateFileHeader(file *descriptor.FileDescriptorProto) {
	t.P("// Code generated by protoc-gen-twirp ", gen.Version, ", DO NOT EDIT.")
	t.P("// source: ", file.GetName())
	t.P()

	comment, err := t.reg.FileComments(file)
	if err == nil && comment.Leading != "" {
		for _, line := range strings.Split(comment.Leading, "\n") {
			if line != "" {
				t.P("// " + strings.TrimPrefix(line, " "))
			}
		}
		t.P()
	}

	t.P(`package `, t.genPkgName)
	t.P()
}

func (t *twirp) generateImports(file *descriptor.FileDescriptorProto) {
	if len(file.Service) == 0 {
		return
	}

	// stdlib imports
	t.P(`import `, t.pkgs["context"], ` "context"`)
	t.P(`import `, t.pkgs["json"], ` "encoding/json"`)
	t.P(`import `, t.pkgs["fmt"], ` "fmt"`)
	t.P()
	// dependency imports
	t.P(`import `, t.pkgs["luratwirp"], ` "github.com/kyawmyintthein/lura-twirp"`)
	t.P(`import `, t.pkgs["config"], ` "github.com/luraproject/lura/config"`)
	t.P(`import `, t.pkgs["logging"], ` "github.com/luraproject/lura/logging"`)
	t.P(`import `, t.pkgs["twirp"], ` "github.com/twitchtv/twirp"`)
	t.P(`import `, t.pkgs["proto"], ` "google.golang.org/protobuf/proto"`)
	t.P()

	// It's legal to import a message and use it as an input or output for a
	// method. Make sure to import the package of any such message. First, dedupe
	// them.
	deps := make(map[string]string) // Map of package name to quoted import path.
	ourImportPath := path.Dir(t.goFileName(file))
	for _, s := range file.Service {
		for _, m := range s.Method {
			defs := []*typemap.MessageDefinition{
				t.reg.MethodInputDefinition(m),
				t.reg.MethodOutputDefinition(m),
			}
			for _, def := range defs {
				importPath, _ := parseGoPackageOption(def.File.GetOptions().GetGoPackage())
				if importPath == "" { // no option go_package
					importPath := path.Dir(t.goFileName(def.File)) // use the dirname of the Go filename as import path
					if importPath == ourImportPath {
						continue
					}
				}

				if substitution, ok := t.importMap[def.File.GetName()]; ok {
					importPath = substitution
				}
				importPath = t.importPrefix + importPath

				pkg := t.goPackageName(def.File)
				if pkg != t.genPkgName {
					deps[pkg] = strconv.Quote(importPath)
				}
			}
		}
	}
	pkgs := make([]string, 0, len(deps))
	for pkg := range deps {
		pkgs = append(pkgs, pkg)
	}
	sort.Strings(pkgs)
	for _, pkg := range pkgs {
		t.P(`import `, pkg, ` `, deps[pkg])
	}
	if len(deps) > 0 {
		t.P()
	}
}

// P forwards to g.gen.P, which prints output.
func (t *twirp) P(args ...string) {
	for _, v := range args {
		t.output.WriteString(v)
	}
	t.output.WriteByte('\n')
}

// Big header comments to makes it easier to visually parse a generated file.
func (t *twirp) sectionComment(sectionTitle string) {
	t.P()
	t.P(`// `, strings.Repeat("=", len(sectionTitle)))
	t.P(`// `, sectionTitle)
	t.P(`// `, strings.Repeat("=", len(sectionTitle)))
	t.P()
}

func (t *twirp) generateService(file *descriptor.FileDescriptorProto, service *descriptor.ServiceDescriptorProto, index int) {
	servName := serviceNameCamelCased(service)

	t.sectionComment(servName + ` Lura Client`)
	t.generateLuraClient(file, service)

	t.sectionComment(servName + ` Methods`)
	t.generateTwirpMethods(file, service)

	t.sectionComment(`New` + servName + `LuraClient ` + `creates a Protobuf client that implements the ` + servName + ` interface.`)
	t.generateLuraConstructor(file, service)

	t.sectionComment(servName + ` getBaseURLByClientID`)
	t.generateUtils(file, service)

	t.sectionComment(`Invoke invoke RPC function regarding given service and method.`)
	t.generateInvokeFunction(file, service)

	t.sectionComment(`Identifier return client identifier to lura-twirp backend registery`)
	t.generateIdentifierFunction(file, service)

	t.sectionComment(`Encode convert JSON to proto.Message`)
	t.generateEncodeFunction(file, service)

	t.sectionComment(`Decode convert proto.Message to JSON`)
	t.generateDecodeFunction(file, service)
}

func (t *twirp) generateLuraClient(file *descriptor.FileDescriptorProto, service *descriptor.ServiceDescriptorProto) {
	servName := serviceNameCamelCased(service)
	unexportedServName := unexported(servName)
	comments, err := t.reg.ServiceComments(file, service)
	if err == nil {
		t.printComments(comments)
	}
	t.P(`type `, unexportedServName, `LuraClient`, ` struct {`)
	t.P(`  id string`)
	t.P(`  service `, servName)
	t.P(`  l logging.Logger`)
	t.P(`}`)
}

func (t *twirp) generateTwirpMethods(file *descriptor.FileDescriptorProto, service *descriptor.ServiceDescriptorProto) {
	servName := serviceNameCamelCased(service)

	comments, err := t.reg.ServiceComments(file, service)
	if err == nil {
		t.printComments(comments)
	}
	t.P(`const (`)
	for _, method := range service.Method {
		comments, err = t.reg.MethodComments(file, service, method)
		if err == nil {
			t.printComments(comments)
		}
		t.P(t.generateSignature(servName, method))
	}
	t.P(`)`)
}

func (t *twirp) generateSignature(servName string, method *descriptor.MethodDescriptorProto) string {
	methName := methodNameCamelCased(method)
	return fmt.Sprintf(`  _%sMethod_%s = "%s"`, servName, methName, methName)
}

func (t *twirp) generateLuraConstructor(file *descriptor.FileDescriptorProto, service *descriptor.ServiceDescriptorProto) {
	servName := serviceNameCamelCased(service)
	unexportedServName := unexported(servName)
	comments, err := t.reg.ServiceComments(file, service)
	if err == nil {
		t.printComments(comments)
	}
	t.P(`func New`, servName, `LuraClient`, `(config *config.ServiceConfig, id string, client HTTPClient, l logging.Logger, opts ...twirp.ClientOption) (luratwirp.LuraTwirpStub, error){`)
	t.P(`  baseURL, err := getBaseURLBy` + servName + `ClientID(config)`)
	t.P(`  if err != nil{`)
	t.P(`    return nil, err`)
	t.P(`  }`)
	t.P(`  protobufClient := New`, servName+`ProtobufClient(baseURL, client, opts...)`)
	t.P(`  return &` + unexportedServName + `LuraClient{`)
	t.P(`    id: id,`)
	t.P(`    service: protobufClient,`)
	t.P(`    l: l,`)
	t.P(`  }, nil`)
	t.P(`}`)
}

func (t *twirp) generateUtils(file *descriptor.FileDescriptorProto, service *descriptor.ServiceDescriptorProto) {
	servName := serviceNameCamelCased(service)
	t.P(`func getBaseURLBy` + servName + `ClientID(config *config.ServiceConfig) (string, error) {`)
	t.P(`  for _, endpoint := range config.Endpoints {`)
	t.P(`    _, ok := endpoint.ExtraConfig[luratwirp.TwirpServiceIdentifierConst].(string)`)
	t.P(`    if ok {`)
	t.P(`      for _, backend := range endpoint.Backend {`)
	t.P(`        _, ok := backend.ExtraConfig[luratwirp.TwirpServiceIdentifierConst].(string)`)
	t.P(`        if ok {`)
	t.P(`          if len(backend.Host) <= 0 {`)
	t.P(`            return "", twirp.InternalError("invalid host configuration")`)
	t.P(`          }`)
	t.P(`        }`)
	t.P(`        return backend.Host[0], nil`)
	t.P(`      }`)
	t.P(`    }`)
	t.P(`  }`)
	t.P(`  return "", twirp.InternalError(fmt.Sprintf("invalid %s", luratwirp.TwirpServiceIdentifierConst))`)
	t.P(`}`)
}

func (t *twirp) generateInvokeFunction(file *descriptor.FileDescriptorProto, service *descriptor.ServiceDescriptorProto) {
	servName := serviceNameCamelCased(service)
	unexportedServName := unexported(servName)
	t.P(`func (c *` + unexportedServName + `LuraClient) Invoke(ctx context.Context, service string, method string, in proto.Message) (proto.Message, error) {`)
	t.P(`  switch method {`)
	for _, method := range service.Method {
		comments, err := t.reg.MethodComments(file, service, method)
		if err == nil {
			t.printComments(comments)
		}
		inputType := t.goTypeName(method.GetInputType())
		methName := methodNameCamelCased(method)
		methodSignature := fmt.Sprintf(`_%sMethod_%s`, servName, methName)
		//outputType := t.goTypeName(method.GetOutputType())
		t.P(`  case ` + methodSignature + `:`)
		t.P(`  req, ok := in.(*` + inputType + `)`)
		t.P(`  if !ok {`)
		t.P(`    return nil, twirp.InternalError("invalid protobuf message")`)
		t.P(`  }`)
		t.P(`  resp, err := c.service.` + servName + `(ctx, req)`)
		t.P(`  if err != nil {`)
		t.P(`    c.l.Error(err, "failed to invoke : ",` + methodSignature + `)`)
		t.P(`    return resp, err`)
		t.P(`  }`)
	}
	t.P(`  }`)
	t.P(`  return nil, twirp.InternalError(fmt.Sprintf("invalid %s", luratwirp.TwirpServiceIdentifierConst))`)
	t.P(`}`)
}

func (t *twirp) generateIdentifierFunction(file *descriptor.FileDescriptorProto, service *descriptor.ServiceDescriptorProto) {
	servName := serviceNameCamelCased(service)
	unexportedServName := unexported(servName)
	t.P(`func (c *` + unexportedServName + `LuraClient) Identifier() string {`)
	t.P(`  return c.id`)
	t.P(`}`)
}

func (t *twirp) generateEncodeFunction(file *descriptor.FileDescriptorProto, service *descriptor.ServiceDescriptorProto) {
	servName := serviceNameCamelCased(service)
	unexportedServName := unexported(servName)
	t.P(`func (c *` + unexportedServName + `LuraClient) Encode(ctx context.Context, method string, data []byte) (proto.Message, error) {`)
	t.P(`  switch method {`)
	for _, method := range service.Method {
		comments, err := t.reg.MethodComments(file, service, method)
		if err == nil {
			t.printComments(comments)
		}
		inputType := t.goTypeName(method.GetInputType())
		methName := methodNameCamelCased(method)
		methodSignature := fmt.Sprintf(`_%sMethod_%s`, servName, methName)
		//outputType := t.goTypeName(method.GetOutputType())
		t.P(`  case ` + methodSignature + `:`)
		t.P(`  out := new(` + inputType + `)`)
		t.P(`  err := json.Unmarshal(data, out)`)
		t.P(`  if err != nil {`)
		t.P(`    c.l.Error(err, "failed to unmarhsal : ",` + methodSignature + `)`)
		t.P(`    return out, err`)
		t.P(`  }`)
	}
	t.P(`  }`)
	t.P(`  return nil, twirp.InternalError(fmt.Sprintf("invalid %s", luratwirp.TwirpServiceIdentifierConst))`)
	t.P(`}`)
}

func (t *twirp) generateDecodeFunction(file *descriptor.FileDescriptorProto, service *descriptor.ServiceDescriptorProto) {
	servName := serviceNameCamelCased(service)
	unexportedServName := unexported(servName)
	t.P(`func (c *` + unexportedServName + `LuraClient) Decode(ctx context.Context, method string, msg proto.Message) ([]byte, error) {`)
	t.P(`  switch method {`)
	for _, method := range service.Method {
		comments, err := t.reg.MethodComments(file, service, method)
		if err == nil {
			t.printComments(comments)
		}
		methName := methodNameCamelCased(method)
		methodSignature := fmt.Sprintf(`_%sMethod_%s`, servName, methName)
		//outputType := t.goTypeName(method.GetOutputType())
		t.P(`  case ` + methodSignature + `:`)
		t.P(`  out, err := proto.Marshal(msg)`)
		t.P(`  if err != nil {`)
		t.P(`    c.l.Error(err, "failed to marshal : ",` + methodSignature + `)`)
		t.P(`    return out, err`)
		t.P(`  }`)
	}
	t.P(`  }`)
	t.P(`  return nil, twirp.InternalError(fmt.Sprintf("invalid %s", luratwirp.TwirpServiceIdentifierConst))`)
	t.P(`}`)
}

func (t *twirp) printComments(comments typemap.DefinitionComments) bool {
	text := strings.TrimSuffix(comments.Leading, "\n")
	if len(strings.TrimSpace(text)) == 0 {
		return false
	}
	split := strings.Split(text, "\n")
	for _, line := range split {
		t.P("// ", strings.TrimPrefix(line, " "))
	}
	return len(split) > 0
}

// Given a protobuf name for a Message, return the Go name we will use for that
// type, including its package prefix.
func (t *twirp) goTypeName(protoName string) string {
	def := t.reg.MessageDefinition(protoName)
	if def == nil {
		gen.Fail("could not find message for", protoName)
	}

	var prefix string
	if pkg := t.goPackageName(def.File); pkg != t.genPkgName {
		prefix = pkg + "."
	}

	var name string
	for _, parent := range def.Lineage() {
		name += stringutils.CamelCase(parent.Descriptor.GetName()) + "_"
	}
	name += stringutils.CamelCase(def.Descriptor.GetName())
	return prefix + name
}

func (t *twirp) goPackageName(file *descriptor.FileDescriptorProto) string {
	return t.fileToGoPackageName[file]
}

func (t *twirp) formattedOutput() string {
	// Reformat generated code.
	fset := token.NewFileSet()
	raw := t.output.Bytes()
	ast, err := parser.ParseFile(fset, "", raw, parser.ParseComments)
	if err != nil {
		// Print out the bad code with line numbers.
		// This should never happen in practice, but it can while changing generated code,
		// so consider this a debugging aid.
		var src bytes.Buffer
		s := bufio.NewScanner(bytes.NewReader(raw))
		for line := 1; s.Scan(); line++ {
			fmt.Fprintf(&src, "%5d\t%s\n", line, s.Bytes())
		}
		gen.Fail("bad Go source code was generated:", err.Error(), "\n"+src.String())
	}

	out := bytes.NewBuffer(nil)
	err = (&printer.Config{Mode: printer.TabIndent | printer.UseSpaces, Tabwidth: 8}).Fprint(out, fset, ast)
	if err != nil {
		gen.Fail("generated Go source code could not be reformatted:", err.Error())
	}

	return out.String()
}

func unexported(s string) string { return strings.ToLower(s[:1]) + s[1:] }

func pkgName(file *descriptor.FileDescriptorProto) string {
	return file.GetPackage()
}

func serviceNameCamelCased(service *descriptor.ServiceDescriptorProto) string {
	return stringutils.CamelCase(service.GetName())
}

func serviceNameLiteral(service *descriptor.ServiceDescriptorProto) string {
	return service.GetName()
}

func pkgServiceNameCamelCased(file *descriptor.FileDescriptorProto, service *descriptor.ServiceDescriptorProto) string {
	name := serviceNameCamelCased(service)
	if pkg := pkgName(file); pkg != "" {
		name = pkg + "." + name
	}
	return name
}

func pkgServiceNameLiteral(file *descriptor.FileDescriptorProto, service *descriptor.ServiceDescriptorProto) string {
	name := serviceNameLiteral(service)
	if pkg := pkgName(file); pkg != "" {
		name = pkg + "." + name
	}
	return name
}

func serviceStruct(service *descriptor.ServiceDescriptorProto) string {
	return unexported(serviceNameCamelCased(service)) + "Server"
}

func methodNameCamelCased(method *descriptor.MethodDescriptorProto) string {
	return stringutils.CamelCase(method.GetName())
}

func methodNameLiteral(method *descriptor.MethodDescriptorProto) string {
	return method.GetName()
}

func fileDescSliceContains(slice []*descriptor.FileDescriptorProto, f *descriptor.FileDescriptorProto) bool {
	for _, sf := range slice {
		if f == sf {
			return true
		}
	}
	return false
}
