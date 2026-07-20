//go:build amd64 && !purego && !(gitpack_libdeflate && cgo)

package objstore

// This file audits the *linked* form of the amd64 inflate kernel and makes
// the manual ABI/ISA validation from the fast-loop PR durable. It re-checks,
// on every test run, the claims pinned by the header comment of
// inflate_fast_amd64.s ("ABI0 leaves R14 undefined and its wrapper restores
// g, so R14 holds the input limit. Keep R15 (dynamic-link scratch), BP, SP,
// and X15 (the ABI zero register) untouched.") and by the PR body ("no
// calls, frame mutation, reserved-register writes, or post-v1 opcodes"):
//
//  a. No CALL/SYSCALL/INT-class instructions: the kernel is a
//     NOSPLIT|NOFRAME leaf and must never re-enter Go or the kernel.
//  b. No stack mutation: no PUSH/POP and no write to SP, consistent with
//     TEXT ..., NOSPLIT|NOFRAME, $0-8. The single legal SP reference is the
//     SP-relative *read* that loads the state+0(FP) argument.
//  c. Reserved registers R15, BP, and X15 are never referenced at all.
//     This is deliberately stricter than "never written": the current
//     kernel has no reason even to read them (R15 may hold linker scratch
//     under -dynlink, BP is the caller's frame pointer, X15 must stay the
//     ABIInternal zero), so any new reference is a contract change that
//     must be reviewed, not silently accepted. R14 is exempt: ABI0 leaves
//     it undefined and the Go wrapper restores g, so the kernel owns it
//     (it holds the input limit).
//  d. GOAMD64=v1 ISA only, enforced as an explicit mnemonic allowlist
//     derived from the actual current disassembly. Any mnemonic outside the
//     list (ANDN/SHLX/BZHI, POPCNT/TZCNT, anything VEX-encoded, ...) fails
//     until a human either fixes the .s or extends the allowlist with a
//     justification comment.
//
// Operand-order contract: `go tool objdump` prints Go/Plan-9-flavored
// operands where the DESTINATION IS THE LAST OPERAND (verified against the
// real output: the argument load renders as "MOVQ 0x8(SP), R12" and the
// input-limit store as "MOVQ CX, R14"). Read-modify-write mnemonics
// (ADDQ/ORQ/SHRQ/...) both read and write their final operand; treating the
// final operand as written is therefore conservative and correct. Mnemonics
// that write no explicit operand (CMP*/TEST*/Jcc/RET/NOP*) are excluded from
// that model, and mnemonics with several or implicit destinations (XCHG,
// MULQ, RDTSC, ...) are absent from the allowlist, so they can never reach
// the single-destination write model unaudited.
//
// External vs internal linking byte-identity (the remaining PR claim) is
// intentionally NOT asserted here: it needs two full test-binary builds plus
// a C toolchain for -linkmode=external, which is too heavy and too
// environment-dependent for a unit test. It lives in the `linkaudit-external`
// Makefile target instead, which builds both link modes, disassembles this
// same symbol, and diffs the (file:line, opcode bytes) columns.

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// amd64LinkAuditSymbolPattern matches the kernel's symbol in the linked test
// binary. The linker decorates the ABI0 assembly symbol as
// "github.com/ahrav/go-gitpack.inflateHuffmanFastAMD64.abi0" today; accept
// the undecorated spelling too so an ABI-wrapper policy change in a future
// toolchain reads as "symbol found" rather than "kernel dead-stripped".
const amd64LinkAuditSymbolPattern = `inflateHuffmanFastAMD64(\.abi0)?$`

// amd64LinkAuditAllowedMnemonics is the ISA allowlist for the kernel,
// derived from the actual `go tool objdump` disassembly of
// inflate_fast_amd64.s (go1.25, GOAMD64=v1). Everything here is baseline
// x86-64: plain integer moves and ALU ops, SSE2 GP<->XMM moves, conditional
// jumps, and the NOP padding the assembler materializes for PCALIGN.
//
// If the audit reports a mnemonic missing from this list, either the .s
// grew a post-v1 instruction (fix the .s) or it grew a new v1-legal
// instruction (extend this list WITH a justification comment, as for the
// entries below).
var amd64LinkAuditAllowedMnemonics = map[string]bool{
	// Data movement. MOVQ between GP and X registers (X0/X1 spill slots in
	// the kernel) is SSE2, which every amd64 Go target guarantees at v1.
	"MOVQ": true,
	"MOVL": true,
	"MOVB": true,
	// The .s spells the RLE byte load MOVBQZX; go tool objdump renders it
	// MOVZX on current toolchains. Accept both renderings of the opcode.
	"MOVZX":   true,
	"MOVBQZX": true,
	// Integer ALU.
	"ADDQ":  true,
	"SUBQ":  true,
	"ANDQ":  true,
	"ORQ":   true,
	"XORQ":  true,
	"NOTQ":  true,
	"INCQ":  true,
	"IMULQ": true, // two-operand form; the RLE 0x0101...01 splat multiply
	// Shifts. Variable shifts use CL modulo 64 (see the .s header comment).
	"SHLQ": true,
	"SHRQ": true,
	// Compare/test: write flags only, no register/memory destination.
	"CMPQ":  true,
	"TESTL": true,
	// Control flow inside the kernel.
	"JMP": true,
	"JA":  true,
	"JAE": true,
	"JB":  true,
	"JE":  true,
	"JNE": true,
	"RET": true,
	// PCALIGN $32 padding: the assembler emits multi-byte NOP forms whose
	// exact mix depends on the gap size and toolchain version.
	"NOP":  true,
	"NOPL": true,
	"NOPW": true,
}

// amd64LinkAuditReservedRegs are the registers the kernel must never
// reference, read or written, per the inflate_fast_amd64.s header comment.
// go tool objdump prints 64-bit GP registers without the R prefix ("BP",
// "SP"), but RBP is kept as a defensive alias in case a disassembler
// revision switches spelling. R14 is deliberately absent: the kernel owns
// it under ABI0 (input limit).
var amd64LinkAuditReservedRegs = map[string]string{
	"R15": "dynamic-link scratch",
	"BP":  "caller frame pointer",
	"RBP": "caller frame pointer",
	"X15": "ABIInternal zero register",
}

// amd64LinkAuditStackMnemonics are mnemonics that implicitly mutate SP.
// This is an exact set, not a prefix match: POPCNT* must fall through to
// the ISA allowlist (post-v1) rather than be misreported as a stack pop.
var amd64LinkAuditStackMnemonics = map[string]bool{
	"PUSHQ": true, "PUSHL": true, "PUSHW": true, "PUSHFQ": true,
	"POPQ": true, "POPL": true, "POPW": true, "POPFQ": true,
	"ENTER": true, "LEAVE": true,
}

// amd64LinkAuditInsn is one disassembled instruction from `go tool objdump`.
type amd64LinkAuditInsn struct {
	loc      string   // "inflate_fast_amd64.s:37"
	addr     string   // "0x811096"
	hex      string   // "4989ce"
	text     string   // "MOVQ CX, R14"
	mnemonic string   // "MOVQ"
	operands []string // ["CX", "R14"]
}

// amd64LinkAuditBlock is one TEXT symbol's disassembly.
type amd64LinkAuditBlock struct {
	symbol string // "github.com/ahrav/go-gitpack.inflateHuffmanFastAMD64.abi0(SB)"
	file   string // source path printed after the symbol
	insns  []amd64LinkAuditInsn
}

// amd64ParseLinkAuditObjdump parses `go tool objdump -s` output. Each block
// starts with "TEXT <symbol>(SB) <file>"; instruction lines are
// tab-separated "  file:line\t0xADDR\tHEXBYTES\tMNEMONIC operands" (the
// column separator is one OR MORE tabs, so empty fields are dropped).
func amd64ParseLinkAuditObjdump(out string) ([]amd64LinkAuditBlock, error) {
	var blocks []amd64LinkAuditBlock
	for _, line := range strings.Split(out, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "TEXT ") {
			rest := strings.TrimPrefix(trimmed, "TEXT ")
			symbol, file, _ := strings.Cut(rest, " ")
			blocks = append(blocks, amd64LinkAuditBlock{
				symbol: symbol,
				file:   strings.TrimSpace(file),
			})
			continue
		}
		if len(blocks) == 0 {
			return nil, fmt.Errorf("objdump instruction line before any TEXT header: %q", line)
		}
		var fields []string
		for _, f := range strings.Split(line, "\t") {
			if f = strings.TrimSpace(f); f != "" {
				fields = append(fields, f)
			}
		}
		if len(fields) < 4 {
			return nil, fmt.Errorf("unparseable objdump line (want file:line, address, opcode bytes, instruction): %q", line)
		}
		// Tolerate a disassembler that tabs between mnemonic and operands.
		text := strings.Join(fields[3:], " ")
		mnemonic, rest, _ := strings.Cut(text, " ")
		var operands []string
		if rest != "" {
			operands = strings.Split(rest, ", ")
		}
		b := &blocks[len(blocks)-1]
		b.insns = append(b.insns, amd64LinkAuditInsn{
			loc:      fields[0],
			addr:     fields[1],
			hex:      fields[2],
			text:     text,
			mnemonic: mnemonic,
			operands: operands,
		})
	}
	return blocks, nil
}

// amd64SelectLinkAuditKernelInsns picks the kernel's instructions out of the
// parsed blocks. A block qualifies when its symbol names the kernel AND
// every instruction is attributed to inflate_fast_amd64.s — the latter
// excludes compiler-generated ABI wrappers or similarly named symbols. An
// empty result is itself a finding: the kernel was dead-stripped, renamed,
// or moved out of inflate_fast_amd64.s.
func amd64SelectLinkAuditKernelInsns(blocks []amd64LinkAuditBlock) ([]amd64LinkAuditInsn, error) {
	var insns []amd64LinkAuditInsn
	for _, b := range blocks {
		if !strings.Contains(b.symbol, "inflateHuffmanFastAMD64") {
			continue
		}
		if len(b.insns) == 0 {
			return nil, fmt.Errorf("kernel symbol %s disassembled to zero instructions", b.symbol)
		}
		fromKernelSource := true
		for _, insn := range b.insns {
			if !strings.HasPrefix(insn.loc, "inflate_fast_amd64.s:") {
				fromKernelSource = false
				break
			}
		}
		if !fromKernelSource {
			continue
		}
		insns = append(insns, b.insns...)
	}
	if len(insns) == 0 {
		return nil, fmt.Errorf(
			"no disassembly for inflateHuffmanFastAMD64 sourced from inflate_fast_amd64.s "+
				"(%d TEXT blocks scanned): the kernel was dead-stripped, renamed, or moved",
			len(blocks))
	}
	return insns, nil
}

// amd64LinkAuditWritesFinalOperand reports whether a mnemonic writes its
// final (Go/Plan-9 destination-position) operand. Valid only for mnemonics
// that can appear in the audit: every allowlisted mnemonic either writes
// exactly its final operand (including the read-modify-write ALU ops, which
// also read it — conservative for write detection) or writes none.
// Mnemonics with several or implicit destinations are kept off the
// allowlist so this model is never applied to them silently.
func amd64LinkAuditWritesFinalOperand(mnemonic string) bool {
	switch {
	case strings.HasPrefix(mnemonic, "CMP"),
		strings.HasPrefix(mnemonic, "TEST"),
		strings.HasPrefix(mnemonic, "J"),
		strings.HasPrefix(mnemonic, "NOP"),
		mnemonic == "RET":
		return false
	}
	return true
}

// amd64LinkAuditOperandTokens splits an operand into register-name-shaped
// tokens (maximal runs of ASCII letters and digits), so "0x8(SP)" yields
// ["0x8" "SP"] and "0(R10)(CX*4)" yields ["0" "R10" "CX" "4"].
func amd64LinkAuditOperandTokens(operand string) []string {
	var tokens []string
	start := -1
	for i := 0; i <= len(operand); i++ {
		var word bool
		if i < len(operand) {
			c := operand[i]
			word = c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9'
		}
		switch {
		case word && start < 0:
			start = i
		case !word && start >= 0:
			tokens = append(tokens, operand[start:i])
			start = -1
		}
	}
	return tokens
}

func amd64LinkAuditOperandUsesSP(operand string) bool {
	for _, tok := range amd64LinkAuditOperandTokens(operand) {
		if tok == "SP" || tok == "RSP" {
			return true
		}
	}
	return false
}

// amd64LinkAuditViolations is the pure checker over parsed instructions: it
// returns one message per contract violation, each quoting the offending
// disassembly line. It is deliberately side-effect free so the synthetic
// negative tests below can drive it with doctored input.
func amd64LinkAuditViolations(insns []amd64LinkAuditInsn) []string {
	var violations []string
	report := func(insn amd64LinkAuditInsn, format string, args ...any) {
		violations = append(violations, fmt.Sprintf(
			"%s: %s: %q", insn.loc, fmt.Sprintf(format, args...), insn.text))
	}
	for _, insn := range insns {
		m := insn.mnemonic

		// (a) No calls or kernel entries: NOSPLIT|NOFRAME leaf contract.
		if strings.HasPrefix(m, "CALL") || strings.HasPrefix(m, "LCALL") ||
			m == "SYSCALL" || m == "SYSENTER" || strings.HasPrefix(m, "INT") {
			report(insn, "call/kernel-entry instruction forbidden in the NOSPLIT|NOFRAME leaf kernel")
		}

		// (b) No implicit stack mutation.
		if amd64LinkAuditStackMnemonics[m] {
			report(insn, "stack push/pop mutates SP in a NOFRAME $0-8 function")
		}

		writes := amd64LinkAuditWritesFinalOperand(m)
		for i, op := range insn.operands {
			opWritten := writes && i == len(insn.operands)-1

			// (b) SP policy: the only legal SP reference is an SP-relative
			// memory READ (the state+0(FP) argument load renders as
			// "MOVQ 0x8(SP), R12"). Bare-register SP in any position and
			// any store through SP are violations.
			if amd64LinkAuditOperandUsesSP(op) {
				switch {
				case !strings.Contains(op, "("):
					report(insn, "SP used as a data register (read or write) in a NOFRAME $0-8 function")
				case opWritten:
					report(insn, "store through SP; the NOFRAME kernel owns no stack slots")
				}
			}

			// (c) Reserved registers: any reference at all.
			for _, tok := range amd64LinkAuditOperandTokens(op) {
				if why, reserved := amd64LinkAuditReservedRegs[tok]; reserved {
					pos := "read"
					if opWritten {
						pos = "written"
					}
					report(insn, "reserved register %s %s (%s; see the inflate_fast_amd64.s header comment)",
						tok, pos, why)
				}
			}
		}

		// (d) GOAMD64=v1 ISA allowlist.
		if !amd64LinkAuditAllowedMnemonics[m] {
			report(insn, "mnemonic %s is not on the GOAMD64=v1 allowlist; "+
				"fix inflate_fast_amd64.s or consciously extend "+
				"amd64LinkAuditAllowedMnemonics with a justification comment", m)
		}
	}
	return violations
}

// amd64LinkAuditGoToolFrom is the injectable core of the go-binary lookup:
// it tries <goroot>/bin/go for each candidate GOROOT, then lookPath("go").
// The indirection exists so the skip decision is testable without doctoring
// process-global state (runtime.GOROOT ignores env changes made after
// process start, so a t.Setenv-based test cannot force the miss path).
func amd64LinkAuditGoToolFrom(goroots []string, lookPath func(string) (string, error)) (string, bool) {
	name := "go"
	if runtime.GOOS == "windows" {
		name = "go.exe"
	}
	for _, goroot := range goroots {
		if goroot == "" {
			continue
		}
		p := filepath.Join(goroot, "bin", name)
		if info, err := os.Stat(p); err == nil && !info.IsDir() {
			return p, true
		}
	}
	if p, err := lookPath("go"); err == nil {
		return p, true
	}
	return "", false
}

// amd64LinkAuditGoTool locates the `go` binary needed for `go tool objdump`:
// the GOROOT environment variable, then runtime.GOROOT()/bin/go, then PATH.
func amd64LinkAuditGoTool() (string, bool) {
	return amd64LinkAuditGoToolFrom(
		//nolint:staticcheck // SA1019: runtime.GOROOT() is an intentional process-start fallback candidate; see amd64LinkAuditGoToolFrom.
		[]string{os.Getenv("GOROOT"), runtime.GOROOT()}, exec.LookPath)
}

// amd64LinkAuditDisassembly returns `go tool objdump` output for the kernel
// symbol. It prefers the running test binary (os.Executable), but plain
// `go test` links its transient binary with -s -w, leaving no ELF symbol
// section for objdump to read. In that case the package's test binary is
// rebuilt with `go test -c` (which keeps the symbol table) into a temp dir
// and that artifact is disassembled instead: same package, same build tags,
// same toolchain, so the kernel's linked instruction stream is identical.
// The fallback relies on the test process running with the package source
// directory as its working directory, which `go test` guarantees; a
// manually executed `go test -c` binary never needs the fallback because it
// already has its symbol table.
func amd64LinkAuditDisassembly(t *testing.T, goTool string) string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	out, err := exec.Command(goTool, "tool", "objdump", "-s", amd64LinkAuditSymbolPattern, exe).CombinedOutput()
	if err == nil {
		return string(out)
	}
	if !strings.Contains(string(out), "no symbol section") {
		t.Fatalf("%s tool objdump -s %q %s: %v\n%s", goTool, amd64LinkAuditSymbolPattern, exe, err, out)
	}
	rebuilt := filepath.Join(t.TempDir(), "linkaudit.test")
	if build, err := exec.Command(goTool, "test", "-c", "-vet=off", "-o", rebuilt, ".").CombinedOutput(); err != nil {
		t.Fatalf("rebuilding symbol-ful test binary (%s test -c): %v\n%s", goTool, err, build)
	}
	out, err = exec.Command(goTool, "tool", "objdump", "-s", amd64LinkAuditSymbolPattern, rebuilt).CombinedOutput()
	if err != nil {
		t.Fatalf("%s tool objdump -s %q %s: %v\n%s", goTool, amd64LinkAuditSymbolPattern, rebuilt, err, out)
	}
	return string(out)
}

// TestInflateHuffmanFastAMD64LinkAuditKernel disassembles the kernel out of
// the linked test binary and audits the instruction stream against the
// register and ISA contract documented at the top of this file.
func TestInflateHuffmanFastAMD64LinkAuditKernel(t *testing.T) {
	goTool, ok := amd64LinkAuditGoTool()
	if !ok {
		t.Skip("link audit needs `go tool objdump`; no go binary via GOROOT, runtime.GOROOT(), or PATH")
	}
	blocks, err := amd64ParseLinkAuditObjdump(amd64LinkAuditDisassembly(t, goTool))
	if err != nil {
		t.Fatalf("parsing objdump output: %v", err)
	}
	insns, err := amd64SelectLinkAuditKernelInsns(blocks)
	if err != nil {
		t.Fatal(err)
	}

	// Sanity floor: the kernel currently disassembles to ~305 instructions.
	// Far fewer means the symbol was truncated or the parse went wrong, and
	// a pass over a fragment would be vacuous.
	const minKernelInsns = 50
	if len(insns) < minKernelInsns {
		t.Fatalf("kernel disassembled to only %d instructions (< %d): truncated symbol or parse failure",
			len(insns), minKernelInsns)
	}

	// Parser-integrity canaries. The kernel is KNOWN to (1) write R14 once
	// ("MOVQ CX, R14", the ABI0-sanctioned input limit — see the .s header),
	// (2) load its argument through SP ("MOVQ 0x8(SP), R12" = state+0(FP)),
	// and (3) return. If the audit stops seeing these, the objdump format
	// drifted and every assertion above would be vacuously green.
	r14Writes, spMemReads, rets := 0, 0, 0
	for _, insn := range insns {
		if len(insn.operands) > 0 && amd64LinkAuditWritesFinalOperand(insn.mnemonic) &&
			insn.operands[len(insn.operands)-1] == "R14" {
			r14Writes++
		}
		for i, op := range insn.operands {
			isDest := amd64LinkAuditWritesFinalOperand(insn.mnemonic) && i == len(insn.operands)-1
			if !isDest && strings.Contains(op, "(") && amd64LinkAuditOperandUsesSP(op) {
				spMemReads++
			}
		}
		if insn.mnemonic == "RET" {
			rets++
		}
	}
	if r14Writes == 0 {
		t.Error("canary failed: no write to R14 seen; the kernel stores its input limit there, so operand parsing has drifted")
	}
	if spMemReads == 0 {
		t.Error("canary failed: no SP-relative argument load seen; operand parsing has drifted")
	}
	if rets == 0 {
		t.Error("canary failed: no RET seen; symbol disassembly is truncated")
	}

	for _, v := range amd64LinkAuditViolations(insns) {
		t.Error(v)
	}
}

// TestInflateHuffmanFastAMD64LinkAuditGoToolLookup pins the skip decision
// through the injectable lookup core: with no usable GOROOT candidate and a
// failing PATH lookup, the locator must report "not found" so the audit
// skips with a message instead of failing spuriously. runtime.GOROOT()
// snapshots its value at process start, so the real miss path cannot be
// forced with t.Setenv; injection keeps this deterministic instead.
func TestInflateHuffmanFastAMD64LinkAuditGoToolLookup(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "no-such-goroot")
	lookPathFail := func(string) (string, error) {
		return "", exec.ErrNotFound
	}
	if p, ok := amd64LinkAuditGoToolFrom([]string{"", missing}, lookPathFail); ok {
		t.Fatalf("go tool located at %q despite bogus GOROOT candidates and failing PATH lookup; the skip path is unreachable", p)
	}

	// Positive control: the real environment must resolve a go binary the
	// same way the live audit does, or the skip message would be hiding a
	// lookup bug rather than a genuinely absent toolchain.
	if _, ok := amd64LinkAuditGoTool(); !ok {
		t.Log("no go binary found in this environment; the live audit will skip")
	}
}

// amd64LinkAuditDoctoredLine renders one synthetic objdump instruction line
// in the real tab-separated format.
func amd64LinkAuditDoctoredLine(srcLine int, text string) string {
	return fmt.Sprintf("  inflate_fast_amd64.s:%d\t0x811040\t\t90\t\t%s\t\t", srcLine, text)
}

// amd64LinkAuditDoctoredObjdump builds a minimal syntactically faithful
// objdump dump for the kernel symbol: a clean prologue (including the two
// deliberate edge cases — the SP-relative argument load and the sanctioned
// R14 write, which must NOT be flagged), any doctored extra instructions,
// and a RET.
func amd64LinkAuditDoctoredObjdump(extra ...string) string {
	lines := []string{
		"TEXT github.com/ahrav/go-gitpack.inflateHuffmanFastAMD64.abi0(SB) /doctored/inflate_fast_amd64.s",
		amd64LinkAuditDoctoredLine(13, "MOVQ 0x8(SP), R12"),
		amd64LinkAuditDoctoredLine(37, "MOVQ CX, R14"),
		amd64LinkAuditDoctoredLine(51, "ADDQ CX, R8"),
		amd64LinkAuditDoctoredLine(58, "NOPL 0(AX)"),
		amd64LinkAuditDoctoredLine(33, "MOVQ R13, X1"),
	}
	for i, text := range extra {
		lines = append(lines, amd64LinkAuditDoctoredLine(900+i, text))
	}
	lines = append(lines, amd64LinkAuditDoctoredLine(204, "RET"))
	return strings.Join(lines, "\n") + "\n"
}

// TestInflateHuffmanFastAMD64LinkAuditCheckerNegatives proves each assertion
// class actually fires, by feeding doctored objdump text through the same
// parse/select/audit pipeline the live test uses. These are pure-function
// tests: they need no binary and no go tool.
func TestInflateHuffmanFastAMD64LinkAuditCheckerNegatives(t *testing.T) {
	audit := func(t *testing.T, dump string) []string {
		t.Helper()
		blocks, err := amd64ParseLinkAuditObjdump(dump)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		insns, err := amd64SelectLinkAuditKernelInsns(blocks)
		if err != nil {
			t.Fatalf("select: %v", err)
		}
		return amd64LinkAuditViolations(insns)
	}

	t.Run("clean baseline audits clean", func(t *testing.T) {
		if v := audit(t, amd64LinkAuditDoctoredObjdump()); len(v) != 0 {
			t.Fatalf("clean doctored dump (incl. the sanctioned R14 write and SP argument load) must audit clean, got:\n%s",
				strings.Join(v, "\n"))
		}
	})

	tests := []struct {
		name string
		insn string
		want string // substring required in the violations
		veto string // substring that must NOT appear (misclassification guard)
	}{
		{name: "call", insn: "CALL 0x40a2e0", want: "call/kernel-entry"},
		{name: "syscall", insn: "SYSCALL", want: "call/kernel-entry"},
		{name: "int3", insn: "INT $0x3", want: "call/kernel-entry"},
		{name: "vex encoded post-v1", insn: "VPAND X1, X2, X3", want: "not on the GOAMD64=v1 allowlist"},
		{name: "bmi2 shift", insn: "SHLXQ CX, AX, BX", want: "not on the GOAMD64=v1 allowlist"},
		{
			name: "popcnt is an ISA violation not a stack pop",
			insn: "POPCNTQ AX, BX",
			want: "not on the GOAMD64=v1 allowlist",
			veto: "stack push/pop",
		},
		{name: "reserved R15 write", insn: "MOVQ AX, R15", want: "reserved register R15 written"},
		{name: "reserved BP read", insn: "MOVQ BP, AX", want: "reserved register BP read"},
		{name: "reserved X15 write", insn: "MOVQ X0, X15", want: "reserved register X15 written"},
		{name: "reserved R15 as memory base", insn: "MOVQ 0(R15), AX", want: "reserved register R15 read"},
		{name: "push", insn: "PUSHQ AX", want: "stack push/pop"},
		{name: "sp arithmetic", insn: "ADDQ $0x20, SP", want: "SP used as a data register"},
		{name: "sp read into gp", insn: "MOVQ SP, CX", want: "SP used as a data register"},
		{name: "store through sp", insn: "MOVQ R12, 0x8(SP)", want: "store through SP"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := audit(t, amd64LinkAuditDoctoredObjdump(tt.insn))
			if len(v) == 0 {
				t.Fatalf("doctored instruction %q produced no violation", tt.insn)
			}
			joined := strings.Join(v, "\n")
			if !strings.Contains(joined, tt.want) {
				t.Errorf("violations for %q lack %q:\n%s", tt.insn, tt.want, joined)
			}
			if !strings.Contains(joined, tt.insn) {
				t.Errorf("violations for %q do not quote the offending line:\n%s", tt.insn, joined)
			}
			if tt.veto != "" && strings.Contains(joined, tt.veto) {
				t.Errorf("violations for %q misclassified (contain %q):\n%s", tt.insn, tt.veto, joined)
			}
		})
	}
}

// TestInflateHuffmanFastAMD64LinkAuditSymbolSelection pins the
// missing/renamed-symbol findings: an absent symbol, an empty body, and a
// symbol whose instructions come from the wrong source file must all be
// reported as errors rather than auditing an empty stream to green.
func TestInflateHuffmanFastAMD64LinkAuditSymbolSelection(t *testing.T) {
	t.Run("no output means dead-stripped or renamed", func(t *testing.T) {
		blocks, err := amd64ParseLinkAuditObjdump("")
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if _, err := amd64SelectLinkAuditKernelInsns(blocks); err == nil {
			t.Fatal("selecting from empty objdump output must fail")
		}
	})

	t.Run("empty symbol body is a finding", func(t *testing.T) {
		dump := "TEXT github.com/ahrav/go-gitpack.inflateHuffmanFastAMD64.abi0(SB) /doctored/inflate_fast_amd64.s\n"
		blocks, err := amd64ParseLinkAuditObjdump(dump)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if _, err := amd64SelectLinkAuditKernelInsns(blocks); err == nil {
			t.Fatal("an empty kernel body must fail selection")
		}
	})

	t.Run("kernel moved out of inflate_fast_amd64.s is a finding", func(t *testing.T) {
		dump := strings.Join([]string{
			"TEXT github.com/ahrav/go-gitpack.inflateHuffmanFastAMD64.abi0(SB) /doctored/other_file.s",
			"  other_file.s:10\t0x811040\t\t90\t\tRET\t\t",
		}, "\n") + "\n"
		blocks, err := amd64ParseLinkAuditObjdump(dump)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if _, err := amd64SelectLinkAuditKernelInsns(blocks); err == nil {
			t.Fatal("a kernel sourced from another file must fail selection")
		}
	})
}
