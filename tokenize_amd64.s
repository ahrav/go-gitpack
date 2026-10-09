//go:build amd64 && !purego && !(gitpack_libdeflate && cgo)

#include "textflag.h"

// func newlineMask64(p *byte) uint64
// Four 16-byte SSE2 compares against a '\n' pattern; PMOVMSKB folds each
// compare into 16 mask bits and the four halves are shifted into one word.
TEXT ·newlineMask64(SB), NOSPLIT|NOFRAME, $0-16
	MOVQ	p+0(FP), SI
	MOVQ	$0x0a0a0a0a0a0a0a0a, AX
	MOVQ	AX, X4
	PUNPCKLQDQ	X4, X4
	MOVOU	(SI), X0
	MOVOU	16(SI), X1
	MOVOU	32(SI), X2
	MOVOU	48(SI), X3
	PCMPEQB	X4, X0
	PCMPEQB	X4, X1
	PCMPEQB	X4, X2
	PCMPEQB	X4, X3
	PMOVMSKB	X0, AX
	PMOVMSKB	X1, BX
	PMOVMSKB	X2, CX
	PMOVMSKB	X3, DX
	SHLQ	$16, BX
	SHLQ	$32, CX
	SHLQ	$48, DX
	ORQ	BX, AX
	ORQ	CX, AX
	ORQ	DX, AX
	MOVQ	AX, ret+8(FP)
	RET
