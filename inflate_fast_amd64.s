//go:build amd64 && !purego && !(gitpack_libdeflate && cgo)

#include "go_asm.h"
#include "textflag.h"

// func inflateHuffmanFastAMD64(state *inflateFastState)
// Baseline GOAMD64=v1. ABI0 leaves R14 undefined and its wrapper restores g,
// so R14 holds the input limit. Keep R15 (dynamic-link scratch), BP, SP, and
// X15 (the ABI zero register) untouched.
// Variable shifts use CL modulo 64. Mask counts only when the full value also
// updates nbits.
TEXT ·inflateHuffmanFastAMD64(SB), NOSPLIT|NOFRAME, $0-8
	MOVQ	state+0(FP), R12

	MOVQ	inflateFastState_src(R12), R8
	ADDQ	inflateFastState_pos(R12), R8
	MOVQ	inflateFastState_dst(R12), R9
	ADDQ	inflateFastState_out(R12), R9
	MOVQ	inflateFastState_litlen(R12), R10
	MOVQ	inflateFastState_litlenMask(R12), R11
	MOVQ	inflateFastState_bitbuf(R12), AX
	MOVQ	inflateFastState_nbits(R12), DX

	// X1 holds min(outLimit, yieldAt) as an absolute output pointer.
	MOVQ	inflateFastState_dst(R12), R13
	ADDQ	inflateFastState_outLimit(R12), R13
	MOVQ	inflateFastState_dst(R12), CX
	ADDQ	inflateFastState_yieldAt(R12), CX
	CMPQ	CX, R13
	JAE	out_stop_ready
	MOVQ	CX, R13
out_stop_ready:
	MOVQ	R13, X1

	MOVQ	inflateFastState_src(R12), CX
	ADDQ	inflateFastState_inLimit(R12), CX
	MOVQ	CX, R14
	CMPQ	R8, CX
	JAE	return_boundary
	CMPQ	R9, R13
	JAE	return_boundary

	// Branchless refill. The Go wrapper's input margin proves this load.
	// CX survives the data shift, and nbits < 64 makes the byte count <= 7.
	MOVQ	(R8), SI
	MOVQ	DX, CX
	SHLQ	CX, SI
	ORQ	SI, AX
	XORQ	$56, CX
	SHRQ	$3, CX
	ADDQ	CX, R8
	ORQ	$56, DX

	MOVQ	AX, CX
	ANDQ	R11, CX
	MOVL	(R10)(CX*4), BX

	PCALIGN $32
decode_entry:
	MOVQ	AX, SI
	MOVQ	BX, CX
	ANDQ	$0x3f, CX
	SHRQ	CX, AX
	SUBQ	CX, DX
	TESTL	$0x80000000, BX
	JNZ	literal_one
	TESTL	$0x8000, BX
	JNZ	litlen_exception

have_length:
	MOVQ	BX, DI
	SHRQ	$16, DI
	MOVQ	$-1, R13
	MOVQ	BX, CX
	SHLQ	CX, R13
	NOTQ	R13
	ANDQ	SI, R13
	MOVQ	BX, CX
	SHRQ	$8, CX
	SHRQ	CX, R13
	ADDQ	R13, DI
	MOVQ	DI, X0

	MOVQ	AX, CX
	ANDQ	inflateFastState_offsetMask(R12), CX
	MOVQ	inflateFastState_offset(R12), R13
	MOVL	(R13)(CX*4), BX
	TESTL	$0x8000, BX
	JNZ	offset_exception

	CMPQ	DX, $const_inflateFastOffsetRefillThreshold
	JA	offset_direct_ready
	MOVQ	(R8), SI
	MOVQ	DX, CX
	SHLQ	CX, SI
	ORQ	SI, AX
	XORQ	$56, CX
	SHRQ	$3, CX
	ADDQ	CX, R8
	ORQ	$56, DX

offset_direct_ready:
	MOVQ	AX, SI
	MOVQ	BX, CX
	ANDQ	$0x3f, CX
	SHRQ	CX, AX
	SUBQ	CX, DX

have_offset:
	MOVQ	BX, DI
	SHRQ	$16, DI
	MOVQ	$-1, R13
	MOVQ	BX, CX
	SHLQ	CX, R13
	NOTQ	R13
	ANDQ	SI, R13
	MOVQ	BX, CX
	SHRQ	$8, CX
	SHRQ	CX, R13
	ADDQ	R13, DI

	MOVQ	R9, CX
	SUBQ	inflateFastState_dst(R12), CX
	CMPQ	CX, DI
	JB	return_bad_data

	// Preload the next entry and refill before issuing match-copy loads.
	MOVQ	AX, CX
	ANDQ	R11, CX
	MOVL	(R10)(CX*4), BX
	MOVQ	(R8), SI
	MOVQ	DX, CX
	SHLQ	CX, SI
	ORQ	SI, AX
	XORQ	$56, CX
	SHRQ	$3, CX
	ADDQ	CX, R8
	ORQ	$56, DX

	MOVQ	R9, SI
	SUBQ	DI, SI
	MOVQ	R9, CX
	MOVQ	X0, R13
	ADDQ	R13, R9
	CMPQ	DI, $8
	JB	copy_narrow

copy_wide:
	MOVQ	(SI), R13
	MOVQ	R13, (CX)
	MOVQ	8(SI), R13
	MOVQ	R13, 8(CX)
	MOVQ	16(SI), R13
	MOVQ	R13, 16(CX)
	MOVQ	24(SI), R13
	MOVQ	R13, 24(CX)
	MOVQ	32(SI), R13
	MOVQ	R13, 32(CX)
	ADDQ	$40, SI
	ADDQ	$40, CX
	CMPQ	CX, R9
	JB	copy_wide

loop_check:
	CMPQ	R8, R14
	JAE	return_boundary
	MOVQ	X1, R13
	CMPQ	R9, R13
	JB	decode_entry

return_boundary:
	CMPQ	R8, R14
	JAE	return_tail
	MOVQ	inflateFastState_dst(R12), CX
	ADDQ	inflateFastState_yieldAt(R12), CX
	CMPQ	R9, CX
	JAE	return_yield

return_tail:
	MOVL	$const_inflateFastTail, BX
	JMP	store_state

return_block_done:
	MOVL	$const_inflateFastBlockDone, BX
	JMP	store_state

return_bad_data:
	MOVL	$const_inflateFastBadData, BX
	JMP	store_state

return_yield:
	ADDQ	$const_inflateFastYieldBytes, inflateFastState_yieldAt(R12)
	MOVL	$const_inflateFastYield, BX

store_state:
	MOVQ	AX, inflateFastState_bitbuf(R12)
	ANDQ	$0xff, DX
	MOVQ	DX, inflateFastState_nbits(R12)
	SUBQ	inflateFastState_src(R12), R8
	MOVQ	R8, inflateFastState_pos(R12)
	SUBQ	inflateFastState_dst(R12), R9
	MOVQ	R9, inflateFastState_out(R12)
	MOVL	BX, inflateFastState_status(R12)
	RET

literal_one:
	MOVQ	BX, CX
	SHRQ	$16, CX
	MOVB	CX, (R9)
	INCQ	R9
	MOVQ	AX, CX
	ANDQ	R11, CX
	MOVL	(R10)(CX*4), BX
	MOVQ	AX, SI
	MOVQ	BX, CX
	ANDQ	$0x3f, CX
	SHRQ	CX, AX
	SUBQ	CX, DX
	TESTL	$0x80000000, BX
	JNZ	literal_two
	TESTL	$0x8000, BX
	JNZ	litlen_exception
	JMP	have_length

literal_two:
	MOVQ	BX, CX
	SHRQ	$16, CX
	MOVB	CX, (R9)
	INCQ	R9
	MOVQ	AX, CX
	ANDQ	R11, CX
	MOVL	(R10)(CX*4), BX
	MOVQ	AX, SI
	MOVQ	BX, CX
	ANDQ	$0x3f, CX
	SHRQ	CX, AX
	SUBQ	CX, DX
	TESTL	$0x80000000, BX
	JZ	literal_two_nonliteral

	// A third literal completes this iteration. Preload the next entry
	// before refilling so its lookup can overlap the refill load.
	MOVQ	BX, CX
	SHRQ	$16, CX
	MOVB	CX, (R9)
	INCQ	R9
	MOVQ	AX, CX
	ANDQ	R11, CX
	MOVL	(R10)(CX*4), BX
	MOVQ	(R8), SI
	MOVQ	DX, CX
	SHLQ	CX, SI
	ORQ	SI, AX
	XORQ	$56, CX
	SHRQ	$3, CX
	ADDQ	CX, R8
	ORQ	$56, DX
	JMP	loop_check

literal_two_nonliteral:
	TESTL	$0x8000, BX
	JNZ	litlen_exception
	JMP	have_length

litlen_exception:
	TESTL	$0x2000, BX
	JNZ	return_block_done
	TESTL	$0x4000, BX
	JZ	return_bad_data

	// The main-table bits were already consumed. Resolve the subtable
	// entry from the remaining low-order bits.
	MOVQ	$-1, DI
	MOVQ	BX, CX
	SHRQ	$8, CX
	SHLQ	CX, DI
	NOTQ	DI
	ANDQ	AX, DI
	MOVQ	BX, CX
	SHRQ	$16, CX
	ADDQ	CX, DI
	MOVL	(R10)(DI*4), BX
	MOVQ	AX, SI
	MOVQ	BX, CX
	ANDQ	$0x3f, CX
	SHRQ	CX, AX
	SUBQ	CX, DX
	TESTL	$0x80000000, BX
	JNZ	subtable_literal
	TESTL	$0x2000, BX
	JNZ	return_block_done
	TESTL	$0x8000, BX
	JNZ	return_bad_data
	JMP	have_length

subtable_literal:
	MOVQ	BX, CX
	SHRQ	$16, CX
	MOVB	CX, (R9)
	INCQ	R9
	MOVQ	AX, CX
	ANDQ	R11, CX
	MOVL	(R10)(CX*4), BX
	MOVQ	(R8), SI
	MOVQ	DX, CX
	SHLQ	CX, SI
	ORQ	SI, AX
	XORQ	$56, CX
	SHRQ	$3, CX
	ADDQ	CX, R8
	ORQ	$56, DX
	JMP	loop_check

offset_exception:
	CMPQ	DX, $const_inflateFastOffsetSubRefillThreshold
	JA	offset_exception_ready
	MOVQ	(R8), SI
	MOVQ	DX, CX
	SHLQ	CX, SI
	ORQ	SI, AX
	XORQ	$56, CX
	SHRQ	$3, CX
	ADDQ	CX, R8
	ORQ	$56, DX

offset_exception_ready:
	TESTL	$0x4000, BX
	JZ	return_bad_data
	MOVQ	BX, CX
	ANDQ	$0x3f, CX
	SHRQ	CX, AX
	SUBQ	CX, DX
	MOVQ	$-1, DI
	MOVQ	BX, CX
	SHRQ	$8, CX
	SHLQ	CX, DI
	NOTQ	DI
	ANDQ	AX, DI
	MOVQ	BX, CX
	SHRQ	$16, CX
	ADDQ	CX, DI
	MOVQ	inflateFastState_offset(R12), R13
	MOVL	(R13)(DI*4), BX
	TESTL	$0x8000, BX
	JNZ	return_bad_data
	MOVQ	AX, SI
	MOVQ	BX, CX
	ANDQ	$0x3f, CX
	SHRQ	CX, AX
	SUBQ	CX, DX
	JMP	have_offset

copy_narrow:
	CMPQ	DI, $1
	JE	copy_rle

copy_small_offset:
	MOVQ	(SI), R13
	MOVQ	R13, (CX)
	ADDQ	DI, SI
	ADDQ	DI, CX
	MOVQ	(SI), R13
	MOVQ	R13, (CX)
	ADDQ	DI, SI
	ADDQ	DI, CX
	CMPQ	CX, R9
	JB	copy_small_offset
	JMP	loop_check

copy_rle:
	MOVBQZX	(SI), R13
	MOVQ	$0x0101010101010101, SI
	IMULQ	SI, R13
copy_rle_loop:
	MOVQ	R13, (CX)
	MOVQ	R13, 8(CX)
	MOVQ	R13, 16(CX)
	MOVQ	R13, 24(CX)
	ADDQ	$32, CX
	CMPQ	CX, R9
	JB	copy_rle_loop
	JMP	loop_check
