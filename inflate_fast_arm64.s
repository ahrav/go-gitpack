//go:build arm64 && !purego && !gitpack_libdeflate

#include "go_asm.h"
#include "textflag.h"

// func inflateHuffmanFastArm64(state *inflateFastState)
TEXT ·inflateHuffmanFastArm64(SB), NOSPLIT|NOFRAME, $0-8
	MOVD	state+0(FP), R0

	MOVD	inflateFastState_src(R0), R1
	MOVD	inflateFastState_dst(R0), R2
	MOVD	inflateFastState_litlen(R0), R3
	MOVD	inflateFastState_offset(R0), R4
	MOVD	inflateFastState_bitbuf(R0), R5
	MOVD	inflateFastState_nbits(R0), R6
	MOVD	inflateFastState_pos(R0), R7
	MOVD	inflateFastState_out(R0), R8
	MOVD	inflateFastState_inLimit(R0), R9
	MOVD	inflateFastState_outLimit(R0), R10
	MOVD	inflateFastState_litlenMask(R0), R11
	MOVD	inflateFastState_offsetMask(R0), R12
	MOVD	inflateFastState_yieldAt(R0), R13
	MOVD	$-1, R21

	ADD	R1, R7, R7
	ADD	R2, R8, R8
	ADD	R1, R9, R9
	ADD	R2, R10, R10
	ADD	R2, R13, R13

	// Stop at whichever comes first: the output margin or the next yield.
	// Keep R27 free: the Go ARM64 toolchain reserves it as REGTMP.
	CMP	R10, R13
	CSEL	LO, R13, R10, R24
	CMP	R9, R7
	CCMP	LO, R8, R24, $2
	BHS	return_boundary

	// Branchless refill. The Go wrapper's input margin proves this load.
	MOVD	(R7), R17
	EOR	$56, R6, R16
	UBFX	$3, R16, $3, R16
	ADD	R16, R7, R7
	LSL	R6, R17, R17
	ORR	R17, R5, R5
	ORR	$56, R6, R6

	AND	R11, R5, R16
	MOVWU	(R3)(R16<<2), R14

	PCALIGN	$32
decode_entry:
	MOVD	R5, R15
	LSR	R14, R5, R5
	SUB	R14, R6, R6
	TBNZ	$31, R14, literal_one
	TBNZ	$15, R14, litlen_exception

have_length:
	LSR	$16, R14, R19
	UBFX	$8, R14, $6, R16
	LSL	R14, R21, R17
	BIC	R17, R15, R17
	LSR	R16, R17, R17
	ADD	R17, R19, R19

	AND	R12, R5, R16
	MOVWU	(R4)(R16<<2), R14
	TBNZ	$15, R14, offset_exception

	AND	$0xff, R6, R16
	CMP	$30, R16
	BGT	offset_direct_ready
	MOVD	(R7), R17
	EOR	$56, R6, R16
	UBFX	$3, R16, $3, R16
	ADD	R16, R7, R7
	LSL	R6, R17, R17
	ORR	R17, R5, R5
	ORR	$56, R6, R6

offset_direct_ready:
	MOVD	R5, R15
	LSR	R14, R5, R5
	SUB	R14, R6, R6

have_offset:
	LSR	$16, R14, R20
	UBFX	$8, R14, $6, R16
	LSL	R14, R21, R17
	BIC	R17, R15, R17
	LSR	R16, R17, R17
	ADD	R17, R20, R20
	SUB	R2, R8, R16
	SUBS	R20, R16, R16
	BCC	return_bad_data

	// Preload the next entry and refill before issuing match-copy loads.
	AND	R11, R5, R16
	MOVWU	(R3)(R16<<2), R14
	MOVD	(R7), R17
	EOR	$56, R6, R16
	UBFX	$3, R16, $3, R16
	ADD	R16, R7, R7
	LSL	R6, R17, R17
	ORR	R17, R5, R5
	ORR	$56, R6, R6

	MOVD	R8, R22
	SUB	R20, R8, R23
	ADD	R19, R8, R8
	CMP	$8, R20
	BLT	copy_narrow

copy_wide:
	MOVD.P	8(R23), R25
	MOVD.P	R25, 8(R22)
	MOVD.P	8(R23), R25
	MOVD.P	R25, 8(R22)
	MOVD.P	8(R23), R25
	MOVD.P	R25, 8(R22)
	MOVD.P	8(R23), R25
	MOVD.P	R25, 8(R22)
	MOVD.P	8(R23), R25
	MOVD.P	R25, 8(R22)
	CMP	R8, R22
	BLT	copy_wide

loop_check:
	CMP	R9, R7
	CCMP	LO, R8, R24, $2
	BLO	decode_entry

return_boundary:
	CMP	R9, R7
	BGE	return_tail
	CMP	R13, R8
	BGE	return_yield

return_tail:
	MOVW	$const_inflateFastTail, R14
	B	store_state

return_block_done:
	MOVW	$const_inflateFastBlockDone, R14
	B	store_state

return_bad_data:
	MOVW	$const_inflateFastBadData, R14
	B	store_state

return_yield:
	ADD	$const_inflateFastYieldBytes, R13, R13
	MOVW	$const_inflateFastYield, R14

store_state:
	MOVD	R5, inflateFastState_bitbuf(R0)
	AND	$0xff, R6, R16
	MOVD	R16, inflateFastState_nbits(R0)
	SUB	R1, R7, R16
	MOVD	R16, inflateFastState_pos(R0)
	SUB	R2, R8, R16
	MOVD	R16, inflateFastState_out(R0)
	SUB	R2, R13, R16
	MOVD	R16, inflateFastState_yieldAt(R0)
	MOVW	R14, inflateFastState_status(R0)
	RET

literal_one:
	LSR	$16, R14, R19
	AND	R11, R5, R16
	MOVWU	(R3)(R16<<2), R14
	MOVD	R5, R15
	LSR	R14, R5, R5
	SUB	R14, R6, R6
	MOVB.P	R19, 1(R8)
	TBNZ	$31, R14, literal_two
	TBNZ	$15, R14, litlen_exception
	B	have_length

literal_two:
	LSR	$16, R14, R19
	AND	R11, R5, R16
	MOVWU	(R3)(R16<<2), R14
	MOVD	R5, R15
	LSR	R14, R5, R5
	SUB	R14, R6, R6
	MOVB.P	R19, 1(R8)
	TBZ	$31, R14, literal_two_nonliteral

	// A third literal completes this iteration. Preload the next entry
	// before refilling so its lookup can overlap the refill load.
	LSR	$16, R14, R19
	AND	R11, R5, R16
	MOVWU	(R3)(R16<<2), R14
	MOVD	(R7), R17
	EOR	$56, R6, R16
	UBFX	$3, R16, $3, R16
	ADD	R16, R7, R7
	LSL	R6, R17, R17
	ORR	R17, R5, R5
	ORR	$56, R6, R6
	MOVB.P	R19, 1(R8)
	B	loop_check

literal_two_nonliteral:
	TBNZ	$15, R14, litlen_exception
	B	have_length

litlen_exception:
	TBNZ	$13, R14, return_block_done
	TBZ	$14, R14, return_bad_data

	// The main-table bits were already consumed. Resolve the subtable
	// entry from the remaining low-order bits.
	UBFX	$8, R14, $6, R16
	LSL	R16, R21, R17
	BIC	R17, R5, R17
	LSR	$16, R14, R19
	ADD	R19, R17, R17
	MOVWU	(R3)(R17<<2), R14
	MOVD	R5, R15
	LSR	R14, R5, R5
	SUB	R14, R6, R6
	TBNZ	$31, R14, subtable_literal
	TBNZ	$13, R14, return_block_done
	TBNZ	$15, R14, return_bad_data
	B	have_length

subtable_literal:
	LSR	$16, R14, R19
	AND	R11, R5, R16
	MOVWU	(R3)(R16<<2), R14
	MOVD	(R7), R17
	EOR	$56, R6, R16
	UBFX	$3, R16, $3, R16
	ADD	R16, R7, R7
	LSL	R6, R17, R17
	ORR	R17, R5, R5
	ORR	$56, R6, R6
	MOVB.P	R19, 1(R8)
	B	loop_check

offset_exception:
	AND	$0xff, R6, R16
	CMP	$37, R16
	BGT	offset_exception_ready
	MOVD	(R7), R17
	EOR	$56, R6, R16
	UBFX	$3, R16, $3, R16
	ADD	R16, R7, R7
	LSL	R6, R17, R17
	ORR	R17, R5, R5
	ORR	$56, R6, R6

offset_exception_ready:
	TBZ	$14, R14, return_bad_data
	LSR	R14, R5, R5
	SUB	R14, R6, R6
	UBFX	$8, R14, $6, R16
	LSL	R16, R21, R17
	BIC	R17, R5, R17
	LSR	$16, R14, R20
	ADD	R20, R17, R17
	MOVWU	(R4)(R17<<2), R14
	TBNZ	$15, R14, return_bad_data
	MOVD	R5, R15
	LSR	R14, R5, R5
	SUB	R14, R6, R6
	B	have_offset

copy_narrow:
	CMP	$1, R20
	BEQ	copy_rle

copy_small_offset:
	MOVD	(R23), R25
	MOVD	R25, (R22)
	ADD	R20, R23, R23
	ADD	R20, R22, R22
	MOVD	(R23), R25
	MOVD	R25, (R22)
	ADD	R20, R23, R23
	ADD	R20, R22, R22
	CMP	R8, R22
	BLT	copy_small_offset
	B	loop_check

copy_rle:
	MOVBU	(R23), R25
	MOVD	$0x0101010101010101, R26
	MUL	R26, R25, R25
copy_rle_loop:
	STP	(R25, R25), (R22)
	STP	(R25, R25), 16(R22)
	ADD	$32, R22, R22
	CMP	R8, R22
	BLT	copy_rle_loop
	B	loop_check
