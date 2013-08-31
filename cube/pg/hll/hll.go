package hll

/*
#cgo pkg-config: hll
#include <postgres.h>
#include "hll.h"
*/
import "C"

import (
	"unsafe"
)

const (
	_                 = iota
	DEFAULT_LOG2M     = 11
	DEFAULT_REGWIDTH  = 5
	DEFAULT_EXPTHRESH = -1
	DEFAULT_SPARSEON  = 1
)

/*

 --> Need to figure out how to fix memory leak now.

 Basic type is multiset_t

 Need to support these operations

 * Make new empty hll

X hll_empty4

 * Merge two hll's together

X hll_union

 * Add a string to an hll

X hll_hash_varlena
X multiset_add

 * serialize hll to []byte

multiset_pack or byteaout

*/

// HllError is used for errors using the hll library.  It implements the
// builtin error interface.
type HllError string

func (err HllError) Error() string {
	return string(err)
}

// Hll is the basic type for the Hll library. Holds an unexported instance
// of the database, for interactions.
type Hll struct {
	ms *C.multiset_t
}

func New(log2m int, regwidth int, expthresh int64, sparseon int) (*Hll, error) {
	cLog2m := C.int32_t(log2m)
	cRegwidth := C.int32_t(regwidth)
	cExpthresh := C.int64_t(expthresh)
	cSparseon := C.int32_t(sparseon)

	hll := &Hll{ms: C.hll_empty4(cLog2m, cRegwidth, cExpthresh, cSparseon)}
	return hll, nil
}

func NewDefault() (*Hll, error) {
	hll := &Hll{ms: C.hll_empty()}
	return hll, nil
}

func (hll *Hll) Delete() {
	C.free(unsafe.Pointer(hll.ms))
}

func (hll *Hll) Print() string {
	cValue := C.multiset_tostring(hll.ms)
	defer C.free(unsafe.Pointer(cValue))
	return C.GoString(cValue)
}

func (hll *Hll) Serialize() []byte {
	csz := C.multiset_packed_size(hll.ms)
	cSer := C.multiset_pack_wrap(hll.ms, csz)
	return C.GoBytes(unsafe.Pointer(cSer), C.int(csz))
}

func (hll *Hll) Union(hllRhs *Hll) {
	C.multiset_union(hll.ms, hllRhs.ms)

	// TODO -- always free RHS?
	// C.free(unsafe.Pointer(hllRhs.ms))
}

func (hll *Hll) Add(value string) {
	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cValue))

	cSeed := C.int(0)
	lVal := C.int(len(value))
	cHashKey := C.hll_hash_varlena(cValue, lVal, cSeed)

	C.multiset_add(hll.ms, cHashKey)
}
