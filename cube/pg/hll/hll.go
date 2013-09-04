package hll

/*
#cgo pkg-config: hll
#include "hll.h"
*/
import "C"

import (
	"fmt"
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
	defer C.free(unsafe.Pointer(cSer))
	return C.GoBytes(unsafe.Pointer(cSer), C.int(csz))
}

func (hll *Hll) Union(hllRhs *Hll) {
	C.multiset_union(hll.ms, hllRhs.ms)
	C.free(unsafe.Pointer(hllRhs.ms)) // Free the RHS
}

func (hll *Hll) Add(value string) {
	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cValue))

	cSeed := C.int(0)
	lVal := C.int(len(value))
	cHashKey := C.hll_hash_varlena(cValue, lVal, cSeed)

	C.multiset_add(hll.ms, cHashKey)
}

func (hll *Hll) AddInt32(value int32) {
	cValue := C.int32_t(value)
	cSeed := C.int32_t(0)

	cHashKey := C.hll_hash_int32(cValue, cSeed)

	C.multiset_add(hll.ms, cHashKey)
}

func (hll *Hll) AddInt64(value int64) {
	cValue := C.int64_t(value)
	cSeed := C.int32_t(0)

	cHashKey := C.hll_hash_int64(cValue, cSeed)

	C.multiset_add(hll.ms, cHashKey)
}

func (hll *Hll) Add4Bytes(value []byte) error {
	if len(value) != 4 {
		return HllError(fmt.Sprintf("Length on input is not 4 -- %d", len(value)))
	}

	cValue := (*C.char)(unsafe.Pointer(&value[0]))
	cSeed := C.int32_t(0)

	cHashKey := C.hll_hash_4bytes(cValue, cSeed)
	C.multiset_add(hll.ms, cHashKey)

	return nil
}

func (hll *Hll) Add8Bytes(value []byte) error {
	if len(value) != 8 {
		return HllError(fmt.Sprintf("Length on input is not 8 -- %d", len(value)))
	}

	cValue := (*C.char)(unsafe.Pointer(&value[0]))
	cSeed := C.int32_t(0)

	cHashKey := C.hll_hash_8bytes(cValue, cSeed)
	C.multiset_add(hll.ms, cHashKey)

	return nil
}

func (hll *Hll) GetCardinality() float64 {
	cCard := float64(C.multiset_card(hll.ms))
	return cCard
}
