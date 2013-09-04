/* Copyright 2013 Aggregate Knowledge, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#if defined(__APPLE__)
#include <libkern/OSByteOrder.h>
#define bswap_64 OSSwapInt64
#else
#include <byteswap.h>
#endif

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <stdbool.h>
#include <assert.h>

#include "hll.h"
#include "MurmurHash3.h"

// ----------------------------------------------------------------
// Output Version Control
// ----------------------------------------------------------------

// Set the default output schema.
static uint8_t g_output_version = 1;

// ----------------------------------------------------------------
// Type Modifiers
// ----------------------------------------------------------------

// The type modifiers need to be packed in the lower 31 bits
// of an int32.  We currently use the lowest 15 bits.
//
#define LOG2M_BITS		5
#define REGWIDTH_BITS 	3
#define EXPTHRESH_BITS	6
#define SPARSEON_BITS	1
#define TYPMOD_BITS		15

#define MAX_BITVAL(nbits)	((1 << nbits) - 1)

static int32_t g_default_log2m = DEFAULT_LOG2M;
static int32_t g_default_regwidth = DEFAULT_REGWIDTH;
static int64_t g_default_expthresh = DEFAULT_EXPTHRESH;
static int32_t g_default_sparseon = DEFAULT_SPARSEON;

enum {
    MST_UNDEFINED	= 0x0,		// Invalid/undefined set.
    MST_EMPTY		= 0x1,		// Empty set.
    MST_EXPLICIT	= 0x2,		// List of explicit ids.
    MST_SPARSE		= 0x3,		// Sparse set of compression registers.
    MST_COMPRESSED	= 0x4,		// Array of compression registers.

    MST_UNINIT		= 0xffff,	// Internal uninitialized.
};

// Generic error reporter. Prints the message and then dies.
static void ereport(const char* msg) {
    printf("%s\n", msg);
    assert(0);
}

static int32_t typmod_log2m(int32_t typmod)
{
    return (typmod >> (TYPMOD_BITS - LOG2M_BITS))
        & MAX_BITVAL(LOG2M_BITS);
}

static int32_t typmod_regwidth(int32_t typmod)
{
    return (typmod >> (TYPMOD_BITS - LOG2M_BITS - REGWIDTH_BITS))
        & MAX_BITVAL(REGWIDTH_BITS);
}

static int32_t typmod_expthresh(int32_t typmod)
{
    return (typmod >> (TYPMOD_BITS - LOG2M_BITS -
                       REGWIDTH_BITS - EXPTHRESH_BITS))
        & MAX_BITVAL(EXPTHRESH_BITS);
}

static int32_t typmod_sparseon(int32_t typmod)
{
    return (typmod >> (TYPMOD_BITS - LOG2M_BITS -
                       REGWIDTH_BITS - EXPTHRESH_BITS - SPARSEON_BITS))
        & MAX_BITVAL(SPARSEON_BITS);
}

// The expthresh is represented in a encoded format in the
// type modifier to save metadata bits.  This routine is used
// when the expthresh comes from a typmod value or hll header.
//
static int64_t decode_expthresh(int32_t encoded_expthresh)
{
    // This routine presumes the encoded value is correct and
    // doesn't range check.
    //
    if (encoded_expthresh == 63)
        return -1LL;
    else if (encoded_expthresh == 0)
        return 0;
    else
        return 1LL << (encoded_expthresh - 1);
}

static int32_t integer_log2(int64_t val)
{
    // Take the log2 of the expthresh.
    int32_t count = 0;
    int64_t value = val;

    assert(val >= 0);

    while (value)
    {
        ++count;
        value >>= 1;
    }
    return count - 1;
}

// This routine is used to encode an expthresh value to be stored
// in the typmod metadata or a hll header.
//
static int32_t encode_expthresh(int64_t expthresh)
{
    // This routine presumes the uncompressed value is correct and
    // doesn't range check.
    //
    if (expthresh == -1)
        return 63;
    else if (expthresh == 0)
        return 0;
    else
        return integer_log2(expthresh) + 1;
}

// If expthresh == -1 (auto select expthresh) determine
// the expthresh to use from nbits and nregs.
//
static size_t
expthresh_value(int64_t expthresh, size_t nbits, size_t nregs)
{
    if (expthresh != -1)
    {
        return (size_t) expthresh;
    }
    else
    {
        // Auto is selected, choose the maximum number of explicit
        // registers that fits in the same space as the compressed
        // encoding.
        size_t cmpsz = ((nbits * nregs) + 7) / 8;
        return cmpsz / sizeof(uint64_t);
    }
}

// ----------------------------------------------------------------
// Maximum Sparse Control
// ----------------------------------------------------------------

// By default we set the sparse to full compressed threshold
// automatically to the point where the sparse representation would
// start to be larger.  This can be overridden with the
// hll_set_max_sparse directive ...
//
static int g_max_sparse = -1;

static uint32_t
bitstream_unpack(bitstream_read_cursor_t * brcp)
{
    uint32_t retval;

    // Fetch the quadword containing our data.
    uint64_t qw = * (uint64_t const *) brcp->brc_curp;

    // Swap the bytes.
    qw = bswap_64(qw);

    // Shift the bits we want into place.
    qw >>= 64 - brcp->brc_nbits - brcp->brc_used;

    // Mask the bits we want.
    retval = (uint32_t) (qw & 0xffffffff) & brcp->brc_mask;

    // We've used some more bits now.
    brcp->brc_used += brcp->brc_nbits;

    // Normalize the cursor.
    while (brcp->brc_used >= 8)
    {
        brcp->brc_used -= 8;
        brcp->brc_curp += 1;
    }

    return retval;
}

static void
compressed_unpack(compreg_t * i_regp,
                  size_t i_width,
                  size_t i_nregs,
                  uint8_t const * i_bitp,
                  size_t i_size,
                  uint8_t i_vers)
{
    size_t bitsz;
    size_t padsz;

    bitstream_read_cursor_t brc;

    bitsz = i_width * i_nregs;

    // Fail fast if the compressed array isn't big enough.
    if (i_size * 8 < bitsz)
        ereport("compressed hll argument not large enough");

    padsz = i_size * 8 - bitsz;

    // Fail fast if the pad size doesn't make sense.
    if (padsz >= 8)
        ereport("inconsistent padding in compressed hll argument");

    brc.brc_nbits = i_width;
    brc.brc_mask = (1 << i_width) - 1;
    brc.brc_curp = i_bitp;
    brc.brc_used = 0;

    for (ssize_t ndx = 0; ndx < i_nregs; ++ndx)
    {
        uint32_t val = bitstream_unpack(&brc);
        i_regp[ndx] = val;
    }
}

static void
sparse_unpack(compreg_t * i_regp,
              size_t i_width,
              size_t i_log2nregs,
              size_t i_nfilled,
              uint8_t const * i_bitp,
              size_t i_size)
{
    size_t bitsz;
    size_t padsz;
    size_t chunksz;
    uint32_t regmask;

    bitstream_read_cursor_t brc;

    chunksz = i_log2nregs + i_width;
    bitsz = chunksz * i_nfilled;
    padsz = i_size * 8 - bitsz;

    // Fail fast if the pad size doesn't make sense.
    if (padsz >= 8)
        ereport("inconsistent padding in sparse hll argument");

    regmask = (1 << i_width) - 1;

    brc.brc_nbits = chunksz;
    brc.brc_mask = (1 << chunksz) - 1;
    brc.brc_curp = i_bitp;
    brc.brc_used = 0;

    for (ssize_t ii = 0; ii < i_nfilled; ++ii)
    {
        uint32_t buffer = bitstream_unpack(&brc);
        uint32_t val = buffer & regmask;
        uint32_t ndx = buffer >> i_width;
        i_regp[ndx] = val;
    }
}

typedef struct
{
    size_t			bwc_nbits;	// Write size.
    uint8_t *		bwc_curp;	// Current byte.
    size_t			bwc_used;	// Used bits.

} bitstream_write_cursor_t;

static void
bitstream_pack(bitstream_write_cursor_t * bwcp, uint32_t val)
{
    // Fetch the quadword where our data goes.
    uint64_t qw = * (uint64_t *) bwcp->bwc_curp;

    // Swap the bytes.
    qw = bswap_64(qw);

    // Shift our bits into place and combine.
    qw |= ((uint64_t) val << (64 - bwcp->bwc_nbits - bwcp->bwc_used));

    // Swap the bytes back again.
    qw = bswap_64(qw);

    // Write the word back out.
    * (uint64_t *) bwcp->bwc_curp = qw;

    // We've used some more bits now.
    bwcp->bwc_used += bwcp->bwc_nbits;

    // Normalize the cursor.
    while (bwcp->bwc_used >= 8)
    {
        bwcp->bwc_used -= 8;
        bwcp->bwc_curp += 1;
    }

}

static void
compressed_pack(compreg_t const * i_regp,
                size_t i_width,
                size_t i_nregs,
                uint8_t * o_bitp,
                size_t i_size,
                uint8_t i_vers)
{
    size_t bitsz;
    size_t padsz;

    bitstream_write_cursor_t bwc;

    // We need to zero the output array because we use
    // an bitwise-or-accumulator below.
    memset(o_bitp, '\0', i_size);

    bitsz = i_width * i_nregs;

    // Fail fast if the compressed array isn't big enough.
    if (i_size * 8 < bitsz)
        ereport("compressed output buffer not large enough");

    padsz = i_size * 8 - bitsz;

    // Fail fast if the pad size doesn't make sense.
    if (padsz >= 8)
        ereport("inconsistent compressed output pad size");

    bwc.bwc_nbits = i_width;
    bwc.bwc_curp = o_bitp;
    bwc.bwc_used = 0;

    for (ssize_t ndx = 0; ndx < i_nregs; ++ndx)
        bitstream_pack(&bwc, i_regp[ndx]);
}

static void
sparse_pack(compreg_t const * i_regp,
            size_t i_width,
            size_t i_nregs,
            size_t i_log2nregs,
            size_t i_nfilled,
            uint8_t * o_bitp,
            size_t i_size)
{
    size_t bitsz;
    size_t padsz;

    bitstream_write_cursor_t bwc;

    // We need to zero the output array because we use
    // an bitwise-or-accumulator below.
    memset(o_bitp, '\0', i_size);

    bitsz = i_nfilled * (i_log2nregs + i_width);

    // Fail fast if the compressed array isn't big enough.
    if (i_size * 8 < bitsz)
        ereport("sparse output buffer not large enough");

    padsz = i_size * 8 - bitsz;

    // Fail fast if the pad size doesn't make sense.
    if (padsz >= 8)
        ereport("inconsistent sparse output pad size");

    bwc.bwc_nbits = i_log2nregs + i_width;
    bwc.bwc_curp = o_bitp;
    bwc.bwc_used = 0;

    for (ssize_t ndx = 0; ndx < i_nregs; ++ndx)
    {
        if (i_regp[ndx] != 0)
        {
            uint32_t buffer = (ndx << i_width) | i_regp[ndx];
            bitstream_pack(&bwc, buffer);
        }
    }
}

static void
check_metadata(multiset_t const * i_omp, multiset_t const * i_imp)
{
    if (i_omp->ms_nbits != i_imp->ms_nbits)
    {
        ereport("register width does not match.");
    }

    if (i_omp->ms_nregs != i_imp->ms_nregs)
    {
        ereport("register count does not match");
    }

    // Don't need to compare log2nregs because we compared nregs ...

    if (i_omp->ms_expthresh != i_imp->ms_expthresh)
    {
        ereport("explicit threshold does not match");
    }

    if (i_omp->ms_sparseon != i_imp->ms_sparseon)
    {
        ereport("sparse enable does not match");
    }
}

static void
copy_metadata(multiset_t * o_msp, multiset_t const * i_msp)
{
    o_msp->ms_nbits = i_msp->ms_nbits;
    o_msp->ms_nregs = i_msp->ms_nregs;
    o_msp->ms_log2nregs = i_msp->ms_log2nregs;
    o_msp->ms_expthresh = i_msp->ms_expthresh;
    o_msp->ms_sparseon = i_msp->ms_sparseon;
}

static void
compressed_add(multiset_t * o_msp, uint64_t elem)
{
    size_t nbits = o_msp->ms_nbits;
    size_t nregs = o_msp->ms_nregs;
    size_t log2nregs = o_msp->ms_log2nregs;

    ms_compressed_t * mscp = &o_msp->ms_data.as_comp;

    uint64_t mask = nregs - 1;

    size_t maxregval = (1 << nbits) - 1;

    size_t ndx = elem & mask;

    uint64_t ss_val = elem >> log2nregs;

    size_t p_w = ss_val == 0 ? 0 : __builtin_ctzll(ss_val) + 1;

    if (p_w > maxregval)
        p_w = maxregval;

    if (mscp->msc_regs[ndx] < p_w)
        mscp->msc_regs[ndx] = p_w;
}

static void
compressed_explicit_union(multiset_t * o_msp, multiset_t const * i_msp)
{
    ms_explicit_t const * msep = &i_msp->ms_data.as_expl;
    for (size_t ii = 0; ii < msep->mse_nelem; ++ii)
        compressed_add(o_msp, msep->mse_elems[ii]);
}

static void
explicit_to_compressed(multiset_t * msp)
{
    // Make a copy of the explicit multiset.
    multiset_t ms;
    memcpy(&ms, msp, sizeof(ms));

    // Clear the multiset.
    memset(msp, '\0', sizeof(*msp));

    // Restore the metadata.
    copy_metadata(msp, &ms);

    // Make it MST_COMPRESSED.
    msp->ms_type = MST_COMPRESSED;

    // Add all the elements back into the compressed multiset.
    compressed_explicit_union(msp, &ms);
}

static int
element_compare(void const * ptr1, void const * ptr2)
{
    // We used signed integer comparison to be compatible
    // with the java code.

    int64_t v1 = * (int64_t const *) ptr1;
    int64_t v2 = * (int64_t const *) ptr2;

    return (v1 < v2) ? -1 : (v1 > v2) ? 1 : 0;
}

static size_t numfilled(multiset_t const * i_msp)
{
    ms_compressed_t const * mscp = &i_msp->ms_data.as_comp;
    size_t nfilled = 0;
    size_t nregs = i_msp->ms_nregs;
    for (size_t ii = 0; ii < nregs; ++ii)
        if (mscp->msc_regs[ii] > 0)
            ++nfilled;

    return nfilled;
}

char *
multiset_tostring(multiset_t const * i_msp)
{
    char expbuf[256];
    char * retstr;
    size_t len;
    size_t used;
    size_t nbits = i_msp->ms_nbits;
    size_t nregs = i_msp->ms_nregs;
    int64_t expthresh = i_msp->ms_expthresh;
    size_t sparseon = i_msp->ms_sparseon;

    size_t expval = expthresh_value(expthresh, nbits, nregs);

    // If the expthresh is set to -1 (auto) augment the value
    // with the automatically determined value.
    //
    if (expthresh == -1)
        snprintf(expbuf, sizeof(expbuf), "%ld(%ld)", expthresh, expval);
    else
        snprintf(expbuf, sizeof(expbuf), "%ld", expthresh);

    // Allocate an initial return buffer.
    len = 1024;
    retstr = (char *) malloc(len);
    memset(retstr, '\0', len);

    // We haven't used any return buffer yet.
    used = 0;

    // Print in a type-dependent way.
    switch (i_msp->ms_type)
    {
    case MST_EMPTY:
        used += snprintf(retstr, len, "EMPTY, "
                         "nregs=%ld, nbits=%ld, expthresh=%s, sparseon=%ld",
                         nregs, nbits, expbuf, sparseon);
        break;
    case MST_EXPLICIT:
        {
            ms_explicit_t const * msep = &i_msp->ms_data.as_expl;
            size_t size = msep->mse_nelem;
            char linebuf[1024];
            ssize_t rv;

            used += snprintf(retstr, len, "EXPLICIT, %ld elements, "
                             "nregs=%ld, nbits=%ld, "
                             "expthresh=%s, sparseon=%ld:",
                             size, nregs, nbits, expbuf, sparseon);
            for (size_t ii = 0; ii < size; ++ii)
            {
                int64_t val = * (int64_t const *) & msep->mse_elems[ii];
                rv = snprintf(linebuf, sizeof(linebuf),
                              "\n%ld: %20" PRIi64 " ",
                              ii, val);
                // Do we need to reallocate the return buffer?
                if (rv + used > len - 1)
                {
                    len += 1024;
                    retstr = (char *) realloc(retstr, len);
                }
                strncpy(&retstr[used], linebuf, len - used);
                used += rv;
            }
        }
        break;
    case MST_COMPRESSED:
        {
            ms_compressed_t const * mscp = &i_msp->ms_data.as_comp;

            char linebuf[1024];

            size_t rowsz = 32;
            size_t nrows = nregs / rowsz;
            size_t ndx = 0;

            used += snprintf(retstr, len,
                             "COMPRESSED, %ld filled "
                             "nregs=%ld, nbits=%ld, expthresh=%s, "
                             "sparseon=%ld:",
                             numfilled(i_msp),
                             nregs, nbits, expbuf, sparseon);

            for (size_t rr = 0; rr < nrows; ++rr)
            {
                size_t pos = 0;
                pos = snprintf(linebuf, sizeof(linebuf), "\n%4ld: ", ndx);
                for (size_t cc = 0; cc < rowsz; ++cc)
                {
                    pos += snprintf(&linebuf[pos], sizeof(linebuf) - pos,
                                    "%2d ", mscp->msc_regs[ndx]);
                    ++ndx;
                }

                // Do we need to reallocate the return buffer?
                if (pos + used > len - 1)
                {
                    len += 1024;
                    retstr = (char *) realloc(retstr, len);
                }
                strncpy(&retstr[used], linebuf, len - used);
                used += pos;
            }
        }
        break;
    case MST_UNDEFINED:
        used += snprintf(retstr, len, "UNDEFINED "
                         "nregs=%ld, nbits=%ld, expthresh=%s, sparseon=%ld",
                         nregs, nbits, expbuf, sparseon);
        break;
    default:
        ereport("unexpected multiset type value");
        break;
    }

    return retstr;
}

static void
explicit_validate(multiset_t const * i_msp, ms_explicit_t const * i_msep)
{
    // Allow explicit multisets with no elements.
    if (i_msep->mse_nelem == 0)
        return;

    // Confirm that all elements are ascending with no duplicates.
    for (int ii = 0; ii < i_msep->mse_nelem - 1; ++ii)
    {
        if (element_compare(&i_msep->mse_elems[ii],
                            &i_msep->mse_elems[ii + 1]) != -1)
        {
            char * buf = multiset_tostring(i_msp);

            ereport("duplicate or descending explicit elements");

            free(buf);
        }
    }
}

void
multiset_add(multiset_t * o_msp, uint64_t element)
{
    // WARNING!  This routine can change the type of the multiset!

    size_t expval = expthresh_value(o_msp->ms_expthresh,
                                    o_msp->ms_nbits,
                                    o_msp->ms_nregs);

    switch (o_msp->ms_type)
    {
    case MST_EMPTY:
        // Are we forcing compressed?
        if (expval == 0)
        {
            // Now we're explicit with no elements.
            o_msp->ms_type = MST_EXPLICIT;
            o_msp->ms_data.as_expl.mse_nelem = 0;

            // Convert it to compressed.
            explicit_to_compressed(o_msp);

            // Add the element in compressed format.
            compressed_add(o_msp, element);
        }
        else
        {
            // Now we're explicit with one element.
            o_msp->ms_type = MST_EXPLICIT;
            o_msp->ms_data.as_expl.mse_nelem = 1;
            o_msp->ms_data.as_expl.mse_elems[0] = element;
        }
        break;

    case MST_EXPLICIT:
        {
            ms_explicit_t * msep = &o_msp->ms_data.as_expl;

            // If the element is already in the set we're done.
            if (bsearch(&element,
                        msep->mse_elems,
                        msep->mse_nelem,
                        sizeof(uint64_t),
                        element_compare))
                return;

            // Is the explicit multiset full?
            if (msep->mse_nelem == expval)
            {
                // Convert it to compressed.
                explicit_to_compressed(o_msp);

                // Add the element in compressed format.
                compressed_add(o_msp, element);
            }
            else
            {
                // Add the element at the end.
                msep->mse_elems[msep->mse_nelem++] = element;

                // Resort the elements.
                qsort(msep->mse_elems,
                      msep->mse_nelem,
                      sizeof(uint64_t),
                      element_compare);
            }
        }
        break;

    case MST_COMPRESSED:
        compressed_add(o_msp, element);
        break;

    case MST_UNDEFINED:
        // Result is unchanged.
        break;

    default:
        ereport("undefined multiset type value #1");
        break;
    }
}

static void
explicit_union(multiset_t * o_msp, ms_explicit_t const * i_msep)
{
    // NOTE - This routine is optimized to add a batch of elements;
    // it doesn't resort until they are all added ...
    //
    // WARNING!  This routine can change the type of the target multiset!

    size_t expval = expthresh_value(o_msp->ms_expthresh,
                                    o_msp->ms_nbits,
                                    o_msp->ms_nregs);

    ms_explicit_t * msep = &o_msp->ms_data.as_expl;

    // Note the starting size of the target set.
    size_t orig_nelem = msep->mse_nelem;

    for (size_t ii = 0; ii < i_msep->mse_nelem; ++ii)
    {
        uint64_t element = i_msep->mse_elems[ii];

        switch (o_msp->ms_type)
        {
        case MST_EXPLICIT:
            if (bsearch(&element,
                        msep->mse_elems,
                        orig_nelem,
                        sizeof(uint64_t),
                        element_compare))
                continue;

            if (msep->mse_nelem < expval)
            {
                // Add the element at the end.
                msep->mse_elems[msep->mse_nelem++] = element;
            }
            else
            {
                // Convert it to compressed.
                explicit_to_compressed(o_msp);

                // Add the element in compressed format.
                compressed_add(o_msp, element);
            }
            break;

        case MST_COMPRESSED:
            compressed_add(o_msp, element);
            break;
        }
    }

    // If the target multiset is still explicit it needs to be
    // resorted.
    if (o_msp->ms_type == MST_EXPLICIT)
    {
        // Resort the elements.
        qsort(msep->mse_elems,
              msep->mse_nelem,
              sizeof(uint64_t),
              element_compare);
    }
}

static void unpack_header(multiset_t * o_msp,
                          uint8_t const * i_bitp,
                          uint8_t vers,
                          uint8_t type)
{
    o_msp->ms_nbits = (i_bitp[1] >> 5) + 1;
    o_msp->ms_log2nregs = i_bitp[1] & 0x1f;
    o_msp->ms_nregs = 1 <<  o_msp->ms_log2nregs;
    o_msp->ms_expthresh = decode_expthresh(i_bitp[2] & 0x3f);
    o_msp->ms_sparseon = (i_bitp[2] >> 6) & 0x1;
}

static uint8_t
multiset_unpack(multiset_t * o_msp,
                uint8_t const * i_bitp,
                size_t i_size,
                uint8_t * o_encoded_type)
{
    // First byte is the version and type header.
    uint8_t vers = (i_bitp[0] >> 4) & 0xf;
    uint8_t type = i_bitp[0] & 0xf;

    if (vers != 1)
        ereport("unknown schema version");

    if (o_encoded_type != NULL)
        *o_encoded_type = type;

    // Set the type. NOTE - MST_SPARSE are converted to MST_COMPRESSED.
    o_msp->ms_type = (type == MST_SPARSE) ? MST_COMPRESSED : type;

    switch (type)
    {
    case MST_EMPTY:
        if (vers == 1)
        {
            size_t hdrsz = 3;

            // Make sure the size is consistent.
            if (i_size != hdrsz)
            {
                ereport("inconsistently sized empty multiset");
            }

            unpack_header(o_msp, i_bitp, vers, type);
        }
        else
        {
            ereport("unsupported empty version");
        }

        break;

    case MST_EXPLICIT:
        if (vers == 1)
        {
            ms_explicit_t * msep = &o_msp->ms_data.as_expl;
            size_t hdrsz = 3;
            size_t nelem = (i_size - hdrsz) / 8;
            size_t ndx = hdrsz;

            // Make sure the size is consistent.
            if (((i_size - hdrsz) % 8) != 0)
            {
                ereport("inconsistently sized explicit multiset");
            }

            // Make sure the explicit array fits in memory.
            if ((i_size - hdrsz) > MS_MAXDATA)
            {
                ereport("explicit multiset too large");
            }

            unpack_header(o_msp, i_bitp, vers, type);

            msep->mse_nelem = nelem;
            for (size_t ii = 0; ii < nelem; ++ii)
            {
                uint64_t val = 0;
                val |= ((uint64_t) i_bitp[ndx++] << 56);
                val |= ((uint64_t) i_bitp[ndx++] << 48);
                val |= ((uint64_t) i_bitp[ndx++] << 40);
                val |= ((uint64_t) i_bitp[ndx++] << 32);
                val |= ((uint64_t) i_bitp[ndx++] << 24);
                val |= ((uint64_t) i_bitp[ndx++] << 16);
                val |= ((uint64_t) i_bitp[ndx++] <<  8);
                val |= ((uint64_t) i_bitp[ndx++] <<  0);
                msep->mse_elems[ii] = val;
            }

            explicit_validate(o_msp, msep);
        }
        else
        {
            ereport("unsupported explicit version");
        }
        break;

    case MST_COMPRESSED:
        if (vers == 1)
        {
            size_t hdrsz = 3;

            // Decode the parameter byte.
            uint8_t param = i_bitp[1];
            size_t nbits = (param >> 5) + 1;
            size_t log2nregs = param & 0x1f;
            size_t nregs = 1 << log2nregs;

            // Make sure the size is consistent.
            size_t bitsz = nbits * nregs;
            size_t packedbytesz = (bitsz + 7) / 8;
            if ((i_size - hdrsz) != packedbytesz)
            {
                ereport("inconsistently sized "
                                "compressed multiset");
            }

            // Make sure the compressed array fits in memory.
            if (nregs * sizeof(compreg_t) > MS_MAXDATA)
            {
                ereport("compressed multiset too large");
            }

            unpack_header(o_msp, i_bitp, vers, type);

            // Fill the registers.
            compressed_unpack(o_msp->ms_data.as_comp.msc_regs,
                              nbits, nregs, &i_bitp[hdrsz], i_size - hdrsz,
                              vers);
        }
        else
        {
            ereport("unsupported compressed version");
        }
        break;

    case MST_UNDEFINED:
        if (vers == 1)
        {
            size_t hdrsz = 3;

            // Make sure the size is consistent.
            if (i_size != hdrsz)
            {
                ereport("undefined multiset value");
            }

            unpack_header(o_msp, i_bitp, vers, type);
        }
        else
        {
            ereport("unsupported undefined version");
        }

        break;

    case MST_SPARSE:
        if (vers == 1)
        {
            size_t hdrsz = 3;

            ms_compressed_t * mscp;

            if (i_size < hdrsz)
            {
                ereport("sparse multiset too small");
            }
            else
            {
                // Decode the parameter byte.
                uint8_t param = i_bitp[1];
                size_t nbits = (param >> 5) + 1;
                size_t log2nregs = param & 0x1f;
                size_t nregs = 1 << log2nregs;

                // Figure out how many encoded registers are in the
                // bitstream.  We depend on the log2nregs + nbits being
                // greater then the pad size so we aren't left with
                // ambiguity in the final pad byte.

                size_t bitsz = (i_size - hdrsz) * 8;
                size_t chunksz = log2nregs + nbits;
                size_t nfilled = bitsz / chunksz;

                // Make sure the compressed array fits in memory.
                if (nregs * sizeof(compreg_t) > MS_MAXDATA)
                {
                    ereport("sparse multiset too large");
                }

                unpack_header(o_msp, i_bitp, vers, type);

                mscp = &o_msp->ms_data.as_comp;

                // Pre-zero the registers since sparse only fills
                // in occasional ones.
                //
                for (int ii = 0; ii < nregs; ++ii)
                    mscp->msc_regs[ii] = 0;

                // Fill the registers.
                sparse_unpack(mscp->msc_regs,
                              nbits, log2nregs, nfilled,
                              &i_bitp[hdrsz], i_size - hdrsz);
            }
        }
        else
        {
            ereport("unsupported sparse version");
        }
        break;

    default:
        // This is always an error.
        ereport("undefined multiset type");
        break;
    }

    return vers;
}

static size_t
pack_header(uint8_t * o_bitp,
            uint8_t vers,
            uint8_t type,
            size_t nbits,
            size_t log2nregs,
            int64_t expthresh,
            size_t sparseon)
{
    size_t ndx = 0;

    o_bitp[ndx++] = (vers << 4) | type;
    o_bitp[ndx++] = ((nbits - 1) << 5) | log2nregs;
    o_bitp[ndx++] = (sparseon << 6) | encode_expthresh(expthresh);

    return ndx;
}

void
multiset_pack(multiset_t const * i_msp, uint8_t * o_bitp, size_t i_size)
{
    uint8_t vers = g_output_version;

    size_t nbits = i_msp->ms_nbits;
    size_t log2nregs = i_msp->ms_log2nregs;
    int64_t expthresh = i_msp->ms_expthresh;
    size_t sparseon = i_msp->ms_sparseon;

    switch (i_msp->ms_type)
    {
    case MST_EMPTY:
        pack_header(o_bitp, vers, MST_EMPTY,
                    nbits, log2nregs, expthresh, sparseon);
        break;

    case MST_EXPLICIT:
        {
            ms_explicit_t const * msep = &i_msp->ms_data.as_expl;
            size_t size = msep->mse_nelem;

            size_t ndx = pack_header(o_bitp, vers, MST_EXPLICIT,
                                     nbits, log2nregs, expthresh, sparseon);

            for (size_t ii = 0; ii < size; ++ii)
            {
                uint64_t val = msep->mse_elems[ii];

                o_bitp[ndx++] = (val >> 56) & 0xff;
                o_bitp[ndx++] = (val >> 48) & 0xff;
                o_bitp[ndx++] = (val >> 40) & 0xff;
                o_bitp[ndx++] = (val >> 32) & 0xff;
                o_bitp[ndx++] = (val >> 24) & 0xff;
                o_bitp[ndx++] = (val >> 16) & 0xff;
                o_bitp[ndx++] = (val >>  8) & 0xff;
                o_bitp[ndx++] = (val >>  0) & 0xff;
            }
        }
        break;

    case MST_COMPRESSED:
        {
            ms_compressed_t const * mscp = &i_msp->ms_data.as_comp;
            size_t nregs = i_msp->ms_nregs;
            size_t nfilled = numfilled(i_msp);

            // Should we pack this as MST_SPARSE or MST_COMPRESSED?
            // IMPORTANT - matching code in multiset_packed_size!
            size_t sparsebitsz;
            size_t cmprssbitsz;
            sparsebitsz = nfilled * (log2nregs + nbits);
            cmprssbitsz = nregs * nbits;

            // If the vector does not have sparse enabled use
            // compressed.
            //
            // If the vector is smaller then the max sparse size use
            // compressed.
            //
            // If the max sparse size is auto (-1) use if smaller then
            // compressed.
            //
            if (sparseon &&
                ((g_max_sparse != -1 && nfilled <= g_max_sparse) ||
                 (g_max_sparse == -1 && sparsebitsz < cmprssbitsz)))
            {
                size_t ndx = pack_header(o_bitp, vers, MST_SPARSE,
                                         nbits, log2nregs, expthresh, sparseon);

                // Marshal the registers.
                sparse_pack(mscp->msc_regs,
                            nbits, nregs, log2nregs, nfilled,
                            &o_bitp[ndx], i_size - ndx);
            }
            else
            {
                size_t ndx = pack_header(o_bitp, vers, MST_COMPRESSED,
                                         nbits, log2nregs, expthresh, sparseon);

                // Marshal the registers.
                compressed_pack(mscp->msc_regs, nbits, nregs,
                                &o_bitp[ndx], i_size - ndx, vers);
            }
            break;
        }

    case MST_UNDEFINED:
        pack_header(o_bitp, vers, MST_UNDEFINED,
                    nbits, log2nregs, expthresh, sparseon);
        break;

    case MST_SPARSE:
        // We only marshal (pack) into sparse format; complain if
        // an in-memory multiset claims it is sparse.
        ereport("invalid internal sparse format");
        break;

    default:
        ereport("undefined multiset type value #2");
        break;
    }
}

static size_t
multiset_copy_size(multiset_t const * i_msp)
{
    size_t retval = 0;

    switch (i_msp->ms_type)
    {
    case MST_EMPTY:
        retval = __builtin_offsetof(multiset_t, ms_data);
        break;

    case MST_EXPLICIT:
        {
            ms_explicit_t const * msep = &i_msp->ms_data.as_expl;
            retval = __builtin_offsetof(multiset_t, ms_data.as_expl.mse_elems);
            retval += (msep->mse_nelem * sizeof(uint64_t));
        }
        break;

    case MST_COMPRESSED:
        {
            retval = __builtin_offsetof(multiset_t, ms_data.as_comp.msc_regs);
            retval += (i_msp->ms_nregs * sizeof(compreg_t));
        }
        break;

    case MST_UNDEFINED:
        retval = __builtin_offsetof(multiset_t, ms_data);
        break;

    default:
        ereport("undefined multiset type value #3");
        break;
    }

    return retval;
}

size_t
multiset_packed_size(multiset_t const * i_msp)
{
    uint8_t vers = g_output_version;

    size_t retval = 0;

    switch (i_msp->ms_type)
    {
    case MST_EMPTY:
        switch (vers)
        {
        case 1:
            retval = 3;
            break;
        default:
            assert(vers == 1);
        }
        break;

    case MST_EXPLICIT:
        switch (vers)
        {
        case 1:
            {
                ms_explicit_t const * msep = &i_msp->ms_data.as_expl;
                retval = 3 + (8 * msep->mse_nelem);
            }
            break;
        default:
            assert(vers == 1);
        }
        break;

    case MST_COMPRESSED:
        if (vers == 1)
        {
            size_t hdrsz = 3;
            size_t nbits = i_msp->ms_nbits;
            size_t nregs = i_msp->ms_nregs;
            size_t nfilled = numfilled(i_msp);
            size_t log2nregs = i_msp->ms_log2nregs;
            size_t sparseon = i_msp->ms_sparseon;
            size_t sparsebitsz;
            size_t cmprssbitsz;

            // Should we pack this as MST_SPARSE or MST_COMPRESSED?
            // IMPORTANT - matching code in multiset_pack!
            //
            sparsebitsz = numfilled(i_msp) * (log2nregs + nbits);
            cmprssbitsz = nregs * nbits;

            // If the vector does not have sparse enabled use
            // compressed.
            //
            // If the vector is smaller then the max sparse size use
            // compressed.
            //
            // If the max sparse size is auto (-1) use if smaller then
            // compressed.
            //
            if (sparseon &&
                ((g_max_sparse != -1 && nfilled <= g_max_sparse) ||
                 (g_max_sparse == -1 && sparsebitsz < cmprssbitsz)))

            {
                // MST_SPARSE is more compact.
                retval = hdrsz + ((sparsebitsz + 7) / 8);
            }
            else
            {
                // MST_COMPRESSED is more compact.
                retval = hdrsz + ((cmprssbitsz + 7) / 8);
            }
        }
        else
        {
            assert(vers == 1);
        }
        break;

    case MST_UNDEFINED:
        if (vers == 1)
        {
            size_t hdrsz = 3;
            retval = hdrsz;
        }
        else
        {
            assert(vers == 1);
        }
        break;

    case MST_SPARSE:
        // We only marshal (pack) into sparse format; complain if
        // an in-memory multiset claims it is sparse.
        ereport("invalid internal sparse format");
        break;

    default:
        ereport("undefined multiset type value #4");
        break;
    }

    return retval;
}

/// Functions hacked up from PG form

// Hash a 4 byte fixed-size object.
uint64_t hll_hash_int32(int32_t key, int32_t seed)
{
    uint64_t out[2];

    if (seed < 0)
        ereport("negative seed values not compatible");

    MurmurHash3_x64_128(&key, sizeof(key), seed, out);

    return out[0];
}

// Hash a 8 byte fixed-size object.
uint64_t hll_hash_int64(int64_t key, int32_t seed)
{
    uint64_t out[2];

    if (seed < 0)
        ereport("negative seed values not compatible");

    MurmurHash3_x64_128(&key, sizeof(key), seed, out);

    return out[0];
}

// Hash a 4 byte fixed-size object.
uint64_t hll_hash_4bytes(const char *key, int32_t seed)
{
    uint64_t out[2];

    if (seed < 0)
        ereport("negative seed values not compatible");

    MurmurHash3_x64_128(key, 4, seed, out);

    return out[0];
}

// Hash a 8 byte fixed-size object.
uint64_t hll_hash_8bytes(const char *key, int32_t seed)
{
    uint64_t out[2];

    if (seed < 0)
        ereport("negative seed values not compatible");

    MurmurHash3_x64_128(key, 8, seed, out);

    return out[0];
}

// Hash a varlena object.
uint64_t hll_hash_varlena(const char* key, int len, int seed)
{

    uint64_t out[2];

    if (seed < 0)
        ereport("negative seed values not compatible");

    MurmurHash3_x64_128(key, len, seed, out);

    return out[0];
}

static void
check_modifiers(int32_t log2m, int32_t regwidth, int64_t expthresh, int32_t sparseon)
{
    // Range check each of the modifiers.
    if (log2m < 0 || log2m > MAX_BITVAL(LOG2M_BITS))
        ereport("log2m modifier must be between 0 and 31");

    if (regwidth < 0 || regwidth > MAX_BITVAL(REGWIDTH_BITS))
        ereport("regwidth modifier must be between 0 and 7");

    if (expthresh < -1 || expthresh > 4294967296LL)
        ereport("expthresh modifier must be between -1 and 2^32");

    if (expthresh > 0 && (1LL << integer_log2(expthresh)) != expthresh)
        ereport("expthresh modifier must be power of 2");

    if (sparseon < 0 || sparseon > MAX_BITVAL(SPARSEON_BITS))
        ereport("sparseon modifier must be 0 or 1");
}

// Create an empty multiset with parameters.
// @TODO -- malloc's a pointer but doesn't free it. Must remember to free when done.
multiset_t*
hll_empty4(int32_t log2m, int32_t regwidth, int64_t expthresh, int32_t sparseon)
{

    multiset_t* o_msap; 
    o_msap = (multiset_t*)malloc(sizeof(multiset_t));
    check_modifiers(log2m, regwidth, expthresh, sparseon);
    memset(o_msap, '\0', sizeof(multiset_t));

    o_msap->ms_type = MST_EMPTY;
    o_msap->ms_nbits = regwidth;
    o_msap->ms_nregs = 1 << log2m;
    o_msap->ms_log2nregs = log2m;
    o_msap->ms_expthresh = expthresh;
    o_msap->ms_sparseon = sparseon;

    return o_msap;
}

// Create an empty multiset with default parameters.
// @TODO -- malloc's a pointer but doesn't free it. Must remember to free when done.
multiset_t*
hll_empty() {
    return hll_empty4(g_default_log2m, g_default_regwidth, g_default_expthresh, g_default_sparseon);
}

void
multiset_union(multiset_t * o_msap, multiset_t const * i_msbp)
{
    int typea = o_msap->ms_type;
    int typeb = i_msbp->ms_type;

    // If either multiset is MST_UNDEFINED result is MST_UNDEFINED.
    if (typea == MST_UNDEFINED || typeb == MST_UNDEFINED)
    {
        o_msap->ms_type = MST_UNDEFINED;
        return;
    }

    // If B is MST_EMPTY, we're done, A is unchanged.
    if (typeb == MST_EMPTY)
        return;

    // If A is MST_EMPTY, return B instead.
    if (typea == MST_EMPTY)
    {
        memcpy(o_msap, i_msbp, multiset_copy_size(i_msbp));
        return;
    }

    switch (typea)
    {
    case MST_EXPLICIT:
        {
            switch (typeb)
            {
            case MST_EXPLICIT:
                {
                    ms_explicit_t const * msebp =
                        (ms_explicit_t const *) &i_msbp->ms_data.as_expl;

                    // Note - we may not be explicit after this ...
                    explicit_union(o_msap, msebp);
                }
                break;

            case MST_COMPRESSED:
                {
                    // Make a copy of B since we can't modify it in place.
                    multiset_t mst;
                    memcpy(&mst, i_msbp, multiset_copy_size(i_msbp));
                    // Union into the copy.
                    compressed_explicit_union(&mst, o_msap);
                    // Copy the result over the A argument.
                    memcpy(o_msap, &mst, multiset_copy_size(&mst));
                }
                break;

            default:
                ereport("undefined multiset type value #5");
                break;
            }
        }
        break;

    case MST_COMPRESSED:
        {
            ms_compressed_t * mscap =
                (ms_compressed_t *) &o_msap->ms_data.as_comp;

            switch (typeb)
            {
            case MST_EXPLICIT:
                {
                    compressed_explicit_union(o_msap, i_msbp);
                }
                break;

            case MST_COMPRESSED:
                {
                    ms_compressed_t const * mscbp =
                        (ms_compressed_t const *) &i_msbp->ms_data.as_comp;

                    // The compressed vectors must be the same length.
                    if (o_msap->ms_nregs != i_msbp->ms_nregs)
                        ereport("union of differently length "
                                        "compressed vectors not supported");

                    for (unsigned ii = 0; ii < o_msap->ms_nregs; ++ii)
                    {
                        if (mscap->msc_regs[ii] < mscbp->msc_regs[ii])
                            mscap->msc_regs[ii] = mscbp->msc_regs[ii];
                    }
                }
                break;

            default:
                ereport("undefined multiset type value #6");
                break;
            }
        }
        break;

    default:
        ereport("undefined multiset type value #7");
        break;
    }
}

// Returns a serialized version of the hll
uint8_t* multiset_pack_wrap(multiset_t const * i_msp, size_t i_size) {
    uint8_t* osr;
    osr = malloc(i_size);
    multiset_pack(i_msp, osr, i_size);
    return osr;
}

// Helper function
double
gamma_register_count_squared(int nregs)
{
    if (nregs <= 8)
        ereport("number of registers too small");

    switch (nregs)
    {
    case 16:	return 0.673 * nregs * nregs;
    case 32:	return 0.697 * nregs * nregs;
    case 64:	return 0.709 * nregs * nregs;
    default:	return (0.7213 / (1.0 + 1.079 / nregs)) * nregs * nregs;
    }
}

// Get the cardinality of a multi-set
double
multiset_card(multiset_t const * i_msp)
{
    size_t nbits = i_msp->ms_nbits;
    size_t log2m = i_msp->ms_log2nregs;

    double retval = 0.0;

    uint64_t max_register_value = (1ULL << nbits) - 1;
    uint64_t pw_bits = (max_register_value - 1);
    uint64_t total_bits = (pw_bits + log2m);
    uint64_t two_to_l = (1ULL << total_bits);

    double large_estimator_cutoff = (double) two_to_l/30.0;

    switch (i_msp->ms_type)
    {
    case MST_EMPTY:
        retval = 0.0;
        break;

    case MST_EXPLICIT:
        {
            ms_explicit_t const * msep = &i_msp->ms_data.as_expl;
            return msep->mse_nelem;
        }
        break;

    case MST_COMPRESSED:
        {
            unsigned ii;
            double sum;
            int zero_count;
            uint64_t rval;
            double estimator;

            ms_compressed_t const * mscp = &i_msp->ms_data.as_comp;
            size_t nregs = i_msp->ms_nregs;

            sum = 0.0;
            zero_count = 0;

            for (ii = 0; ii < nregs; ++ii)
            {
                rval = mscp->msc_regs[ii];
                sum += 1.0 / (1L << rval);
                if (rval == 0)
                    ++zero_count;
            }

            estimator = gamma_register_count_squared(nregs) / sum;

            if ((zero_count != 0) && (estimator < (5.0 * nregs / 2.0)))
                retval = nregs * log((double) nregs / zero_count);
            else if (estimator <= large_estimator_cutoff)
                retval = estimator;
            else
                return (-1 * two_to_l) * log(1.0 - (estimator/two_to_l));
        }
        break;

    case MST_UNDEFINED:
        // Our caller will convert this to a NULL.
        retval = -1.0;
        break;

    default:
        ereport("undefined multiset type value #8");
        break;
    }

    return retval;
}
