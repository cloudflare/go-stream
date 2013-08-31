/**

Code to wrap the postgres-hll extra in go-ness.

https://github.com/aggregateknowledge/postgresql-hll

This exposes the following functions:

* Make new hll:

hll_empty4

 * Merge two hll's together

hll_union

 * Add a string to an hll

hll_hash_varlena

 * serialize hll to []byte

multiset_pack or byteaout

This is designed to allow the go code to create an HLL, add values to it, and then serialize it in the same format that PG can read.

*/

// Defaults if type modifier values are not specified.
//
#define DEFAULT_LOG2M		11
#define DEFAULT_REGWIDTH	5
#define DEFAULT_EXPTHRESH	-1
#define DEFAULT_SPARSEON	1

// ----------------------------------------------------------------
// Aggregating Data Structure
// ----------------------------------------------------------------

typedef int64_t        int64;
typedef uint64_t       uint64;
typedef int32_t        int32;

typedef struct
{
    size_t		mse_nelem;
    uint64_t	mse_elems[0];

} ms_explicit_t;

// Defines the *unpacked* register.
typedef uint8_t compreg_t;

typedef struct
{
    compreg_t	msc_regs[0];

} ms_compressed_t;

// Size of the compressed or explicit data.
#define MS_MAXDATA		(128 * 1024)

typedef struct
{
    size_t		ms_nbits;
    size_t		ms_nregs;
    size_t		ms_log2nregs;
    int64		ms_expthresh;
    bool		ms_sparseon;

	uint64_t	ms_type;	// size is only for alignment.

    union
    {
        // MST_EMPTY and MST_UNDEFINED don't need data.
        // MST_SPARSE is only used in the packed format.
        //
        ms_explicit_t	as_expl;	// MST_EXPLICIT
        ms_compressed_t	as_comp;	// MST_COMPRESSED
        uint8_t			as_size[MS_MAXDATA];	// sizes the union.

    }		ms_data;

} multiset_t;

typedef struct
{
    size_t			brc_nbits;	// Read size.
    uint32_t		brc_mask;	// Read mask.
    uint8_t const *	brc_curp;	// Current byte.
    size_t			brc_used;	// Used bits.

} bitstream_read_cursor_t;

// Create an empty multiset with parameters.
// @TODO -- malloc's a pointer but doesn't free it. Must remember to free when done.
multiset_t* hll_empty4(int32 log2m, int32 regwidth, int64 expthresh, int32 sparseon);

multiset_t* hll_empty();

void multiset_union(multiset_t * o_msap, multiset_t const * i_msbp);

// Hash a varlena object.
uint64 hll_hash_varlena(const char* key, int len, int seed);

// And the hashed value to the hll
void multiset_add(multiset_t * o_msp, uint64_t element);

// Returns a human readable digest version of the hll
char* multiset_tostring(multiset_t const * i_msp);

// Returns the packed size of the input ms
size_t multiset_packed_size(multiset_t const * i_msp);

// Returns a serialized version of the hll
void multiset_pack(multiset_t const * i_msp, uint8_t * o_bitp, size_t i_size);

// Returns a serialized version of the hll
uint8_t* multiset_pack_wrap(multiset_t const * i_msp, size_t i_size);
