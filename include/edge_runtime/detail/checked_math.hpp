#ifndef EDGE_RUNTIME_DETAIL_CHECKED_MATH_HPP
#define EDGE_RUNTIME_DETAIL_CHECKED_MATH_HPP

#include <cstdint>

namespace edge_runtime::detail {

constexpr bool checked_add_u64(uint64_t a, uint64_t b, uint64_t* out) noexcept {
	if (b > UINT64_MAX - a) return false;
	*out = a + b;
	return true;
}

constexpr bool checked_mul_u64(uint64_t a, uint64_t b, uint64_t* out) noexcept {
	if (a != 0 && b > UINT64_MAX / a) return false;
	*out = a * b;
	return true;
}

// Rounds v up to the next multiple of align (align must be nonzero). Used to
// keep every slot on a 64-byte boundary (design §8.1).
constexpr bool round_up_to_multiple_u64(uint64_t v, uint64_t align, uint64_t* out) noexcept {
	if (align == 0) return false;
	const uint64_t rem = v % align;
	if (rem == 0) {
		*out = v;
		return true;
	}
	const uint64_t delta = align - rem;
	if (v > UINT64_MAX - delta) return false;
	*out = v + delta;
	return true;
}

}  // namespace edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_CHECKED_MATH_HPP
