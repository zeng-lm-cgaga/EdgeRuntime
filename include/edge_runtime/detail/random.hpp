#ifndef EDGE_RUNTIME_DETAIL_RANDOM_HPP
#define EDGE_RUNTIME_DETAIL_RANDOM_HPP

#include <sys/random.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>

namespace edge_runtime::detail {

// getrandom() with EINTR retry. Used for instance nonce and test seeds.
inline bool random_bytes(void* out, size_t size) noexcept {
	auto* p = static_cast<uint8_t*>(out);
	size_t got = 0;
	while (got < size) {
		const ssize_t n = ::getrandom(p + got, size - got, 0);
		if (n < 0) {
			if (errno == EINTR) continue;
			return false;
		}
		got += static_cast<size_t>(n);
	}
	return true;
}

}  // namespace edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_RANDOM_HPP
