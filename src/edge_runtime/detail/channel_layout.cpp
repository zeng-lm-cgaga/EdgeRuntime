#include "edge_runtime/detail/channel_layout.hpp"

#include <unistd.h>

#include <string>

namespace edge_runtime::detail {

bool validate_channel_name(const char* name, size_t len) noexcept {
	if (name == nullptr) return false;
	if (len < 1 || len > kMaxChannelNameLen) return false;
	for (size_t i = 0; i < len; ++i) {
		const unsigned char c = static_cast<unsigned char>(name[i]);
		const bool allowed = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
		                     (c >= '0' && c <= '9') || c == '_' || c == '.' || c == '-';
		if (!allowed) return false;
	}
	// Reject ".." anywhere (design §7.1 path-traversal guard). '/' is already
	// rejected by the charset.
	for (size_t i = 0; i + 1 < len; ++i) {
		if (name[i] == '.' && name[i + 1] == '.') return false;
	}
	return true;
}

std::string channel_shm_name(const std::string& channel_name) {
	return "/edgeruntime." + std::to_string(getuid()) + "." + channel_name;
}

std::string channel_lock_path(const std::string& channel_name) {
	return "/run/user/" + std::to_string(getuid()) + "/edgeruntime/" + channel_name + ".lock";
}

}  // namespace edge_runtime::detail
