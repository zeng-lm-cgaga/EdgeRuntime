#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <inttypes.h>
#include <string>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace {

constexpr char kLegacyMagic[] = "EDGMPMC1";
constexpr uint32_t kEndianMarker = 0x01020304u;
constexpr uint32_t kLegacyHeaderSize = 256;

struct LegacyOwner {
	uint64_t pid;
	uint64_t proc_start_ticks;
	uint64_t boot_id_hash_hi;
	uint64_t boot_id_hash_lo;
};

struct alignas(64) LegacyHeader {
	char magic[8];
	uint16_t abi_major;
	uint16_t abi_minor;
	uint32_t header_size;
	uint32_t endian_marker;
	uint32_t capacity;
	uint32_t payload_size;
	uint32_t schema_version;
	uint32_t reserved0;
	uint64_t mapping_size;
	uint8_t schema_fingerprint[32];
	uint64_t generation;
	uint64_t instance_nonce_hi;
	uint64_t instance_nonce_lo;
	LegacyOwner creator;
	uint64_t enqueue_position;
	uint64_t dequeue_position;
	uint32_t enqueue_lock;
	uint32_t dequeue_lock;
	uint32_t init_state;
	uint32_t reserved1;
	LegacyOwner enqueue_owner;
	LegacyOwner dequeue_owner;
	uint64_t header_checksum;
	uint32_t notify_epoch;
	uint8_t reserved[12];
};

static_assert(sizeof(LegacyHeader) == kLegacyHeaderSize, "legacy header shape");

const char* arg_value(int argc, char** argv, const char* name) {
	for (int i = 1; i + 1 < argc; ++i) {
		if (std::strcmp(argv[i], name) == 0) return argv[i + 1];
	}
	return nullptr;
}

uint32_t arg_u32(int argc, char** argv, const char* name, uint32_t fallback) {
	const char* value = arg_value(argc, argv, name);
	if (value == nullptr) return fallback;
	return static_cast<uint32_t>(std::strtoul(value, nullptr, 10));
}

std::string shm_name(const char* name) {
	return "/edgeruntime.mpmc." + std::to_string(static_cast<unsigned long long>(getuid())) +
	       "." + name;
}

uint64_t mapping_size_for(uint32_t capacity, uint32_t payload_size) {
	const uint64_t stride =
	        ((sizeof(uint64_t) * 8u + payload_size + 63u) / 64u) * 64u;
	return kLegacyHeaderSize + static_cast<uint64_t>(capacity) * stride;
}

int create_legacy(const std::string& name, uint32_t capacity, uint32_t payload_size) {
	const uint64_t mapping_size = mapping_size_for(capacity, payload_size);
	const int fd = shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
	if (fd < 0) return 5;
	if (ftruncate(fd, static_cast<off_t>(mapping_size)) != 0) {
		(void)close(fd);
		(void)shm_unlink(name.c_str());
		return 6;
	}
	void* mapping = mmap(nullptr, static_cast<size_t>(mapping_size), PROT_READ | PROT_WRITE,
	                     MAP_SHARED, fd, 0);
	if (mapping == MAP_FAILED) {
		(void)close(fd);
		(void)shm_unlink(name.c_str());
		return 7;
	}
	std::memset(mapping, 0, static_cast<size_t>(mapping_size));
	auto* header = static_cast<LegacyHeader*>(mapping);
	std::memcpy(header->magic, kLegacyMagic, sizeof(header->magic));
	header->abi_major = 1;
	header->abi_minor = 0;
	header->header_size = kLegacyHeaderSize;
	header->endian_marker = kEndianMarker;
	header->capacity = capacity;
	header->payload_size = payload_size;
	header->schema_version = 1;
	header->mapping_size = mapping_size;
	header->generation = 1;
	header->init_state = 2;
	(void)msync(mapping, static_cast<size_t>(mapping_size), MS_SYNC);
	(void)munmap(mapping, static_cast<size_t>(mapping_size));
	(void)close(fd);
	std::printf("LEGACY_CREATED name=%s size=%" PRIu64 "\n", name.c_str(), mapping_size);
	return 0;
}

int open_legacy(const std::string& name) {
	const int fd = shm_open(name.c_str(), O_RDWR, 0600);
	if (fd < 0) return 5;
	LegacyHeader header{};
	const ssize_t read_count = pread(fd, &header, sizeof(header), 0);
	(void)close(fd);
	if (read_count != static_cast<ssize_t>(sizeof(header))) return 6;
	if (std::memcmp(header.magic, kLegacyMagic, sizeof(header.magic)) != 0 ||
	    header.abi_major != 1 || header.header_size != kLegacyHeaderSize) {
		std::printf("LEGACY_REJECT code=AbiMismatch\n");
		return 0;
	}
	std::printf("LEGACY_ACCEPT\n");
	return 0;
}

}  // namespace

int main(int argc, char** argv) {
	const char* mode = arg_value(argc, argv, "--mode");
	const char* raw_name = arg_value(argc, argv, "--name");
	if (mode == nullptr || raw_name == nullptr) return 2;
	const std::string name = shm_name(raw_name);
	if (std::strcmp(mode, "create") == 0) {
		return create_legacy(name, arg_u32(argc, argv, "--capacity", 1),
		                     arg_u32(argc, argv, "--payload-size", 16));
	}
	if (std::strcmp(mode, "open") == 0) return open_legacy(name);
	return 2;
}
