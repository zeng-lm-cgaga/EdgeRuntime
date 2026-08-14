#ifndef EDGE_RUNTIME_DETAIL_PRODUCER_IMPL_HPP
#define EDGE_RUNTIME_DETAIL_PRODUCER_IMPL_HPP

#include <sys/socket.h>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include "edge_runtime/channel_options.hpp"
#include "edge_runtime/detail/process_identity.hpp"
#include "edge_runtime/detail/shm_object.hpp"
#include "edge_runtime/result.hpp"
#include "edge_runtime/sample.hpp"
#include "edge_runtime/schema.hpp"

namespace edge_runtime::detail {

// State owned by a Producer<T> handle (design §15.6: frozen channel name,
// inode, generation, instance nonce, schema). publish() re-validates the
// frozen identity against the shared header before touching payload.
struct ProducerHandle {
	ShmObject shm;
	std::string channel_name;
	uint64_t generation{0};
	uint64_t role_epoch{0};  // frozen even producer role_epoch
	uint64_t instance_nonce_hi{0};
	uint64_t instance_nonce_lo{0};
	std::array<std::byte, 32> schema_fingerprint{};
	uint32_t schema_version{0};
	uint32_t payload_size{0};
	ProcessIdentity self{};
	std::atomic<bool> operation_in_use{false};  // same-handle overlap guard (§18.1)

	// v0.2 fd-pass transport (design §33). Members are declared AFTER shm
	// (whose mapping the serving thread reads) so destruction joins the thread
	// before munmap runs.
	Transport transport{Transport::kPosixShm};
	uint32_t channel_hash{0};  // frozen for the serving thread (never a local)
	std::string socket_path;
	UniqueFd listen_fd;
	std::atomic<bool> serve_stop{false};
	std::thread server_thread;
	bool socket_unlinked{false};

	// v0.2 optional heartbeat (design §34): frozen interval in ns (0 = off).
	uint64_t heartbeat_interval_ns{0};

	~ProducerHandle() {
		// Join the serving thread before the mapping dies (design §18.2). The
		// wake uses shutdown(), never close(): close would leave a fd number a
		// concurrent accept could reuse.
		if (server_thread.joinable()) {
			serve_stop.store(true, std::memory_order_relaxed);
			if (listen_fd.get() >= 0) {
				(void)::shutdown(listen_fd.get(), SHUT_RDWR);
			}
			server_thread.join();
		}
	}
	ProducerHandle(const ProducerHandle&) = delete;
	ProducerHandle& operator=(const ProducerHandle&) = delete;
	ProducerHandle() = default;
};

// Full §9.1 create sequence. payload_size comes from PayloadCodec<T> at the
// public template boundary; impl never touches T.
Result<std::shared_ptr<ProducerHandle>> producer_create_impl(const ChannelOptions& options,
                                                             const SchemaDescriptor& schema,
                                                             uint32_t payload_size);

// §11.1 publish. The public template encodes T into `encoded`; the impl only
// operates on the encoded bytes and the ABI header. Returns StaleHandle when
// the shared header no longer matches the frozen identity, so an old handle
// never writes into a replaced instance.
Result<PublishInfo> producer_publish_impl(const std::shared_ptr<ProducerHandle>& handle,
                                          const std::byte* encoded, uint32_t encoded_size) noexcept;

// Slow-check diagnostic (design §15.6): revalidates the name still resolves to
// the frozen inode, then snapshots the channel state.
Result<ChannelStatus> producer_status_impl(const std::shared_ptr<ProducerHandle>& handle) noexcept;

// Frozen generation accessor (design §15.6). The public header only sees a
// forward-declared handle, so the value is fetched here.
uint64_t producer_generation_impl(const std::shared_ptr<ProducerHandle>& handle) noexcept;

// Best-effort clean shutdown (design §15.2): marks producer_state OFFLINE on
// the shared header only if the producer identity is still ours. Destructor
// path; never reports errors.
void producer_shutdown_impl(const std::shared_ptr<ProducerHandle>& handle) noexcept;

// Explicit, verified removal (§9.4 inode-checked unlink + journal REMOVING).
Result<void> producer_remove_if_owner_impl(const std::shared_ptr<ProducerHandle>& handle) noexcept;

// v0.2 optional heartbeat (design §34): application-declared making-progress.
// Same stale guards as publish; only ever writes heartbeat_boot_ns.
Result<void> producer_heartbeat_impl(const std::shared_ptr<ProducerHandle>& handle) noexcept;

}  // namespace edge_runtime::detail

#endif  // EDGE_RUNTIME_DETAIL_PRODUCER_IMPL_HPP
