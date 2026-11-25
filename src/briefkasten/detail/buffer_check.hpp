// Copyright (c) 2021-2025 Tim Niklas Uhl
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
// FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
// COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
// IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
// CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#pragma once

/// Checks that memory handed to MPI stays put until MPI is done with it (BRIEFKASTEN_CHECK_BUFFERS, CMake option of
/// the same name): a send buffer keeps its address, size and contents from MPI_Isend to completion, no send slot is
/// reused or destroyed while its send is in flight, and an armed persistent receive buffer keeps its address and size.
/// A violation is a use-after-free under MPI, which a transport may report far away from the cause (Intel MPI's
/// single-copy CMA path: `cma_read_nbytes == size`) or not at all.
///
/// The environment variable BRIEFKASTEN_CHECK_BUFFERS selects what a build with the checks does, read once:
///   full (default): everything, including a checksum of every send buffer at MPI_Isend and at completion (slow:
///                   it reads every buffer twice, which changes the timing of a timing-dependent bug)
///   light:          addresses, sizes and request states only, nearly the timing of a build without checks
///   off:            nothing

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <print>
#include <ranges>
#include <span>
#include <string_view>

#include <mpi.h>

namespace briefkasten::internal {

enum class BufferCheckMode : std::uint8_t { off, light, full };

/// The mode from the environment variable BRIEFKASTEN_CHECK_BUFFERS (off / light / full, default full); an unknown
/// value aborts, so a typo does not silently run the wrong mode.
[[nodiscard]] inline BufferCheckMode buffer_check_mode();

/// Whether any check runs, and whether send buffers are checksummed.
[[nodiscard]] inline bool buffer_checks_enabled() {
    return buffer_check_mode() != BufferCheckMode::off;
}
[[nodiscard]] inline bool buffer_checksums_enabled() {
    return buffer_check_mode() == BufferCheckMode::full;
}

inline constexpr std::uint64_t FNV_OFFSET_BASIS = 14695981039346656037ULL;
inline constexpr std::uint64_t FNV_PRIME = 1099511628211ULL;

/// FNV-1a over the bytes of \p buffer.
template <std::ranges::contiguous_range Buffer>
[[nodiscard]] std::uint64_t buffer_checksum(Buffer const& buffer) {
    std::uint64_t hash = FNV_OFFSET_BASIS;
    for (std::byte byte : std::as_bytes(std::span(std::ranges::data(buffer), std::ranges::size(buffer)))) {
        hash ^= static_cast<std::uint64_t>(byte);
        hash *= FNV_PRIME;
    }
    return hash;
}

/// Reports a violation on stderr (with the world rank) and aborts the job.
[[noreturn]] inline void buffer_check_failed(char const* what) {
    int rank = -1;
    int initialized = 0;
    MPI_Initialized(&initialized);
    if (initialized != 0) {
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    }
    std::println(stderr, "[BriefKAsten buffer check, rank {}] {}", rank, what);
    std::fflush(stderr);
    if (initialized != 0) {
        MPI_Abort(MPI_COMM_WORLD, 3);
    }
    std::abort();
}

inline BufferCheckMode buffer_check_mode() {
    static BufferCheckMode const mode = [] {
        char const* value = std::getenv("BRIEFKASTEN_CHECK_BUFFERS");
        std::string_view const name = value == nullptr ? "full" : value;
        if (name == "full") {
            return BufferCheckMode::full;
        }
        if (name == "light") {
            return BufferCheckMode::light;
        }
        if (name == "off") {
            return BufferCheckMode::off;
        }
        buffer_check_failed("unknown BRIEFKASTEN_CHECK_BUFFERS (expected off, light or full)");
    }();
    return mode;
}

/// What a send slot or an armed receive was given to MPI.
struct BufferRecord {
    void const* data = nullptr;
    std::size_t size = 0;
    std::uint64_t checksum = 0;
};

}  // namespace briefkasten::internal
