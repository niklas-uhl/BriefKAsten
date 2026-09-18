// Copyright (c) 2021-2026 Tim Niklas Uhl
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

#include <cstdint>

namespace briefkasten {

/// @brief What the far end of a link is obliged to do with what arrives over it.
///
/// This is a property of the LINK, not of the individual record travelling over it, which is what makes one
/// buffer and one credit counter per peer sufficient: no per-record classes, no per-record inspection, and
/// whole-buffer flush stays legal.
///
/// On GridIndirectionScheme the two classes are "same column" and "different column", and the mapping was
/// verified exhaustively for every p in [1, 200) plus 256/512/768/1000/1024/3072/6144/12288, ragged
/// fallback included (get_proxy's `{from.column, to.column}` branch):
///
///   - every relay hop (proxy -> destination) is a same-column link;
///   - a destination in our own column is always reached in ONE hop, so a same-column link never carries
///     first-hop traffic. Same-column links are therefore terminal-only.
///
/// CAREFUL, and this is the one place the shape is less tidy than "one class per link" suggests: a
/// different-column link is MIXED. A peer in our own row is reached directly (get_proxy returns the
/// destination itself when it shares our row), so the very same link carries both traffic destined for that
/// peer and traffic it must relay into its column. What is uniform per link is the RECEIVER'S OBLIGATION,
/// which is the only thing flow control needs: on a \ref to_proxy link an arrival *may* have to be
/// forwarded, so the receiver must grant conservatively against its relay reserve; on a \ref to_destination
/// link an arrival never will be, so the receiver may grant freely against terminal consumption.
///
/// That asymmetry is exactly the deadlock argument. The dependency chain
///
///     different-column send -> same-column send -> delivery
///
/// is acyclic: a \ref to_proxy send can be blocked only by the relay's same-column credits, a \ref
/// to_destination send only by the far end consuming, and consuming is terminal -- it never waits on a send.
///
/// GENERALISATION. Per-link credits work for any *link-ranked* scheme: a d-dimensional grid, a hypercube
/// with a fixed bit order, a k-ary n-fly. For H hops the relay budgets must be per incoming rank, i.e. H-1
/// structured pools. Schemes that reuse a link at different hop counts -- wrapped butterfly, de Bruijn,
/// Valiant routing -- break the argument above and need per-record classes instead. The technique is not
/// universal and should not be written up as if it were.
enum class LinkClass : std::uint8_t {
    /// The terminal hop. Everything arriving over this link is consumed locally and never forwarded, so the
    /// receiver can return credit as soon as its handler has run.
    to_destination,
    /// The relay hop. An arrival over this link may have to be forwarded on a \ref to_destination link, so
    /// the receiver must have reserved forwarding space before it grants the credit that admitted it.
    to_proxy,
};

}  // namespace briefkasten
