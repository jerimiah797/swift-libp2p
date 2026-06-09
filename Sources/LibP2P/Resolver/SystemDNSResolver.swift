//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-libp2p open source project
//
// Copyright (c) 2022-2025 swift-libp2p project authors
// Licensed under MIT
//
// See LICENSE for license information
// See CONTRIBUTORS for the list of swift-libp2p project authors
//
// SPDX-License-Identifier: MIT
//
//===----------------------------------------------------------------------===//

import Dispatch
import LibP2PCore
import NIOCore

#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#endif

/// Resolves `/dns`, `/dns4` and `/dns6` multiaddr components to concrete
/// `/ip4` / `/ip6` addresses using the host's system resolver (`getaddrinfo`).
///
/// This mirrors the behavior of `rust-libp2p`'s `libp2p-dns` transport
/// (`transports/dns/src/lib.rs`), which is the reference implementation:
///
/// - `/dns4/<name>`  → A    records → `/ip4/<addr>`
/// - `/dns6/<name>`  → AAAA records → `/ip6/<addr>`
/// - `/dns/<name>`   → A + AAAA     → `/ip4/<addr>` and/or `/ip6/<addr>`
///
/// In every case only the *leading* dns component is replaced; the remainder
/// of the multiaddr (e.g. `/tcp/7373/p2p/<peer-id>`) is preserved verbatim, so
/// `/dns4/example.org/tcp/7373/p2p/Qm…` resolves to
/// `/ip4/93.184.216.34/tcp/7373/p2p/Qm…`.
///
/// `/dnsaddr` (TXT `_dnsaddr.<name>` lookups) is **not** handled here — that
/// requires a TXT query (`res_query`), which `getaddrinfo` cannot perform. It
/// remains a follow-up; this resolver returns `nil` for `/dnsaddr` so a future
/// TXT resolver can own that codec.
public struct SystemDNSResolver: AddressResolver {
    public static var key: String { "system-dns" }

    private let eventLoopGroup: EventLoopGroup

    public init(eventLoopGroup: EventLoopGroup) {
        self.eventLoopGroup = eventLoopGroup
    }

    public func resolve(multiaddr ma: Multiaddr) -> EventLoopFuture<[Multiaddr]?> {
        let el = eventLoopGroup.next()

        guard let first = ma.addresses.first, let host = first.addr, !host.isEmpty else {
            return el.makeSucceededFuture(nil)
        }

        let family: Int32
        switch first.codec {
        case .dns4: family = AF_INET
        case .dns6: family = AF_INET6
        case .dns: family = AF_UNSPEC
        default:
            // `/dnsaddr` (TXT) and any non-dns codec are not ours to resolve.
            return el.makeSucceededFuture(nil)
        }

        let tail = Array(ma.addresses.dropFirst())
        let promise = el.makePromise(of: [Multiaddr]?.self)

        // `getaddrinfo` blocks; never run it on an event-loop thread.
        DispatchQueue.global(qos: .userInitiated).async {
            let records = Self.systemLookup(host: host, family: family)
            guard !records.isEmpty else {
                promise.succeed(nil)
                return
            }

            let resolved: [Multiaddr] = records.compactMap { record in
                try? Self.rebuild(ipCodec: record.codec, ip: record.address, tail: tail)
            }

            promise.succeed(resolved.isEmpty ? nil : resolved)
        }

        return promise.futureResult
    }

    public func resolve(
        multiaddr ma: Multiaddr,
        for codecs: Set<MultiaddrProtocol>
    ) -> EventLoopFuture<Multiaddr?> {
        resolve(multiaddr: ma).map { resolved in
            guard let resolved = resolved else { return nil }
            return resolved.first(where: { Set($0.protocols()).isSuperset(of: codecs) })
                ?? resolved.first
        }
    }

    // MARK: - Helpers

    /// One resolved address: the concrete IP codec (`.ip4`/`.ip6`) and its
    /// numeric string form.
    private struct Record {
        let codec: MultiaddrProtocol
        let address: String
    }

    /// Synchronously resolves `host` via `getaddrinfo`, returning the numeric
    /// IPv4/IPv6 results in resolver order, de-duplicated. `SOCK_STREAM` is
    /// requested so we get one entry per address rather than one per socket
    /// type. Link-local IPv6 results carrying a `%scope` suffix are dropped —
    /// they are not dialable across hosts.
    private static func systemLookup(host: String, family: Int32) -> [Record] {
        var hints = addrinfo()
        hints.ai_family = family
        hints.ai_socktype = SOCK_STREAM

        var rawResult: UnsafeMutablePointer<addrinfo>?
        guard getaddrinfo(host, nil, &hints, &rawResult) == 0, let head = rawResult else {
            return []
        }
        defer { freeaddrinfo(head) }

        var records: [Record] = []
        var seen = Set<String>()
        var cursor: UnsafeMutablePointer<addrinfo>? = head

        while let node = cursor {
            let info = node.pointee
            if let sockaddr = info.ai_addr {
                var nameBuffer = [CChar](repeating: 0, count: 1025)  // NI_MAXHOST
                let rc = getnameinfo(
                    sockaddr,
                    info.ai_addrlen,
                    &nameBuffer,
                    socklen_t(nameBuffer.count),
                    nil,
                    0,
                    NI_NUMERICHOST
                )
                if rc == 0 {
                    let ip = String(cString: nameBuffer)
                    let codec: MultiaddrProtocol = info.ai_family == AF_INET6 ? .ip6 : .ip4
                    if !ip.contains("%"), seen.insert(ip).inserted {
                        records.append(Record(codec: codec, address: ip))
                    }
                }
            }
            cursor = info.ai_next
        }

        return records
    }

    /// Rebuilds a multiaddr from a resolved IP component plus the preserved
    /// tail (everything after the original dns component).
    private static func rebuild(
        ipCodec: MultiaddrProtocol,
        ip: String,
        tail: [Address]
    ) throws -> Multiaddr {
        var ma = try Multiaddr(ipCodec, address: ip)
        for component in tail {
            ma = try ma.encapsulate(proto: component.codec, address: component.addr)
        }
        return ma
    }
}
