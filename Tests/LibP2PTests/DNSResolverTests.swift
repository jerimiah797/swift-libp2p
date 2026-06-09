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

import LibP2PTesting
import NIOCore
import Testing

extension LibP2PTests {

    @Suite("DNS Resolver Tests", .serialized)
    struct DNSResolverTests {

        /// `/dns4/<name>` resolves via the system resolver (A records) to an
        /// `/ip4` address, preserving the trailing `/tcp` component.
        /// `localhost` resolves to `127.0.0.1` from `/etc/hosts`, so this is
        /// hermetic (no network).
        @Test func resolvesDns4ToIp4PreservingTail() async throws {
            try await withApp { app in
                let resolved = try await app.resolve(Multiaddr("/dns4/localhost/tcp/7373")).get()
                let addresses = try #require(resolved)
                #expect(
                    addresses.contains(try Multiaddr("/ip4/127.0.0.1/tcp/7373"))
                )
                // Every resolved address must carry the preserved tail.
                for address in addresses {
                    #expect(address.tcpAddress?.port == 7373)
                }
            }
        }

        /// `/dns/<name>` requests both A and AAAA. `localhost` yields at least
        /// the IPv4 loopback; the resolved set must include `/ip4/127.0.0.1`.
        @Test func resolvesDnsToLoopback() async throws {
            try await withApp { app in
                let resolved = try await app.resolve(Multiaddr("/dns/localhost/tcp/7373")).get()
                let addresses = try #require(resolved)
                #expect(addresses.contains(try Multiaddr("/ip4/127.0.0.1/tcp/7373")))
            }
        }

        /// A bare `/dns4/<name>` with no port still resolves to `/ip4/<addr>`.
        @Test func resolvesBareDns4() async throws {
            try await withApp { app in
                let resolved = try await app.resolve(Multiaddr("/dns4/localhost")).get()
                let addresses = try #require(resolved)
                #expect(addresses.contains(try Multiaddr("/ip4/127.0.0.1")))
            }
        }

        /// A non-dns multiaddr is returned unresolved (`nil`) — the resolver
        /// only owns the dns family.
        @Test func leavesNonDnsUnresolved() async throws {
            try await withApp { app in
                let resolved = try await app.resolve(Multiaddr("/ip4/127.0.0.1/tcp/7373")).get()
                #expect(resolved == nil)
            }
        }
    }
}
