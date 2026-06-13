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

import Foundation
import LibP2PTesting
import NIOCore
import Testing

@testable import LibP2P

extension LibP2PTests {

    /// Coverage for `SingleBufferingRequest.Style.responseExpectedThenHalfClose`
    /// — the canonical request/response client gesture (write request, FIN the
    /// write side, read the reply). Added with the directory-list interop work:
    /// a canonical responder (rust-libp2p `request_response`) reads the request
    /// to EOF before replying, so a client that never FIN'd blocked until the
    /// responder's inbound timeout. These tests pin the swift client behaviour
    /// that fixes that — and guard the multi-frame read-until-close reassembly.
    @Suite("Half-Close Request Tests")
    struct HalfCloseRequestTests {

        /// Two real apps over loopback TCP. Client uses the half-close style;
        /// the server echoes on `.data`. Asserts the round-trip completes and
        /// the body is intact (not truncated to the first frame).
        @Test("responseExpectedThenHalfClose round-trips an echo")
        func halfCloseRoundTrip() async throws {
            try await withApp { server in
                server.listen(.tcp)
                server.routes.group("halfclose") { group in
                    group.on("1.0.0") { req -> Response<ByteBuffer> in
                        switch req.event {
                        case .ready:
                            return .stayOpen
                        case .data(let payload):
                            return .respondThenClose(ByteBuffer(string: payload.string.uppercased()))
                        case .closed:
                            return .close
                        case .error(let error):
                            return .reset(error)
                        }
                    }
                }

                let addr = try server.listenAddresses.first!.encapsulate(
                    proto: .p2p, address: server.peerID.b58String)

                try await withApp { client in
                    client.listen(.tcp)
                    let response = try await client.newRequest(
                        to: addr,
                        forProtocol: "halfclose/1.0.0",
                        withRequest: Data("hello".utf8),
                        style: .responseExpectedThenHalfClose,
                        withTimeout: .seconds(5)
                    ).get()
                    #expect(String(decoding: response, as: UTF8.self) == "HELLO")
                }
            }
        }

        /// An *empty* request still has to FIN: the directory-list client sends
        /// no request bytes, only the half-close. A responder that reads to EOF
        /// must still see the stream end and reply. Mirrors that exact shape.
        @Test("responseExpectedThenHalfClose works with an empty request body")
        func halfCloseEmptyRequest() async throws {
            try await withApp { server in
                server.listen(.tcp)
                server.routes.group("halfclose-empty") { group in
                    group.on("1.0.0") { req -> Response<ByteBuffer> in
                        switch req.event {
                        case .ready:
                            // Respond on stream-open (the request carries no
                            // bytes); the client's FIN tears down cleanly after.
                            return .respondThenClose(ByteBuffer(string: "PONG"))
                        case .data:
                            return .stayOpen
                        case .closed:
                            return .close
                        case .error(let error):
                            return .reset(error)
                        }
                    }
                }

                let addr = try server.listenAddresses.first!.encapsulate(
                    proto: .p2p, address: server.peerID.b58String)

                try await withApp { client in
                    client.listen(.tcp)
                    let response = try await client.newRequest(
                        to: addr,
                        forProtocol: "halfclose-empty/1.0.0",
                        withRequest: Data(),
                        style: .responseExpectedThenHalfClose,
                        withTimeout: .seconds(5)
                    ).get()
                    #expect(String(decoding: response, as: UTF8.self) == "PONG")
                }
            }
        }
    }
}
