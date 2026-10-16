// Copyright (c) 2026 swift-tailcat contributors.
// SPDX-License-Identifier: BSD-3-Clause
import Foundation
import Tailcat

/// Typed request/response and streaming messages over native Tailcat TCP streams.
public enum Requests {
    public struct ConnectionAddresses: Sendable {
        public let localAddress: Data
        public let remoteAddress: Data
        public let localPort: UInt16
        public let remotePort: UInt16
    }
}

/// Bridges synchronous cancellation into the native actor-based connection.
/// The lock protects the single cleanup task; close always joins it.
final class MessageTransport: @unchecked Sendable {
    let connection: TailcatConnection
    private let lock = NSLock()
    private var cleanup: Task<Void, Never>?
    init(_ connection: TailcatConnection) { self.connection = connection }
    var addresses: Requests.ConnectionAddresses {
        .init(localAddress: connection.localAddress, remoteAddress: connection.remoteAddress,
              localPort: connection.localPort, remotePort: connection.remotePort)
    }
    func read(maxBytes: Int) async throws -> Data? {
        let data = try await connection.read(maximumBytes: maxBytes)
        return data.isEmpty ? nil : data
    }
    func write(_ data: Data) async throws { try await connection.write(data) }
    func closeWrite() async throws { try await connection.closeWrite() }
    func requestShutdown(abort: Bool = true) {
        lock.withLock {
            guard cleanup == nil else { return }
            cleanup = Task { [connection] in
                if abort { await connection.abort() }
                else { await connection.close() }
            }
        }
    }
    func close() async throws {
        requestShutdown(abort: false)
        let task = lock.withLock { cleanup }
        await task?.value
    }
}
