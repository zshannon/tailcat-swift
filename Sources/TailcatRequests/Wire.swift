// Copyright (c) 2026 swift-tailcat contributors.
// SPDX-License-Identifier: BSD-3-Clause
import Foundation

public protocol Request: Sendable {
    associatedtype Inbound: Sendable = Never
    associatedtype Input: Sendable = Void
    associatedtype Output: Sendable = Void
    associatedtype Yield: Sendable = Never
    static var event: String { get }
    static var version: Int { get }
}
extension Request { public static var version: Int { 1 } }

extension Requests {
    public enum Messaging {
        public struct Configuration: Sendable {
            public var maxBufferedMessages: Int
            public var maxEncodedMessageBytes: Int
            public var maxPendingSends: Int
            public init(maxBufferedMessages: Int = 2, maxEncodedMessageBytes: Int = 8 * 1024 * 1024, maxPendingSends: Int = 16) {
                self.maxBufferedMessages = maxBufferedMessages
                self.maxEncodedMessageBytes = maxEncodedMessageBytes
                self.maxPendingSends = maxPendingSends
            }
            func validate() throws {
                guard (1...65535).contains(maxBufferedMessages), (1...Int(UInt32.max)).contains(maxEncodedMessageBytes),
                      (1...65535).contains(maxPendingSends) else { throw Failure.invalidConfiguration }
            }
        }
        public enum Failure: Error, Equatable, Sendable {
            case capacity, closed, ownershipConflict, consumerConflict, invalidConfiguration, invalidRequest, malformedFrame, oversizedMessage
            case remote(code: String, message: String)
        }
        /// Local lifecycle only; ready does not establish remote or relay reachability.
        public enum Status: Sendable { case ready, closing, closed }
        public struct Context: Sendable {
            public let addresses: Requests.ConnectionAddresses
            public let event: String
            public let version: Int
        }
    }
}

typealias MessageFailure = Requests.Messaging.Failure
struct MessageRoute: Codable, Hashable, Sendable {
    let event: String
    let version: Int
    func validate() throws {
        guard (1...128).contains(event.utf8.count), event.utf8.allSatisfy({ (33...126).contains($0) }),
              (1...65535).contains(version) else { throw MessageFailure.invalidRequest }
    }
}
struct MessageRemoteError: Codable, Sendable { let code: String; let message: String }
struct MessageFrame: Equatable, Sendable {
    enum Kind: UInt8, Sendable { case open = 1, input, data, output, end, error }
    let kind: Kind
    var payload = Data()
    static func limit(_ kind: Kind, maximum: Int) -> Int {
        switch kind { case .open: 512; case .error: 1024; case .end: 0; default: maximum }
    }
    func encoded(limit: Int) throws -> Data {
        guard payload.count <= Self.limit(kind, maximum: limit), payload.count <= Int(UInt32.max) else {
            throw MessageFailure.oversizedMessage
        }
        let length = UInt32(payload.count)
        return Data([84, 67, 65, 84, 1, kind.rawValue, UInt8(truncatingIfNeeded: length >> 24),
                     UInt8(truncatingIfNeeded: length >> 16), UInt8(truncatingIfNeeded: length >> 8),
                     UInt8(truncatingIfNeeded: length)]) + payload
    }
}
/// Exactly one task reads a flow. Limits are checked on the header before payload allocation.
final class MessageReader: Sendable {
    let raw: MessageTransport
    let limit: Int
    init(raw: MessageTransport, limit: Int) { self.raw = raw; self.limit = limit }
    func read() async throws -> MessageFrame? {
        guard let header = try await exact(10, allowEOF: true) else { return nil }
        guard header.prefix(4) == Data([84,67,65,84]), header[4] == 1,
              let kind = MessageFrame.Kind(rawValue: header[5]) else { throw MessageFailure.malformedFrame }
        let length = header.suffix(4).reduce(0) { ($0 << 8) | Int($1) }
        guard length <= MessageFrame.limit(kind, maximum: limit) else { throw MessageFailure.oversizedMessage }
        return MessageFrame(kind: kind, payload: try await exact(length, allowEOF: false) ?? Data())
    }
    private func exact(_ count: Int, allowEOF: Bool) async throws -> Data? {
        var bytes = Data()
        while bytes.count < count {
            guard let chunk = try await raw.read(maxBytes: min(65536, count - bytes.count)) else {
                if bytes.isEmpty && allowEOF { return nil }
                throw MessageFailure.malformedFrame
            }
            guard !chunk.isEmpty, chunk.count <= count - bytes.count else { throw MessageFailure.malformedFrame }
            bytes.append(chunk)
        }
        return bytes
    }
}
struct MessageCodec<Value: Sendable>: Sendable {
    let decode: @Sendable (Data) throws -> Value
    let encode: @Sendable (Value) throws -> Data
    var absent = false
}
extension MessageCodec where Value: Codable {
    static var codable: Self { Self(decode: {
        guard !$0.isEmpty else { throw MessageFailure.malformedFrame }
        return try JSONDecoder().decode(Value.self, from: $0)
    }, encode: {
        let encoder = JSONEncoder()
        // Base64 may consist almost entirely of '/'; escaping each slash would
        // double a 4 MiB envelope past the default 8 MiB wire limit.
        encoder.outputFormatting = [.withoutEscapingSlashes]
        return try encoder.encode($0)
    }) }
}
extension MessageCodec where Value == Void {
    static var empty: Self { Self(decode: {
        guard $0.isEmpty else { throw MessageFailure.malformedFrame }; return ()
    }, encode: { _ in Data() }) }
}
extension MessageCodec {
    static var absent: Self { Self(decode: { _ in throw MessageFailure.malformedFrame }, encode: { _ in throw MessageFailure.malformedFrame }, absent: true) }
}
