// Copyright (c) 2026 swift-tailcat contributors.
// SPDX-License-Identifier: BSD-3-Clause
import Foundation

/// The reader and writer capture this state, never the public stream or its owner.
final class MessageFlowState: @unchecked Sendable {
    let inbox: MessageInbox
    let reader: MessageReader
    let result = MessageResult()
    let tasks = MessageTasks()
    let writer: MessageSendGate
    private var completed = false
    private var terminalSent = false
    private let lock = NSLock()
    private var revoked = false
    private var stopped = false
    init(configuration: Requests.Messaging.Configuration, raw: MessageTransport) {
        inbox = MessageInbox(capacity: configuration.maxBufferedMessages)
        reader = MessageReader(raw: raw, limit: configuration.maxEncodedMessageBytes)
        writer = MessageSendGate(limit: configuration.maxPendingSends)
    }
    func stop() {
        let completion = lock.withLock { () -> (Bool, Bool)? in
            if stopped { return nil }
            stopped = true; revoked = true
            return (self.completed, terminalSent)
        }
        guard let (completed, terminalSent) = completion else { return }
        reader.raw.requestShutdown(abort: !(completed || terminalSent))
        if !completed {
            inbox.finish(MessageFailure.closed)
            result.complete(.failure(MessageFailure.closed))
        }
        writer.stop()
        tasks.stop()
    }
    var isComplete: Bool { lock.withLock { completed } }
    var isStopped: Bool { lock.withLock { stopped } }
    func fail(_ error: any Error) {
        result.complete(.failure(error))
        inbox.finish(error)
        stop()
    }
    func revoke() { lock.withLock { revoked = true } }
    func write<Value>(_ value: Value, codec: MessageCodec<Value>, kind: MessageFrame.Kind) async throws {
        try await writer.enter()
        defer { writer.leave() }
        try lock.withLock { if revoked || stopped { throw MessageFailure.closed } }
        try Task.checkCancellation()
        let payload = try codec.encode(value)
        let frame = try MessageFrame(kind: kind, payload: payload).encoded(limit: reader.limit)
        do { try await reader.raw.write(frame) }
        catch { fail(error); throw error }
    }
    func begin(route: MessageRoute, input: Data) async throws {
        try await writer.enter()
        defer { writer.leave() }
        try await reader.raw.write(MessageFrame(kind: .open, payload: JSONEncoder().encode(route)).encoded(limit: reader.limit))
        try await reader.raw.write(MessageFrame(kind: .input, payload: input).encoded(limit: reader.limit))
    }
    func endInput() async throws {
        if isComplete { return }
        do { try await writer.enter() }
        catch { if isComplete { return }; throw error }
        defer { writer.leave() }
        let shouldEnd = try lock.withLock {
            if completed { return false }
            if stopped { throw MessageFailure.closed }
            if revoked { return false }
            revoked = true
            return true
        }
        guard shouldEnd else { return }
        do {
            try await reader.raw.write(MessageFrame(kind: .end).encoded(limit: reader.limit))
            try await reader.raw.closeWrite()
        } catch { if isComplete { return }; fail(error); throw error }
    }
    func terminal<Value>(_ value: Value, codec: MessageCodec<Value>) async throws {
        try await terminal { MessageFrame(kind: .output, payload: try codec.encode(value)) }
    }
    func terminal(_ frame: MessageFrame) async throws { try await terminal { frame } }
    private func terminal(_ makeFrame: () throws -> MessageFrame) async throws {
        revoke()
        try await writer.enter()
        defer { writer.leave() }
        let frame = try makeFrame().encoded(limit: reader.limit)
        do {
            try await reader.raw.write(frame)
            try await reader.raw.write(MessageFrame(kind: .end).encoded(limit: reader.limit))
            try await reader.raw.closeWrite()
            lock.withLock { terminalSent = true }
        } catch { fail(error); throw error }
    }
    func startReadingClient(absent: Bool, onComplete: @escaping @Sendable () -> Void) {
        tasks.start { [self] in
            do {
                while let frame = try await reader.read() {
                    switch frame.kind {
                    case .data:
                        guard !absent, !frame.payload.isEmpty else { throw MessageFailure.malformedFrame }
                        try await inbox.put(frame.payload)
                    case .output, .error:
                        guard let end = try await reader.read(), end.kind == .end,
                              try await reader.read() == nil else { throw MessageFailure.malformedFrame }
                        let terminalError: MessageFailure?
                        if frame.kind == .error {
                            let error = try JSONDecoder().decode(MessageRemoteError.self, from: frame.payload)
                            terminalError = .remote(code: error.code, message: error.message)
                        } else { terminalError = nil }
                        // A remote error is a complete response, not a broken transport.
                        // Preserve it when the reply races the client's input half-close.
                        lock.withLock { completed = true; revoked = true }
                        if let terminalError { result.complete(.failure(terminalError)) }
                        else { result.complete(.success(frame.payload)) }
                        inbox.finish(terminalError)
                        onComplete()
                        return
                    default: throw MessageFailure.malformedFrame
                    }
                }
                throw MessageFailure.malformedFrame
            } catch { fail(error); onComplete() }
        }
    }
    func startReadingServer(absent: Bool) {
        tasks.start { [self] in
            do {
                while let frame = try await reader.read() {
                    switch frame.kind {
                    case .data:
                        guard !absent, !frame.payload.isEmpty else { throw MessageFailure.malformedFrame }
                        try await inbox.put(frame.payload)
                    case .end:
                        guard try await reader.read() == nil else { throw MessageFailure.malformedFrame }
                        inbox.finish()
                        result.complete(.success(Data()))
                        return
                    default: throw MessageFailure.malformedFrame
                    }
                }
                throw MessageFailure.malformedFrame
            } catch { fail(error) }
        }
    }
}

final class MessageFlow: Sendable {
    let context: Requests.Messaging.Context
    let ownership: Ownership
    let state: MessageFlowState
    init(configuration: Requests.Messaging.Configuration, onClose: @escaping @Sendable () -> Void = {}, raw: MessageTransport, route: MessageRoute) {
        context = .init(addresses: raw.addresses, event: route.event, version: route.version)
        let state = MessageFlowState(configuration: configuration, raw: raw)
        self.state = state
        ownership = Ownership(abort: { state.stop() }, close: {
            defer { onClose() }
            await state.tasks.join()
            await state.writer.join()
            try await raw.close()
        })
    }
    deinit { ownership.requestShutdown() }
    func close() async { try? await ownership.close() }
    func operation<Value>(_ body: () async throws -> Value) async throws -> Value {
        try await withTaskCancellationHandler {
            do {
                try Task.checkCancellation()
                let value = try await body()
                try Task.checkCancellation()
                return value
            }
            catch {
                if Task.isCancelled || state.isStopped { ownership.requestShutdown(); await close() }
                throw error
            }
        } onCancel: { self.ownership.requestShutdown() }
    }
}

extension Requests {
    public struct Incoming<Value: Sendable>: AsyncSequence, Sendable {
        public typealias Element = Value
        let codec: MessageCodec<Value>
        let flow: MessageFlow
        public func makeAsyncIterator() -> AsyncIterator { AsyncIterator(codec: codec, flow: flow) }
        public struct AsyncIterator: AsyncIteratorProtocol {
            let codec: MessageCodec<Value>
            let flow: MessageFlow
            private let consumer = MessageConsumer()
            init(codec: MessageCodec<Value>, flow: MessageFlow) { self.codec = codec; self.flow = flow }
            public mutating func next() async throws -> Value? {
                let codec = codec, flow = flow, consumer = consumer
                try consumer.enter()
                defer { consumer.leave() }
                return try await flow.operation {
                    guard let data = try await flow.state.inbox.next(consumer.id) else {
                        if flow.state.isComplete { try await flow.ownership.close() }
                        return nil
                    }
                    do { return try codec.decode(data) }
                    catch { flow.state.fail(error); await flow.close(); throw error }
                }
            }
        }
    }
    public struct Stream<Inbound: Sendable, Yield: Sendable>: Sendable {
        public var context: Messaging.Context { flow.context }
        public let incoming: Incoming<Inbound>
        let codec: MessageCodec<Yield>
        let flow: MessageFlow
        /// Ends the ongoing yield lane. The handler's returned output remains separate.
        public func finishSending() async throws { try await flow.operation { flow.state.revoke() } }
        public func yield(_ value: Yield) async throws {
            try await flow.operation { try await flow.state.write(value, codec: codec, kind: .data) }
        }
    }
    public struct RequestStream<Yield: Sendable, Inbound: Sendable, Output: Sendable>: Sendable {
        public let connection: Connection
        public var context: Messaging.Context { flow.context }
        public let incoming: Incoming<Yield>
        let flow: MessageFlow
        let inboundCodec: MessageCodec<Inbound>
        let outputCodec: MessageCodec<Output>
        public func finishSending() async throws { try await flow.operation { try await flow.state.endInput() } }
        public func resetAndWait() async { await flow.close() }
        /// One lifetime consumer. Success requires OUTPUT, END and EOF in that order.
        public func result() async throws -> Output {
            try await flow.operation {
                let data = try await flow.state.result.wait()
                do {
                    let output = try outputCodec.decode(data)
                    try await flow.ownership.close()
                    return output
                }
                catch { flow.state.fail(error); await flow.close(); throw error }
            }
        }
        public func send(_ value: Inbound) async throws {
            try await flow.operation { try await flow.state.write(value, codec: inboundCodec, kind: .data) }
        }
    }
}
