// Copyright (c) 2026 swift-tailcat contributors.
// SPDX-License-Identifier: BSD-3-Clause
import Foundation
import Tailcat

extension Requests {
    /// Borrows a native client. Closing this owner closes its requests, not the client.
    public final class Connection: Sendable {
        public struct Configuration: Sendable {
            public var maxConcurrentRequests: Int
            public var messaging: Messaging.Configuration
            public init(maxConcurrentRequests: Int = 16, messaging: Messaging.Configuration = .init()) {
                self.maxConcurrentRequests = maxConcurrentRequests
                self.messaging = messaging
            }
            func validate() throws {
                try messaging.validate()
                guard (1...65535).contains(maxConcurrentRequests) else { throw MessageFailure.invalidConfiguration }
            }
        }
        let configuration: Configuration
        let capacity: MessageCapacity
        let dialer: MessageDialer
        let ownership: Ownership

        public convenience init(client: TailcatClient, port: UInt16 = 1, configuration: Configuration = .init()) throws {
            guard port != 0 else { throw TailcatTransportError.invalidPort }
            try self.init(configuration: configuration) { try await client.openTCP(port: port) }
        }
        init(configuration: Configuration = .init(), open: @escaping @Sendable () async throws -> TailcatConnection) throws {
            try configuration.validate()
            self.configuration = configuration
            let capacity = MessageCapacity(limit: configuration.maxConcurrentRequests)
            let dialer = MessageDialer(open: open)
            self.capacity = capacity; self.dialer = dialer
            ownership = Ownership(abort: { capacity.stop(); dialer.stop() }, close: { await capacity.join() })
        }
        deinit { ownership.requestShutdown() }
        public func requestShutdown() { ownership.requestShutdown() }
        public func close() async throws { try await ownership.close() }

        func open<R>(_ bindings: RequestBindings<R>, input: R.Input) async throws -> RequestStream<R.Yield, R.Inbound, R.Output> {
            try Task.checkCancellation()
            let route = MessageRoute(event: R.event, version: R.version)
            try route.validate()
            try ownership.checkOpen()
            try capacity.enter()
            var transferred = false
            defer { if !transferred { capacity.leave() } }
            let inputData = try bindings.input.encode(input)
            guard inputData.count <= configuration.messaging.maxEncodedMessageBytes else { throw MessageFailure.oversizedMessage }
            let raw = MessageTransport(try await dialer.open())
            let flow = MessageFlow(configuration: configuration.messaging,
                                   onClose: { [capacity] in capacity.leave() }, raw: raw, route: route)
            transferred = true
            do {
                try ownership.attach(flow.ownership)
                try await flow.operation {
                    try Task.checkCancellation()
                    try await flow.state.begin(route: route, input: inputData)
                    flow.state.startReadingClient(absent: bindings.yield.absent) { [ownership = flow.ownership] in
                        ownership.requestShutdown()
                    }
                }
                return RequestStream(connection: self, incoming: Incoming(codec: bindings.yield, flow: flow),
                                     flow: flow, inboundCodec: bindings.inbound, outputCodec: bindings.output)
            } catch { await flow.close(); throw error }
        }
        func call<R>(_ bindings: RequestBindings<R>, input: R.Input) async throws -> R.Output {
            let stream = try await open(bindings, input: input)
            do {
                try await stream.finishSending()
                let output = try await stream.result()
                try await stream.flow.ownership.close()
                return output
            } catch { await stream.resetAndWait(); throw error }
        }
    }

    /// Owns the supplied TCP listener and accepted requests. The native server remains caller-owned.
    public final class Listener: Sendable {
        public struct Configuration: Sendable {
            public var maxConnections: Int
            public var messaging: Messaging.Configuration
            /// Bounds framing, handler execution, and response delivery; requires iOS 16+ or macOS 13+.
            public var requestTimeout: TimeInterval?
            fileprivate var sleep: @Sendable (TimeInterval) async throws -> Void = { timeout in
                if #available(iOS 16, macOS 13, *) {
                    try await ContinuousClock().sleep(for: .seconds(timeout))
                } else {
                    throw MessageFailure.invalidConfiguration
                }
            }
            public init(maxConnections: Int = 16, messaging: Messaging.Configuration = .init(), requestTimeout: TimeInterval? = nil) {
                self.maxConnections = maxConnections; self.messaging = messaging
                self.requestTimeout = requestTimeout
            }
            /// Supplies a clock for deterministic request deadlines.
            @available(iOS 16, macOS 13, *)
            public init<C: Clock>(clock: C, maxConnections: Int = 16, messaging: Messaging.Configuration = .init(), requestTimeout: TimeInterval? = nil) where C.Duration == Duration {
                self.init(maxConnections: maxConnections, messaging: messaging, requestTimeout: requestTimeout)
                sleep = { try await clock.sleep(for: .seconds($0)) }
            }
            func validate() throws {
                try messaging.validate()
                guard (1...65535).contains(maxConnections) else { throw MessageFailure.invalidConfiguration }
                if let requestTimeout {
                    guard requestTimeout > 0, requestTimeout.isFinite, requestTimeout < Double(Int64.max) else {
                        throw MessageFailure.invalidConfiguration
                    }
                    guard #available(iOS 16, macOS 13, *) else { throw MessageFailure.invalidConfiguration }
                }
            }
        }
        public let port: UInt16
        private let ownership: Ownership
        private let taskID: UUID

        public init(listener: TailcatListener, configuration: Configuration = .init(), @HandlerBuilder handlers: () -> [Handler]) throws {
            try configuration.validate()
            var registrations: [MessageRoute: Handler] = [:]
            for handler in handlers() {
                try handler.route.validate()
                guard registrations.updateValue(handler, forKey: handler.route) == nil else { throw MessageFailure.invalidRequest }
            }
            let snapshot = registrations
            let acceptTask = MessageTasks(), handlerTasks = MessageTasks()
            let taskID = UUID()
            self.taskID = taskID
            port = listener.port
            ownership = Ownership(abort: { acceptTask.stop(); handlerTasks.stop() }, close: {
                await listener.close()
                await acceptTask.join()
                await handlerTasks.join()
            })
            acceptTask.start { [weak owner = ownership] in
                do {
                    while !Task.isCancelled {
                        let raw = try await listener.accept()
                        if !handlerTasks.start(limit: configuration.maxConnections, {
                            await MessageTaskScope.$owners.withValue(MessageTaskScope.owners.union([taskID])) {
                                await serveMessage(configuration: configuration, handlers: snapshot, raw: MessageTransport(raw))
                            }
                        }) { await raw.abort() }
                    }
                } catch { owner?.requestShutdown() }
            }
        }
        deinit { ownership.requestShutdown() }
        public func requestShutdown() { ownership.requestShutdown() }
        public func close() async throws {
            guard !MessageTaskScope.owners.contains(taskID) else { throw MessageFailure.ownershipConflict }
            try await ownership.close()
        }
    }
}

extension TailcatServer {
    public func listenRequests(port: UInt16 = 1, configuration: Requests.Listener.Configuration = .init(),
                               @HandlerBuilder handlers: () -> [Requests.Handler]) async throws -> Requests.Listener {
        try configuration.validate()
        let registrations = handlers()
        let listener = try await listen(port: port)
        do { return try Requests.Listener(listener: listener, configuration: configuration) { registrations } }
        catch { await listener.close(); throw error }
    }
}

private func serveMessage(configuration: Requests.Listener.Configuration, handlers: [MessageRoute: Requests.Handler], raw: MessageTransport) async {
    guard let timeout = configuration.requestTimeout else {
        await serveMessage(configuration: configuration.messaging, handlers: handlers, raw: raw)
        return
    }
    if #available(iOS 16, macOS 13, *) {
        await withTaskGroup(of: Void.self) { group in
            group.addTask { await serveMessage(configuration: configuration.messaging, handlers: handlers, raw: raw) }
            group.addTask {
                do { try await configuration.sleep(timeout) }
                catch { return }
            }
            await group.next()
            group.cancelAll()
        }
    }
}

private func serveMessage(configuration: Requests.Messaging.Configuration, handlers: [MessageRoute: Requests.Handler], raw: MessageTransport) async {
    let reader = MessageReader(raw: raw, limit: configuration.maxEncodedMessageBytes)
    var flow: MessageFlow?
    await withTaskCancellationHandler {
        do {
            try Task.checkCancellation()
            guard let open = try await reader.read(), open.kind == .open else { throw MessageFailure.malformedFrame }
            let route = try JSONDecoder().decode(MessageRoute.self, from: open.payload)
            try route.validate()
            guard let input = try await reader.read(), input.kind == .input else { throw MessageFailure.malformedFrame }
            let request = MessageFlow(configuration: configuration, raw: raw, route: route)
            flow = request
            guard let handler = handlers[route] else { throw MessageFailure.invalidRequest }
            try await handler.invoke(input.payload, request)
        } catch {
            let error = MessageRemoteError(code: "request_failed", message: "Request could not be completed")
            if !Task.isCancelled, let payload = try? JSONEncoder().encode(error) {
                if let flow { try? await flow.state.terminal(MessageFrame(kind: .error, payload: payload)) }
                else {
                    try? await raw.write(MessageFrame(kind: .error, payload: payload).encoded(limit: configuration.maxEncodedMessageBytes))
                    try? await raw.write(MessageFrame(kind: .end).encoded(limit: configuration.maxEncodedMessageBytes))
                    try? await raw.closeWrite()
                }
            }
        }
        if let flow { await flow.close() }
        else { try? await raw.close() }
    } onCancel: { raw.requestShutdown() }
}

private enum MessageTaskScope {
    @TaskLocal static var owners: Set<UUID> = []
}

/// Pending dials are cancelled by either the caller or connection shutdown.
final class MessageDialer: @unchecked Sendable {
    private let lock = NSLock()
    private var closed = false
    private var tasks: [UUID: Task<TailcatConnection, Error>] = [:]
    private let connect: @Sendable () async throws -> TailcatConnection
    init(open: @escaping @Sendable () async throws -> TailcatConnection) { connect = open }
    func open() async throws -> TailcatConnection {
        let id = UUID()
        let task = try lock.withLock {
            guard !closed else { throw MessageFailure.closed }
            let task = Task { [connect] in try await connect() }
            tasks[id] = task
            return task
        }
        defer { _ = lock.withLock { tasks.removeValue(forKey: id) } }
        return try await withTaskCancellationHandler {
            let connection = try await task.value
            if Task.isCancelled || lock.withLock({ closed }) {
                await connection.abort()
                throw CancellationError()
            }
            return connection
        } onCancel: { task.cancel() }
    }
    func stop() {
        let pending = lock.withLock { closed = true; return Array(tasks.values) }
        pending.forEach { $0.cancel() }
    }
}

final class MessageCapacity: @unchecked Sendable {
    private var closed = false
    private var count = 0
    private let limit: Int
    private let lock = NSLock()
    private var waiters: [CheckedContinuation<Void, Never>] = []
    init(limit: Int) { self.limit = limit }
    func enter() throws { try lock.withLock {
        if closed { throw MessageFailure.closed }
        guard count < limit else { throw MessageFailure.capacity }
        count += 1
    } }
    func leave() {
        let done = lock.withLock { () -> [CheckedContinuation<Void, Never>] in
            count -= 1
            guard count == 0 else { return [] }
            let done = waiters; waiters.removeAll(); return done
        }
        done.forEach { $0.resume() }
    }
    func stop() { lock.withLock { closed = true } }
    func join() async { await withCheckedContinuation { continuation in
        let ready = lock.withLock { if count == 0 { return true }; waiters.append(continuation); return false }
        if ready { continuation.resume() }
    } }
}

