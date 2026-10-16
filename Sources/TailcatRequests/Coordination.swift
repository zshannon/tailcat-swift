// Copyright (c) 2026 swift-tailcat contributors.
// SPDX-License-Identifier: BSD-3-Clause
import Foundation

/// Bounded single-producer encoded inbox. No decoded messages are retained here.
final class MessageInbox: @unchecked Sendable {
    private let capacity: Int
    private var consumer: UUID?
    private var failure: (any Error)?
    private var finished = false
    private let lock = NSLock()
    private var pending: (Data, CheckedContinuation<Void, any Error>)?
    private var queue: [Data] = []
    private var waiting: CheckedContinuation<Data?, any Error>?
    init(capacity: Int) { self.capacity = capacity }
    func put(_ data: Data) async throws {
        try await withCheckedThrowingContinuation { continuation in
            let outcome = lock.withLock { () -> (CheckedContinuation<Data?, any Error>?, (any Error)?, Bool) in
                if let failure { return (nil, failure, true) }
                if finished { return (nil, MessageFailure.closed, true) }
                if let waiting { self.waiting = nil; return (waiting, nil, true) }
                if queue.count < capacity { queue.append(data); return (nil, nil, true) }
                precondition(pending == nil)
                pending = (data, continuation)
                return (nil, nil, false)
            }
            outcome.0?.resume(returning: data)
            if let error = outcome.1 { continuation.resume(throwing: error) }
            else if outcome.2 { continuation.resume() }
        }
    }
    func next(_ id: UUID) async throws -> Data? {
        try await withCheckedThrowingContinuation { continuation in
            let outcome = lock.withLock { () -> (Result<Data?, any Error>?, CheckedContinuation<Void, any Error>?) in
                guard (consumer == nil || consumer == id), waiting == nil else {
                    return (.failure(MessageFailure.consumerConflict), nil)
                }
                consumer = id
                if let failure { return (.failure(failure), nil) }
                if !queue.isEmpty {
                    let data = queue.removeFirst()
                    let admitted = pending
                    pending = nil
                    if let admitted { queue.append(admitted.0) }
                    return (.success(data), admitted?.1)
                }
                if finished { return (.success(nil), nil) }
                waiting = continuation
                return (nil, nil)
            }
            outcome.1?.resume()
            if let result = outcome.0 { continuation.resume(with: result) }
        }
    }
    func finish(_ error: (any Error)? = nil) {
        let continuations = lock.withLock { () -> (CheckedContinuation<Data?, any Error>?, CheckedContinuation<Void, any Error>?) in
            guard failure == nil, !finished || error != nil else { return (nil, nil) }
            finished = true
            if let error { failure = error; queue.removeAll() }
            let result = (waiting, pending?.1)
            waiting = nil; pending = nil
            return result
        }
        if let error { continuations.0?.resume(throwing: error) } else { continuations.0?.resume(returning: nil) }
        continuations.1?.resume(throwing: error ?? MessageFailure.closed)
    }
}

final class MessageResult: @unchecked Sendable {
    private var claimed = false
    private let lock = NSLock()
    private var result: Result<Data, any Error>?
    private var waiter: CheckedContinuation<Data, any Error>?
    func wait() async throws -> Data {
        try await withCheckedThrowingContinuation { continuation in
            let value = lock.withLock { () -> Result<Data, any Error>? in
                guard !claimed else { return .failure(MessageFailure.consumerConflict) }
                claimed = true
                if let result { self.result = nil; return result }
                waiter = continuation
                return nil
            }
            if let value { continuation.resume(with: value) }
        }
    }
    func complete(_ value: Result<Data, any Error>) {
        let target = lock.withLock { () -> CheckedContinuation<Data, any Error>? in
            guard result == nil, !(claimed && waiter == nil) else { return nil }
            if let waiter { self.waiter = nil; return waiter }
            result = value
            return nil
        }
        target?.resume(with: value)
    }
}

/// A bounded FIFO lease admits one encoder plus whole-frame writer at a time.
final class MessageSendGate: @unchecked Sendable {
    private var active = false
    private var drain: [CheckedContinuation<Void, Never>] = []
    private var closed = false
    private let limit: Int
    private let lock = NSLock()
    private var waiters: [CheckedContinuation<Void, any Error>] = []
    init(limit: Int) { self.limit = limit }
    var pendingCount: Int { lock.withLock { waiters.count } }
    func enter() async throws {
        try Task.checkCancellation()
        try await withCheckedThrowingContinuation { continuation in
            let value = lock.withLock { () -> Result<Void, any Error>? in
                if closed { return .failure(MessageFailure.closed) }
                if !active { active = true; return .success(()) }
                guard waiters.count < limit else { return .failure(MessageFailure.capacity) }
                waiters.append(continuation)
                return nil
            }
            if let value { continuation.resume(with: value) }
        }
    }
    func leave() {
        let next = lock.withLock { () -> CheckedContinuation<Void, any Error>? in
            if !waiters.isEmpty { return waiters.removeFirst() }
            active = false
            return nil
        }
        next?.resume()
        let done = lock.withLock { () -> [CheckedContinuation<Void, Never>] in
            guard !active else { return [] }
            let done = drain; drain.removeAll(); return done
        }
        done.forEach { $0.resume() }
    }
    func join() async {
        await withCheckedContinuation { continuation in
            let ready = lock.withLock {
                if !active { return true }
                drain.append(continuation); return false
            }
            if ready { continuation.resume() }
        }
    }
    func stop() {
        let pending = lock.withLock { closed = true; let pending = waiters; waiters.removeAll(); return pending }
        pending.forEach { $0.resume(throwing: MessageFailure.closed) }
    }
}

/// Task handles never capture their public owner. Shutdown interrupts transport before joining.
final class MessageTasks: @unchecked Sendable {
    private var closed = false
    private let lock = NSLock()
    private var tasks: [UUID: Task<Void, Never>] = [:]
    private var waiters: [CheckedContinuation<Void, Never>] = []
    @discardableResult func start(limit: Int = .max, _ work: @escaping @Sendable () async -> Void) -> Bool {
        lock.withLock {
            guard !closed, tasks.count < limit else { return false }
            let id = UUID()
            tasks[id] = Task { await work(); self.finished(id) }
            return true
        }
    }
    private func finished(_ id: UUID) {
        let waiting = lock.withLock {
            tasks.removeValue(forKey: id)
            guard tasks.isEmpty else { return [CheckedContinuation<Void, Never>]() }
            let waiting = waiters; waiters.removeAll(); return waiting
        }
        waiting.forEach { $0.resume() }
    }
    func stop() {
        let pending = lock.withLock { closed = true; return Array(tasks.values) }
        pending.forEach { $0.cancel() }
    }
    func join() async {
        await withCheckedContinuation { continuation in
            let immediate = lock.withLock {
                if tasks.isEmpty { return true }
                waiters.append(continuation); return false
            }
            if immediate { continuation.resume() }
        }
    }
}

/// Copies of an iterator share admission so concurrent next() never races decoding.
final class MessageConsumer: @unchecked Sendable {
    let id = UUID()
    private var active = false
    private let lock = NSLock()
    func enter() throws { try lock.withLock {
        guard !active else { throw MessageFailure.consumerConflict }
        active = true
    } }
    func leave() { lock.withLock { active = false } }
}
