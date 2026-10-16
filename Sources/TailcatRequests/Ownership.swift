// Copyright (c) 2026 swift-tailcat contributors.
// SPDX-License-Identifier: BSD-3-Clause
import Foundation

/// All mutable ownership state is protected by `lock`. Cleanup callbacks and
/// continuation resumes execute after releasing every lock.
final class Ownership: @unchecked Sendable {
    private let abort: @Sendable () -> Void
    private var children: [ObjectIdentifier: Ownership] = [:]
    private var claimed = false
    private let completion = CloseCompletion()
    private let finish: @Sendable () async throws -> Void
    private var isClosing = false
    private let lock = NSLock()
    private weak var parent: Ownership?

    init(abort: @escaping @Sendable () -> Void, close: @escaping @Sendable () async throws -> Void) {
        self.abort = abort
        finish = close
    }

    deinit { requestShutdown() }

    func checkOpen() throws {
        try lock.withLock { if isClosing { throw MessageFailure.closed } }
    }

    var transferOwner: Ownership? { lock.withLock { parent } }

    /// Pair locks have a stable address order, including invalid provider graphs.
    /// Already-owned returns are rejected before deciding whether to dispose them.
    func attach(_ child: Ownership) throws {
        guard child !== self else { throw MessageFailure.ownershipConflict }
        let first = UInt(bitPattern: Unmanaged.passUnretained(self).toOpaque())
            < UInt(bitPattern: Unmanaged.passUnretained(child).toOpaque()) ? self : child
        let second = first === self ? child : self
        first.lock.lock()
        second.lock.lock()
        defer { second.lock.unlock(); first.lock.unlock() }
        guard !child.claimed else { throw MessageFailure.ownershipConflict }
        child.claimed = true
        guard !isClosing else { throw MessageFailure.closed }
        child.parent = self
        children[ObjectIdentifier(child)] = child
    }

    fileprivate func callbackScope(inheriting owners: Set<ObjectIdentifier>) -> Set<ObjectIdentifier> {
        var owners = owners
        var current: Ownership? = self
        while let owner = current {
            owners.insert(ObjectIdentifier(owner))
            current = owner.transferOwner
        }
        return owners
    }

    func requestShutdown() { requestShutdown(inheriting: CleanupScope.owners) }

    fileprivate func requestShutdown(inheriting owners: Set<ObjectIdentifier>) {
        let work = lock.withLock { () -> [Ownership]? in
            guard !isClosing else { return nil }
            isClosing = true
            return Array(children.values)
        }
        guard let children = work else { return }
        let callbackOwners = callbackScope(inheriting: owners)
        // Synchronous interruption precedes joining and may forward shutdown.
        CleanupScope.$owners.withValue(callbackOwners) { abort() }
        for child in children { child.requestShutdown(inheriting: callbackOwners) }
        let completion = completion
        let finish = finish
        let id = ObjectIdentifier(self)
        let parent = lock.withLock { self.parent }
        Task.detached {
            var failure: (any Error)?
            for child in children {
                do { try await child.close() } catch { if failure == nil { failure = error } }
            }
            do { try await CleanupScope.$owners.withValue(callbackOwners) { try await finish() } } catch {
                if failure == nil { failure = error }
            }
            if let failure {
                completion.complete(.failure(failure))
            } else {
                completion.complete(.success(()))
            }
            parent?.remove(id)
        }
    }

    func close() async throws {
        guard !CleanupScope.owners.contains(ObjectIdentifier(self)) else { throw MessageFailure.ownershipConflict }
        requestShutdown()
        try await completion.wait()
    }

    private func remove(_ id: ObjectIdentifier) { _ = lock.withLock { children.removeValue(forKey: id) } }
}


private enum CleanupScope {
    @TaskLocal static var owners: Set<ObjectIdentifier> = []
}

/// The result and waiters are guarded by the lock. Close waiters are deliberately
/// uncancelled so every caller observes the single completed cleanup outcome.
fileprivate final class CloseCompletion: @unchecked Sendable {
    private let lock = NSLock()
    private var result: Result<Void, any Error>?
    private var waiters: [CheckedContinuation<Void, any Error>] = []
    func complete(_ result: Result<Void, any Error>) {
        let pending = lock.withLock {
            guard self.result == nil else { return [CheckedContinuation<Void, any Error>]() }
            self.result = result
            let pending = waiters
            waiters.removeAll()
            return pending
        }
        for waiter in pending { waiter.resume(with: result) }
    }
    func wait() async throws {
        try await withCheckedThrowingContinuation { waiter in
            let result: Result<Void, any Error>? = lock.withLock {
                if let result = self.result { return result }
                waiters.append(waiter)
                return nil
            }
            if let result { waiter.resume(with: result) }
        }
    }
}
