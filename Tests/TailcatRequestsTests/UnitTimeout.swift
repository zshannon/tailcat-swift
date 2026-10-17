import Testing

/// A wall-clock watchdog only; deadline behavior is driven by TestClock.
struct UnitTimeout: TestTrait, TestScoping {
    func provideScope(
        for test: Test,
        testCase: Test.Case?,
        performing function: @Sendable () async throws -> Void
    ) async throws {
        guard #available(iOS 16, macOS 13, *) else { throw UnitTimeoutError.unavailable }
        let deadline = ContinuousClock.now.advanced(by: .milliseconds(100))
        try await withoutActuallyEscaping(function) { function in
            try await withThrowingTaskGroup(of: Void.self) { group in
                defer { group.cancelAll() }
                group.addTask { try await function() }
                group.addTask {
                    try await ContinuousClock().sleep(until: deadline)
                    throw UnitTimeoutError.exceeded
                }
                _ = try await group.next()
                guard ContinuousClock.now < deadline else { throw UnitTimeoutError.exceeded }
            }
        }
        guard ContinuousClock.now < deadline else { throw UnitTimeoutError.exceeded }
    }
}

private enum UnitTimeoutError: Error { case exceeded, unavailable }

extension Trait where Self == UnitTimeout {
    static var unitTimeout: Self { Self() }
}
