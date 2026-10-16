// Copyright (c) 2026 swift-tailcat contributors.
// SPDX-License-Identifier: BSD-3-Clause
// Finite concrete overloads select private Void/Never codecs; authors need no witnesses.
import Foundation

public func Handle<R: Request>(_: R.Type, handler: @escaping @Sendable (R.Input, Requests.Stream<R.Inbound, R.Yield>) async throws -> R.Output) -> Requests.Handler
where R.Inbound == Never, R.Input == Void, R.Output == Void, R.Yield == Never {
    makeHandler(RequestBindings<R>(inbound: .absent, input: .empty, output: .empty, yield: .absent), handler: handler)
}
extension Requests.Connection {
    public func open<R: Request>(_: R.Type, input: R.Input) async throws -> Requests.RequestStream<R.Yield, R.Inbound, R.Output>
    where R.Inbound == Never, R.Input == Void, R.Output == Void, R.Yield == Never {
        try await open(RequestBindings<R>(inbound: .absent, input: .empty, output: .empty, yield: .absent), input: input)
    }
}
extension Requests.Connection {
    public func open<R: Request>(_: R.Type) async throws -> Requests.RequestStream<R.Yield, R.Inbound, R.Output>
    where R.Inbound == Never, R.Input == Void, R.Output == Void, R.Yield == Never {
        try await open(RequestBindings<R>(inbound: .absent, input: .empty, output: .empty, yield: .absent), input: ())
    }
}
public func Handle<R: Request>(_: R.Type, handler: @escaping @Sendable (R.Input) async throws -> R.Output) -> Requests.Handler
where R.Inbound == Never, R.Input == Void, R.Output == Void, R.Yield == Never {
    makeHandler(RequestBindings<R>(inbound: .absent, input: .empty, output: .empty, yield: .absent)) { input, _ in try await handler(input) }
}
extension Requests.Connection {
    public func call<R: Request>(_: R.Type, input: R.Input) async throws -> R.Output
    where R.Inbound == Never, R.Input == Void, R.Output == Void, R.Yield == Never {
        try await call(RequestBindings<R>(inbound: .absent, input: .empty, output: .empty, yield: .absent), input: input)
    }
}
extension Requests.Connection {
    public func call<R: Request>(_: R.Type) async throws -> R.Output
    where R.Inbound == Never, R.Input == Void, R.Output == Void, R.Yield == Never {
        try await call(RequestBindings<R>(inbound: .absent, input: .empty, output: .empty, yield: .absent), input: ())
    }
}
public func Handle<R: Request>(_: R.Type, handler: @escaping @Sendable (R.Input, Requests.Stream<R.Inbound, R.Yield>) async throws -> R.Output) -> Requests.Handler
where R.Inbound: Codable, R.Input == Void, R.Output == Void, R.Yield == Never {
    makeHandler(RequestBindings<R>(inbound: .codable, input: .empty, output: .empty, yield: .absent), handler: handler)
}
extension Requests.Connection {
    public func open<R: Request>(_: R.Type, input: R.Input) async throws -> Requests.RequestStream<R.Yield, R.Inbound, R.Output>
    where R.Inbound: Codable, R.Input == Void, R.Output == Void, R.Yield == Never {
        try await open(RequestBindings<R>(inbound: .codable, input: .empty, output: .empty, yield: .absent), input: input)
    }
}
extension Requests.Connection {
    public func open<R: Request>(_: R.Type) async throws -> Requests.RequestStream<R.Yield, R.Inbound, R.Output>
    where R.Inbound: Codable, R.Input == Void, R.Output == Void, R.Yield == Never {
        try await open(RequestBindings<R>(inbound: .codable, input: .empty, output: .empty, yield: .absent), input: ())
    }
}
public func Handle<R: Request>(_: R.Type, handler: @escaping @Sendable (R.Input, Requests.Stream<R.Inbound, R.Yield>) async throws -> R.Output) -> Requests.Handler
where R.Inbound == Never, R.Input: Codable, R.Output == Void, R.Yield == Never {
    makeHandler(RequestBindings<R>(inbound: .absent, input: .codable, output: .empty, yield: .absent), handler: handler)
}
extension Requests.Connection {
    public func open<R: Request>(_: R.Type, input: R.Input) async throws -> Requests.RequestStream<R.Yield, R.Inbound, R.Output>
    where R.Inbound == Never, R.Input: Codable, R.Output == Void, R.Yield == Never {
        try await open(RequestBindings<R>(inbound: .absent, input: .codable, output: .empty, yield: .absent), input: input)
    }
}
public func Handle<R: Request>(_: R.Type, handler: @escaping @Sendable (R.Input) async throws -> R.Output) -> Requests.Handler
where R.Inbound == Never, R.Input: Codable, R.Output == Void, R.Yield == Never {
    makeHandler(RequestBindings<R>(inbound: .absent, input: .codable, output: .empty, yield: .absent)) { input, _ in try await handler(input) }
}
extension Requests.Connection {
    public func call<R: Request>(_: R.Type, input: R.Input) async throws -> R.Output
    where R.Inbound == Never, R.Input: Codable, R.Output == Void, R.Yield == Never {
        try await call(RequestBindings<R>(inbound: .absent, input: .codable, output: .empty, yield: .absent), input: input)
    }
}
public func Handle<R: Request>(_: R.Type, handler: @escaping @Sendable (R.Input, Requests.Stream<R.Inbound, R.Yield>) async throws -> R.Output) -> Requests.Handler
where R.Inbound: Codable, R.Input: Codable, R.Output == Void, R.Yield == Never {
    makeHandler(RequestBindings<R>(inbound: .codable, input: .codable, output: .empty, yield: .absent), handler: handler)
}
extension Requests.Connection {
    public func open<R: Request>(_: R.Type, input: R.Input) async throws -> Requests.RequestStream<R.Yield, R.Inbound, R.Output>
    where R.Inbound: Codable, R.Input: Codable, R.Output == Void, R.Yield == Never {
        try await open(RequestBindings<R>(inbound: .codable, input: .codable, output: .empty, yield: .absent), input: input)
    }
}
public func Handle<R: Request>(_: R.Type, handler: @escaping @Sendable (R.Input, Requests.Stream<R.Inbound, R.Yield>) async throws -> R.Output) -> Requests.Handler
where R.Inbound == Never, R.Input == Void, R.Output: Codable, R.Yield == Never {
    makeHandler(RequestBindings<R>(inbound: .absent, input: .empty, output: .codable, yield: .absent), handler: handler)
}
extension Requests.Connection {
    public func open<R: Request>(_: R.Type, input: R.Input) async throws -> Requests.RequestStream<R.Yield, R.Inbound, R.Output>
    where R.Inbound == Never, R.Input == Void, R.Output: Codable, R.Yield == Never {
        try await open(RequestBindings<R>(inbound: .absent, input: .empty, output: .codable, yield: .absent), input: input)
    }
}
extension Requests.Connection {
    public func open<R: Request>(_: R.Type) async throws -> Requests.RequestStream<R.Yield, R.Inbound, R.Output>
    where R.Inbound == Never, R.Input == Void, R.Output: Codable, R.Yield == Never {
        try await open(RequestBindings<R>(inbound: .absent, input: .empty, output: .codable, yield: .absent), input: ())
    }
}
public func Handle<R: Request>(_: R.Type, handler: @escaping @Sendable (R.Input) async throws -> R.Output) -> Requests.Handler
where R.Inbound == Never, R.Input == Void, R.Output: Codable, R.Yield == Never {
    makeHandler(RequestBindings<R>(inbound: .absent, input: .empty, output: .codable, yield: .absent)) { input, _ in try await handler(input) }
}
extension Requests.Connection {
    public func call<R: Request>(_: R.Type, input: R.Input) async throws -> R.Output
    where R.Inbound == Never, R.Input == Void, R.Output: Codable, R.Yield == Never {
        try await call(RequestBindings<R>(inbound: .absent, input: .empty, output: .codable, yield: .absent), input: input)
    }
}
extension Requests.Connection {
    public func call<R: Request>(_: R.Type) async throws -> R.Output
    where R.Inbound == Never, R.Input == Void, R.Output: Codable, R.Yield == Never {
        try await call(RequestBindings<R>(inbound: .absent, input: .empty, output: .codable, yield: .absent), input: ())
    }
}
public func Handle<R: Request>(_: R.Type, handler: @escaping @Sendable (R.Input, Requests.Stream<R.Inbound, R.Yield>) async throws -> R.Output) -> Requests.Handler
where R.Inbound: Codable, R.Input == Void, R.Output: Codable, R.Yield == Never {
    makeHandler(RequestBindings<R>(inbound: .codable, input: .empty, output: .codable, yield: .absent), handler: handler)
}
extension Requests.Connection {
    public func open<R: Request>(_: R.Type, input: R.Input) async throws -> Requests.RequestStream<R.Yield, R.Inbound, R.Output>
    where R.Inbound: Codable, R.Input == Void, R.Output: Codable, R.Yield == Never {
        try await open(RequestBindings<R>(inbound: .codable, input: .empty, output: .codable, yield: .absent), input: input)
    }
}
extension Requests.Connection {
    public func open<R: Request>(_: R.Type) async throws -> Requests.RequestStream<R.Yield, R.Inbound, R.Output>
    where R.Inbound: Codable, R.Input == Void, R.Output: Codable, R.Yield == Never {
        try await open(RequestBindings<R>(inbound: .codable, input: .empty, output: .codable, yield: .absent), input: ())
    }
}
public func Handle<R: Request>(_: R.Type, handler: @escaping @Sendable (R.Input, Requests.Stream<R.Inbound, R.Yield>) async throws -> R.Output) -> Requests.Handler
where R.Inbound == Never, R.Input: Codable, R.Output: Codable, R.Yield == Never {
    makeHandler(RequestBindings<R>(inbound: .absent, input: .codable, output: .codable, yield: .absent), handler: handler)
}
extension Requests.Connection {
    public func open<R: Request>(_: R.Type, input: R.Input) async throws -> Requests.RequestStream<R.Yield, R.Inbound, R.Output>
    where R.Inbound == Never, R.Input: Codable, R.Output: Codable, R.Yield == Never {
        try await open(RequestBindings<R>(inbound: .absent, input: .codable, output: .codable, yield: .absent), input: input)
    }
}
public func Handle<R: Request>(_: R.Type, handler: @escaping @Sendable (R.Input) async throws -> R.Output) -> Requests.Handler
where R.Inbound == Never, R.Input: Codable, R.Output: Codable, R.Yield == Never {
    makeHandler(RequestBindings<R>(inbound: .absent, input: .codable, output: .codable, yield: .absent)) { input, _ in try await handler(input) }
}
extension Requests.Connection {
    public func call<R: Request>(_: R.Type, input: R.Input) async throws -> R.Output
    where R.Inbound == Never, R.Input: Codable, R.Output: Codable, R.Yield == Never {
        try await call(RequestBindings<R>(inbound: .absent, input: .codable, output: .codable, yield: .absent), input: input)
    }
}
public func Handle<R: Request>(_: R.Type, handler: @escaping @Sendable (R.Input, Requests.Stream<R.Inbound, R.Yield>) async throws -> R.Output) -> Requests.Handler
where R.Inbound: Codable, R.Input: Codable, R.Output: Codable, R.Yield == Never {
    makeHandler(RequestBindings<R>(inbound: .codable, input: .codable, output: .codable, yield: .absent), handler: handler)
}
extension Requests.Connection {
    public func open<R: Request>(_: R.Type, input: R.Input) async throws -> Requests.RequestStream<R.Yield, R.Inbound, R.Output>
    where R.Inbound: Codable, R.Input: Codable, R.Output: Codable, R.Yield == Never {
        try await open(RequestBindings<R>(inbound: .codable, input: .codable, output: .codable, yield: .absent), input: input)
    }
}
public func Handle<R: Request>(_: R.Type, handler: @escaping @Sendable (R.Input, Requests.Stream<R.Inbound, R.Yield>) async throws -> R.Output) -> Requests.Handler
where R.Inbound == Never, R.Input == Void, R.Output == Void, R.Yield: Codable {
    makeHandler(RequestBindings<R>(inbound: .absent, input: .empty, output: .empty, yield: .codable), handler: handler)
}
extension Requests.Connection {
    public func open<R: Request>(_: R.Type, input: R.Input) async throws -> Requests.RequestStream<R.Yield, R.Inbound, R.Output>
    where R.Inbound == Never, R.Input == Void, R.Output == Void, R.Yield: Codable {
        try await open(RequestBindings<R>(inbound: .absent, input: .empty, output: .empty, yield: .codable), input: input)
    }
}
extension Requests.Connection {
    public func open<R: Request>(_: R.Type) async throws -> Requests.RequestStream<R.Yield, R.Inbound, R.Output>
    where R.Inbound == Never, R.Input == Void, R.Output == Void, R.Yield: Codable {
        try await open(RequestBindings<R>(inbound: .absent, input: .empty, output: .empty, yield: .codable), input: ())
    }
}
public func Handle<R: Request>(_: R.Type, handler: @escaping @Sendable (R.Input, Requests.Stream<R.Inbound, R.Yield>) async throws -> R.Output) -> Requests.Handler
where R.Inbound: Codable, R.Input == Void, R.Output == Void, R.Yield: Codable {
    makeHandler(RequestBindings<R>(inbound: .codable, input: .empty, output: .empty, yield: .codable), handler: handler)
}
extension Requests.Connection {
    public func open<R: Request>(_: R.Type, input: R.Input) async throws -> Requests.RequestStream<R.Yield, R.Inbound, R.Output>
    where R.Inbound: Codable, R.Input == Void, R.Output == Void, R.Yield: Codable {
        try await open(RequestBindings<R>(inbound: .codable, input: .empty, output: .empty, yield: .codable), input: input)
    }
}
extension Requests.Connection {
    public func open<R: Request>(_: R.Type) async throws -> Requests.RequestStream<R.Yield, R.Inbound, R.Output>
    where R.Inbound: Codable, R.Input == Void, R.Output == Void, R.Yield: Codable {
        try await open(RequestBindings<R>(inbound: .codable, input: .empty, output: .empty, yield: .codable), input: ())
    }
}
public func Handle<R: Request>(_: R.Type, handler: @escaping @Sendable (R.Input, Requests.Stream<R.Inbound, R.Yield>) async throws -> R.Output) -> Requests.Handler
where R.Inbound == Never, R.Input: Codable, R.Output == Void, R.Yield: Codable {
    makeHandler(RequestBindings<R>(inbound: .absent, input: .codable, output: .empty, yield: .codable), handler: handler)
}
extension Requests.Connection {
    public func open<R: Request>(_: R.Type, input: R.Input) async throws -> Requests.RequestStream<R.Yield, R.Inbound, R.Output>
    where R.Inbound == Never, R.Input: Codable, R.Output == Void, R.Yield: Codable {
        try await open(RequestBindings<R>(inbound: .absent, input: .codable, output: .empty, yield: .codable), input: input)
    }
}
public func Handle<R: Request>(_: R.Type, handler: @escaping @Sendable (R.Input, Requests.Stream<R.Inbound, R.Yield>) async throws -> R.Output) -> Requests.Handler
where R.Inbound: Codable, R.Input: Codable, R.Output == Void, R.Yield: Codable {
    makeHandler(RequestBindings<R>(inbound: .codable, input: .codable, output: .empty, yield: .codable), handler: handler)
}
extension Requests.Connection {
    public func open<R: Request>(_: R.Type, input: R.Input) async throws -> Requests.RequestStream<R.Yield, R.Inbound, R.Output>
    where R.Inbound: Codable, R.Input: Codable, R.Output == Void, R.Yield: Codable {
        try await open(RequestBindings<R>(inbound: .codable, input: .codable, output: .empty, yield: .codable), input: input)
    }
}
public func Handle<R: Request>(_: R.Type, handler: @escaping @Sendable (R.Input, Requests.Stream<R.Inbound, R.Yield>) async throws -> R.Output) -> Requests.Handler
where R.Inbound == Never, R.Input == Void, R.Output: Codable, R.Yield: Codable {
    makeHandler(RequestBindings<R>(inbound: .absent, input: .empty, output: .codable, yield: .codable), handler: handler)
}
extension Requests.Connection {
    public func open<R: Request>(_: R.Type, input: R.Input) async throws -> Requests.RequestStream<R.Yield, R.Inbound, R.Output>
    where R.Inbound == Never, R.Input == Void, R.Output: Codable, R.Yield: Codable {
        try await open(RequestBindings<R>(inbound: .absent, input: .empty, output: .codable, yield: .codable), input: input)
    }
}
extension Requests.Connection {
    public func open<R: Request>(_: R.Type) async throws -> Requests.RequestStream<R.Yield, R.Inbound, R.Output>
    where R.Inbound == Never, R.Input == Void, R.Output: Codable, R.Yield: Codable {
        try await open(RequestBindings<R>(inbound: .absent, input: .empty, output: .codable, yield: .codable), input: ())
    }
}
public func Handle<R: Request>(_: R.Type, handler: @escaping @Sendable (R.Input, Requests.Stream<R.Inbound, R.Yield>) async throws -> R.Output) -> Requests.Handler
where R.Inbound: Codable, R.Input == Void, R.Output: Codable, R.Yield: Codable {
    makeHandler(RequestBindings<R>(inbound: .codable, input: .empty, output: .codable, yield: .codable), handler: handler)
}
extension Requests.Connection {
    public func open<R: Request>(_: R.Type, input: R.Input) async throws -> Requests.RequestStream<R.Yield, R.Inbound, R.Output>
    where R.Inbound: Codable, R.Input == Void, R.Output: Codable, R.Yield: Codable {
        try await open(RequestBindings<R>(inbound: .codable, input: .empty, output: .codable, yield: .codable), input: input)
    }
}
extension Requests.Connection {
    public func open<R: Request>(_: R.Type) async throws -> Requests.RequestStream<R.Yield, R.Inbound, R.Output>
    where R.Inbound: Codable, R.Input == Void, R.Output: Codable, R.Yield: Codable {
        try await open(RequestBindings<R>(inbound: .codable, input: .empty, output: .codable, yield: .codable), input: ())
    }
}
public func Handle<R: Request>(_: R.Type, handler: @escaping @Sendable (R.Input, Requests.Stream<R.Inbound, R.Yield>) async throws -> R.Output) -> Requests.Handler
where R.Inbound == Never, R.Input: Codable, R.Output: Codable, R.Yield: Codable {
    makeHandler(RequestBindings<R>(inbound: .absent, input: .codable, output: .codable, yield: .codable), handler: handler)
}
extension Requests.Connection {
    public func open<R: Request>(_: R.Type, input: R.Input) async throws -> Requests.RequestStream<R.Yield, R.Inbound, R.Output>
    where R.Inbound == Never, R.Input: Codable, R.Output: Codable, R.Yield: Codable {
        try await open(RequestBindings<R>(inbound: .absent, input: .codable, output: .codable, yield: .codable), input: input)
    }
}
public func Handle<R: Request>(_: R.Type, handler: @escaping @Sendable (R.Input, Requests.Stream<R.Inbound, R.Yield>) async throws -> R.Output) -> Requests.Handler
where R.Inbound: Codable, R.Input: Codable, R.Output: Codable, R.Yield: Codable {
    makeHandler(RequestBindings<R>(inbound: .codable, input: .codable, output: .codable, yield: .codable), handler: handler)
}
extension Requests.Connection {
    public func open<R: Request>(_: R.Type, input: R.Input) async throws -> Requests.RequestStream<R.Yield, R.Inbound, R.Output>
    where R.Inbound: Codable, R.Input: Codable, R.Output: Codable, R.Yield: Codable {
        try await open(RequestBindings<R>(inbound: .codable, input: .codable, output: .codable, yield: .codable), input: input)
    }
}
