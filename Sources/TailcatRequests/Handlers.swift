// Copyright (c) 2026 swift-tailcat contributors.
// SPDX-License-Identifier: BSD-3-Clause
import Foundation

struct RequestBindings<R: Request>: Sendable {
    let inbound: MessageCodec<R.Inbound>
    let input: MessageCodec<R.Input>
    let output: MessageCodec<R.Output>
    let yield: MessageCodec<R.Yield>
}
extension Requests {
    public struct Handler: Sendable {
        public var event: String { route.event }
        public var version: Int { route.version }
        let route: MessageRoute
        let invoke: @Sendable (Data, MessageFlow) async throws -> Void
    }
}
@resultBuilder public enum HandlerBuilder {
    public static func buildExpression(_ handlers: [Requests.Handler]) -> [Requests.Handler] { handlers }
    public static func buildArray(_ components: [[Requests.Handler]]) -> [Requests.Handler] { components.flatMap { $0 } }
    public static func buildBlock(_ components: [Requests.Handler]...) -> [Requests.Handler] { components.flatMap { $0 } }
    public static func buildEither(first: [Requests.Handler]) -> [Requests.Handler] { first }
    public static func buildEither(second: [Requests.Handler]) -> [Requests.Handler] { second }
    public static func buildExpression(_ handler: Requests.Handler) -> [Requests.Handler] { [handler] }
    public static func buildOptional(_ component: [Requests.Handler]?) -> [Requests.Handler] { component ?? [] }
}
func makeHandler<R>(_ bindings: RequestBindings<R>, handler: @escaping @Sendable (R.Input, Requests.Stream<R.Inbound, R.Yield>) async throws -> R.Output) -> Requests.Handler {
    Requests.Handler(route: MessageRoute(event: R.event, version: R.version)) { input, flow in
        let value = try bindings.input.decode(input)
        flow.state.startReadingServer(absent: bindings.inbound.absent)
        // With no inbound lane the complete request is INPUT, END, EOF.
        // Reject extra/truncated frames before entering application code.
        if bindings.inbound.absent { _ = try await flow.state.result.wait() }
        let stream = Requests.Stream(incoming: Requests.Incoming(codec: bindings.inbound, flow: flow), codec: bindings.yield, flow: flow)
        let output = try await handler(value, stream)
        flow.state.revoke()
        // Join/close cancels any reader stalled on unconsumed inbound messages.
        try await flow.state.terminal(output, codec: bindings.output)
    }
}
