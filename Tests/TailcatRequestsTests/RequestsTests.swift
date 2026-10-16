import Foundation
import Testing
@testable import Tailcat
@testable import TailcatRequests

private enum Echo: Request {
    typealias Input = String
    typealias Output = String
    static let event = "echo"
}
private enum Missing: Request {
    typealias Output = Int
    static let event = "missing"
}
private enum Empty: Request { static let event = "empty" }
private enum Duplex: Request {
    typealias Input = Int
    typealias Inbound = Int
    typealias Yield = Int
    typealias Output = Int
    static let event = "sum"
}
private enum Reject: Error { case secret }

// Connect the real userspace TCP stacks directly; no mocked request transport.
private actor Link {
    var a: IPStack?
    var b: IPStack?
    func attach(_ a: IPStack, _ b: IPStack) { self.a = a; self.b = b }
    func send(_ packet: Data, fromA: Bool) async { await (fromA ? b : a)?.receive(packet) }
}

private struct Fixture: Sendable {
    let a: IPStack
    let b: IPStack
    let listener: TailcatListener
    let connection: Requests.Connection
    static func make(configuration: Requests.Connection.Configuration = .init()) async throws -> Self {
        let link = Link()
        let addressA = Data([0xfd] + [UInt8](repeating: 0, count: 14) + [1])
        let addressB = Data([0xfd] + [UInt8](repeating: 0, count: 14) + [2])
        let a = IPStack(address: addressA) { await link.send($0, fromA: true) }
        let b = IPStack(address: addressB) { await link.send($0, fromA: false) }
        await link.attach(a, b)
        let listener = try await b.listen(port: 7000)
        let connection = try Requests.Connection(configuration: configuration) {
            try await a.connect(peer: addressB, port: 7000)
        }
        return Self(a: a, b: b, listener: listener, connection: connection)
    }
    func close() async {
        try? await connection.close()
        await listener.close()
        await a.close()
        await b.close()
    }
}

struct RequestsTests {
    @Test(.unitTimeout)
    func testTypedCallsUseIndependentStreams() async throws {
        let f = try await Fixture.make()
        let server = try Requests.Listener(listener: f.listener) {
            Handle(Echo.self) { "reply:" + $0 }
            Handle(Empty.self) { _ in () }
        }
        do {
            do {
                async let first = f.connection.call(Echo.self, input: "one")
                async let second = f.connection.call(Echo.self, input: "two")
                let values = try await [first, second]
                #expect(values == ["reply:one", "reply:two"])
                try await f.connection.call(Empty.self)
            }
        } catch { try? await server.close(); await f.close(); throw error }
        try await server.close(); await f.close()
    }

    @Test(.unitTimeout)
    func testDuplexYieldsAndFinalResult() async throws {
        let f = try await Fixture.make()
        let server = try Requests.Listener(listener: f.listener) {
            Handle(Duplex.self) { initial, stream in
                var total = initial
                for try await number in stream.incoming {
                    total += number
                    try await stream.yield(total)
                }
                return total
            }
        }
        do {
            do {
                let stream = try await f.connection.open(Duplex.self, input: 10)
                try await stream.send(2)
                try await stream.send(3)
                try await stream.finishSending()
                var yielded: [Int] = []
                for try await value in stream.incoming { yielded.append(value) }
                #expect(yielded == [12, 15])
                let result = try await stream.result()
                #expect(result == 15)
            }
        } catch { try? await server.close(); await f.close(); throw error }
        try await server.close(); await f.close()
    }

    @Test(.unitTimeout)
    func testMissingRouteAndHandlerFailureAreRemoteErrors() async throws {
        let f = try await Fixture.make()
        let server = try Requests.Listener(listener: f.listener) {
            Handle(Echo.self) { _ in throw Reject.secret }
        }
        do {
            do {
                do { _ = try await f.connection.call(Missing.self); Issue.record("Missing route succeeded") }
                catch let error as Requests.Messaging.Failure {
                    #expect(error == .remote(code: "request_failed", message: "Request could not be completed"))
                }
                do { _ = try await f.connection.call(Echo.self, input: "private"); Issue.record("Rejected handler succeeded") }
                catch let error as Requests.Messaging.Failure {
                    #expect(error == .remote(code: "request_failed", message: "Request could not be completed"))
                }
            }
        } catch { try? await server.close(); await f.close(); throw error }
        try await server.close(); await f.close()
    }

    @Test(.unitTimeout)
    func testDuplicateHandlersAreRejectedBeforeServing() async throws {
        let f = try await Fixture.make()
        #expect(throws: Requests.Messaging.Failure.invalidRequest) {
            try Requests.Listener(listener: f.listener) {
                Handle(Echo.self) { $0 }
                Handle(Echo.self) { $0 }
            }
        }
        await f.close()
    }

    @Test(.unitTimeout)
    func testOversizedInputIsRejectedAndConnectionRemainsUsable() async throws {
        let f = try await Fixture.make(configuration: .init(messaging: .init(maxEncodedMessageBytes: 16)))
        let server = try Requests.Listener(listener: f.listener) { Handle(Echo.self) { $0 } }
        do {
            do { _ = try await f.connection.call(Echo.self, input: String(repeating: "x", count: 20)); Issue.record("Oversized input succeeded") }
            catch let error as Requests.Messaging.Failure { #expect(error == .oversizedMessage) }
            let output = try await f.connection.call(Echo.self, input: "small")
            #expect(output == "small")
        } catch { try? await server.close(); await f.close(); throw error }
        try await server.close(); await f.close()
    }

    @Test(.unitTimeout)
    func testCancellingAnOpenStreamReleasesItsCapacity() async throws {
        let f = try await Fixture.make(configuration: .init(maxConcurrentRequests: 1))
        let server = try Requests.Listener(listener: f.listener) {
            Handle(Duplex.self) { initial, stream in
                for try await _ in stream.incoming { }
                return initial
            }
            Handle(Echo.self) { $0 }
        }
        do {
            do {
                let stream = try await f.connection.open(Duplex.self, input: 0)
                let waiting = Task { try await stream.result() }
                waiting.cancel()
                do { _ = try await waiting.value; Issue.record("Cancelled result succeeded") } catch { }
                let output = try await f.connection.call(Echo.self, input: "after cancellation")
                #expect(output == "after cancellation")
            }
        } catch { try? await server.close(); await f.close(); throw error }
        try await server.close(); await f.close()
    }

    @Test(.unitTimeout)
    func testClosingConnectionStopsActiveRequestsAndRejectsNewOnes() async throws {
        let f = try await Fixture.make()
        let server = try Requests.Listener(listener: f.listener) {
            Handle(Duplex.self) { _, stream in
                for try await _ in stream.incoming { }
                return 0
            }
        }
        do {
            do {
                let stream = try await f.connection.open(Duplex.self, input: 0)
                try await f.connection.close()
                do { _ = try await stream.result(); Issue.record("Closed request succeeded") } catch { }
                do { _ = try await f.connection.open(Duplex.self, input: 0); Issue.record("Closed owner admitted a request") } catch { }
            }
        } catch { try? await server.close(); await f.close(); throw error }
        try await server.close(); await f.close()
    }

    @Test(.unitTimeout)
    func testFrameReaderRejectsMalformedOversizedAndTruncatedBytes() async throws {
        let vectors: [[UInt8]] = [
            [0,67,65,84,1,3,0,0,0,1],
            [84,67,65,84,1,99,0,0,0,0],
            [84,67,65,84,1,3,0,0,0,9],
            [84,67,65,84,1,3,0,0,0],
            [84,67,65,84,1,3,0,0,0,2,1],
            [84,67,65,84,1,5,0,0,0,1]
        ]
        for vector in vectors {
            let f = try await Fixture.make()
            do {
                do {
                    let client = try await f.a.connect(peer: f.listener.localAddress, port: f.listener.port)
                    let accepted = try await f.listener.accept()
                    let writer = Task {
                        try await client.write(Data(vector))
                        try await client.closeWrite()
                    }
                    do {
                        _ = try await MessageReader(raw: MessageTransport(accepted), limit: 8).read()
                        Issue.record("Malformed frame was accepted")
                    } catch let error as Requests.Messaging.Failure {
                        #expect(error == .malformedFrame || error == .oversizedMessage)
                    }
                    await client.abort(); await accepted.abort()
                    _ = await writer.result
                }
            } catch { await f.close(); throw error }
            await f.close()
        }
    }

    @Test(.unitTimeout)
    func testFourMiBEnvelopeFitsTheDefaultLimit() throws {
        let payload = Data(repeating: 255, count: 4 * 1024 * 1024)
        let encoded = try MessageCodec<Data>.codable.encode(payload)
        #expect(encoded.count <= Requests.Messaging.Configuration().maxEncodedMessageBytes)
        #expect(try MessageCodec<Data>.codable.decode(encoded) == payload)
    }


    @Test(.unitTimeout)
    func testServerCompletionUnblocksUnreadInboundQueue() async throws {
        let f = try await Fixture.make()
        let client = try await f.a.connect(peer: f.listener.localAddress, port: f.listener.port)
        let accepted = try await f.listener.accept()
        let flow = MessageFlow(configuration: .init(maxBufferedMessages: 1),
                               raw: MessageTransport(accepted), route: .init(event: "early", version: 1))
        try await flow.state.inbox.put(Data([1]))
        flow.state.tasks.start { try? await flow.state.inbox.put(Data([2])) }
        try await flow.state.terminal((), codec: .empty)
        await withTaskCancellationHandler {
            await flow.close()
        } onCancel: {
            flow.state.inbox.finish(MessageFailure.closed)
        }
        await client.abort()
        await f.close()
    }

}
