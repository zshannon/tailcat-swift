import Clocks
import Foundation
import Testing
@testable import Tailcat
@testable import TailcatRequests

private enum DeadlineEcho: Request {
    typealias Input = Data
    typealias Output = Data
    static let event = "deadline-echo"
}

private actor DeadlineLink {
    var client: IPStack?
    var server: IPStack?

    func attach(client: IPStack, server: IPStack) {
        self.client = client
        self.server = server
    }

    func send(_ packet: Data, fromClient: Bool) async {
        await (fromClient ? server : client)?.receive(packet)
    }
}

struct RequestDeadlineTests {
    @Test("Stalled framing releases listener capacity", .unitTimeout, arguments: [false, true])
    func stalledFraming(partialRequest: Bool) async throws {
        guard #available(iOS 16, macOS 13, *) else {
            Issue.record("TestClock requires iOS 16 or macOS 13")
            return
        }
        let clock = TestClock<Duration>()
        let link = DeadlineLink()
        let clientAddress = Data([0xfd] + [UInt8](repeating: 0, count: 14) + [1])
        let serverAddress = Data([0xfd] + [UInt8](repeating: 0, count: 14) + [2])
        let client = IPStack(address: clientAddress) { await link.send($0, fromClient: true) }
        let server = IPStack(address: serverAddress) { await link.send($0, fromClient: false) }
        await link.attach(client: client, server: server)
        let rawListener = try await server.listen(port: 7000)
        let listener = try Requests.Listener(
            listener: rawListener,
            configuration: .init(clock: clock, maxConnections: 1, requestTimeout: 0.75)
        ) { Handle(DeadlineEcho.self) { $0 } }
        let requests = try Requests.Connection { try await client.connect(peer: serverAddress, port: 7000) }
        do {
            if partialRequest {
                let unfinished = try await requests.open(DeadlineEcho.self, input: Data([1]))
                try await advanceToDeadline(clock: clock)
                await #expect(throws: (any Error).self) { try await unfinished.result() }
                await unfinished.resetAndWait()
            } else {
                let stalled = try await client.connect(peer: serverAddress, port: 7000)
                try await advanceToDeadline(clock: clock)
                await #expect(throws: (any Error).self) { try await stalled.read() }
                await stalled.abort()
            }
            #expect(try await requests.call(DeadlineEcho.self, input: Data([2])) == Data([2]))
        } catch {
            try? await requests.close()
            try? await listener.close()
            await client.close()
            await server.close()
            throw error
        }
        try await requests.close()
        try await listener.close()
        await client.close()
        await server.close()
        try await clock.checkSuspension()
    }

    @Test(.unitTimeout, arguments: [0.0, -1.0, .infinity, .nan, Double(Int64.max)])
    func invalidDeadline(timeout: TimeInterval) {
        #expect(throws: Requests.Messaging.Failure.invalidConfiguration) {
            try Requests.Listener.Configuration(requestTimeout: timeout).validate()
        }
    }
}

@available(iOS 16, macOS 13, *)
private func advanceToDeadline(clock: TestClock<Duration>) async throws {
    while true {
        try Task.checkCancellation()
        do { try await clock.checkSuspension() }
        catch { break }
    }
    await clock.advance(by: .milliseconds(750))
}
