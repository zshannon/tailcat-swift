import XCTest
@testable import Tailcat

#if canImport(Glibc)
/// The client's first initiation goes with its meow, before the server has admitted it.
final class EarlyInitiationTests: XCTestCase {
    private func loopbackDiscovery() -> DirectDiscovery {
        var discovery = DirectDiscovery()
        discovery.interfaces = { [DiscoveryEndpoint(host: "127.0.0.1", port: $0.port)] }
        return discovery
    }

    /// Connects one client and echoes over it; returns the time from its meow to an established session.
    private func connectAndEcho(_ fixture: LocalRelay, allowClient: (@Sendable (NodePublicKey) async -> Bool)? = nil,
                                afterConnect: TimeInterval = 0) async throws -> TimeInterval {
        var serverConfig = TailcatServer.Configuration()
        serverConfig.relayFirst = true
        serverConfig.region = DERPRegion(regionID: 1, nodes: [fixture.node])
        serverConfig.directDiscovery = loopbackDiscovery()
        serverConfig.allowClient = allowClient
        let server = TailcatServer(configuration: serverConfig)
        var client: TailcatClient?
        do {
            let listener = try await server.listen(port: 8100)
            let address = try await server.address
            var config = TailcatClient.Configuration()
            config.relayFirst = true; config.directDiscovery = loopbackDiscovery(); config.connectTimeout = 10
            let trace = StartupTrace(); config.startupTrace = trace
            let connected = try await TailcatClient.connect(to: address, configuration: config)
            client = connected
            let events = trace.snapshot()
            let meow = try XCTUnwrap(events.first { $0.name == "meowWritten" }?.elapsedMs)
            let established = try XCTUnwrap(events.first { $0.name == "wireGuardEstablished" }?.elapsedMs)
            let elapsed = (established - meow) / 1000
            let names = events.map(\.name)
            XCTAssertLessThan(try XCTUnwrap(names.firstIndex(of: "wireGuardInitiationSendStarted")),
                              try XCTUnwrap(names.firstIndex(of: "meowAcknowledged") ?? names.endIndex), "Initiated before meowed")
            try await Task.sleep(nanoseconds: UInt64(afterConnect * 1_000_000_000))
            let stream = try await connected.openTCP(port: 8100)
            let accepted = try await listener.accept()
            try await stream.write(Data("early".utf8))
            let received = try await accepted.read()
            XCTAssertEqual(received, Data("early".utf8))
            await connected.close(); await server.close()
            return elapsed
        } catch { await client?.close(); await server.close(); throw error }
    }

    func testEarlyInitiationCostsNoExtraRoundTrip() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        let elapsed = try await connectAndEcho(fixture)
        print("EARLY-INITIATION-MEOW-TO-WG \(Int(elapsed * 1000))ms")
        XCTAssertLessThan(elapsed, 1)
        XCTAssertLessThanOrEqual(fixture.relayedInitiations, 6, "The first initiation and at most five copies")
    }

    func testCopiesDroppedBeforeASlowAdmissionAreAnsweredAtMeowed() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        // Every copy reaches the server before it admits the client, and is dropped.
        let elapsed = try await connectAndEcho(fixture, allowClient: { _ in
            try? await Task.sleep(nanoseconds: 400_000_000); return true
        })
        print("SLOW-ADMISSION-MEOW-TO-WG \(Int(elapsed * 1000))ms")
        XCTAssertGreaterThan(elapsed, 0.4); XCTAssertLessThan(elapsed, 0.7, "Not held for the five-second retry")
        XCTAssertEqual(fixture.relayedInitiations, 7, "Six early copies and one at meowed; no fresh initiation")
    }

    func testALostResponseToAnEarlyCopyGetsAFreshInitiation() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        // The server takes a copy but its response is lost: later copies are replays.
        fixture.drop(1) { WireGuard.packetType($0) == 2 }
        let elapsed = try await connectAndEcho(fixture)
        print("LOST-EARLY-RESPONSE-MEOW-TO-WG \(Int(elapsed * 1000))ms")
        XCTAssertGreaterThan(elapsed, 0.3); XCTAssertLessThan(elapsed, 0.8, "Not held for the five-second retry")
        XCTAssertLessThanOrEqual(fixture.relayedInitiations, 8, "Early copies, one at meowed and one fresh")
    }

    func testEstablishedBeforeMeowedConnectsAndKeepsTheSession() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        // The first meowed is lost: the next meow goes a second later.
        fixture.drop(1) { $0.starts(with: Data("meow\u{02}".utf8)) }
        let elapsed = try await connectAndEcho(fixture, afterConnect: 1.5)
        print("ESTABLISHED-BEFORE-MEOWED-MEOW-TO-WG \(Int(elapsed * 1000))ms")
        XCTAssertLessThan(elapsed, 0.9, "Connected without waiting for the next meow")
        XCTAssertLessThanOrEqual(fixture.relayedInitiations, 6, "A late meowed starts no new handshake")
    }
}
#endif
