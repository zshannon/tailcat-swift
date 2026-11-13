import XCTest
@testable import Tailcat

#if canImport(Glibc)
private actor DiscoveryGate {
    private var blocked = true
    private var waiters = [CheckedContinuation<Void, Never>]()
    private(set) var calls = 0
    func wait() async {
        calls += 1
        if blocked { await withCheckedContinuation { waiters.append($0) } }
    }
    func release() {
        blocked = false
        let pending = waiters; waiters.removeAll()
        for waiter in pending { waiter.resume() }
    }
    func block() { blocked = true }
}

private actor SocketCapture {
    private(set) var socket: UDPChannel?
    private(set) var sockets = [UDPChannel]()
    func set(_ value: UDPChannel) { socket = value; sockets.append(value) }
}

private final class DiscoveryPacketFilter: @unchecked Sendable {
    private let lock = NSLock()
    private var blocked = false
    func setBlocked(_ value: Bool) { lock.lock(); blocked = value; lock.unlock() }
    func drops(_ packet: Data) -> Bool {
        lock.lock(); defer { lock.unlock() }
        return blocked && Discovery.isPacket(packet)
    }
}

/// The first receiver index it is asked about, and only that one, matches.
private final class FirstReceiver: @unchecked Sendable {
    private let lock = NSLock()
    private var index: Data?
    func matches(_ receiver: Data) -> Bool {
        lock.lock(); defer { lock.unlock() }
        if index == nil { index = receiver }
        return index == receiver
    }
}

final class RelayFirstTests: XCTestCase {
    private func within<T: Sendable>(_ seconds: Double = 10, _ operation: @escaping @Sendable () async throws -> T) async throws -> T {
        try await withThrowingTaskGroup(of: T.self) { group in
            group.addTask { try await operation() }
            group.addTask { try await Task.sleep(nanoseconds: UInt64(seconds * 1_000_000_000)); throw TailcatError.timedOut }
            defer { group.cancelAll() }
            return try await group.next()!
        }
    }

    private func eventually(_ predicate: @escaping @Sendable () async -> Bool) async throws {
        try await within {
            while !(await predicate()) { try await Task.sleep(nanoseconds: 20_000_000) }
        }
    }

    private func gatedDiscovery(_ gate: DiscoveryGate, base: DirectDiscovery? = nil) -> DirectDiscovery {
        var discovery = base ?? loopbackDiscovery()
        discovery.open = { receive in
            await gate.wait()
            return try UDPChannel(receive: receive)
        }
        return discovery
    }

    private func loopbackDiscovery() -> DirectDiscovery {
        var discovery = DirectDiscovery()
        // Keep fixtures independent of the host's VPN/container interface count.
        discovery.interfaces = { [DiscoveryEndpoint(host: "127.0.0.1", port: $0.port)] }
        return discovery
    }

    private func exchange(_ client: TailcatConnection, _ server: TailcatConnection, _ text: String) async throws {
        try await within {
            let data = Data(text.utf8)
            try await client.write(data)
            let received = try await server.read()
            XCTAssertEqual(received, data)
            try await server.write(received)
            let response = try await client.read()
            XCTAssertEqual(response, data)
        }
    }

    func testRelayFirstClientWithDefaultServerPreservesEncryptedStream() async throws {
        try await exerciseStream(relayFirstServer: false)
    }

    func testRelayFirstServerWithDefaultClientPreservesEncryptedStream() async throws {
        try await exerciseStream(relayFirstServer: true)
    }

    func testMultipleCandidatesKeepStreamUsableDuringDiscovery() async throws {
        var discovery = loopbackDiscovery()
        discovery.interfaces = { socket in
            ["127.0.0.1", "::1", "127.0.0.2", "127.0.0.3"].map { DiscoveryEndpoint(host: $0, port: socket.port) }
        }
        try await exerciseStream(relayFirstServer: false, discovery: discovery)
    }

    func testHostInterfacesStreamSmoke() async throws {
        guard ProcessInfo.processInfo.environment["TAILCAT_TEST_HOST_INTERFACES"] == "1" else {
            throw XCTSkip("Set TAILCAT_TEST_HOST_INTERFACES=1 for the host VPN/container interface smoke test")
        }
        try await exerciseStream(relayFirstServer: false, discovery: DirectDiscovery())
    }

    private func exerciseStream(relayFirstServer: Bool, discovery supplied: DirectDiscovery? = nil) async throws {
        let fixture = try LocalRelay()
        defer { fixture.close() }
        let gate = DiscoveryGate()
        let discovery = supplied ?? loopbackDiscovery()
        var serverConfiguration = TailcatServer.Configuration()
        serverConfiguration.directDiscovery = discovery
        serverConfiguration.region = DERPRegion(regionID: 1, nodes: [fixture.node])
        serverConfiguration.relayFirst = relayFirstServer
        if relayFirstServer { serverConfiguration.directDiscovery = gatedDiscovery(gate, base: discovery) }
        let server = TailcatServer(configuration: serverConfiguration)
        var client: TailcatClient?
        var stage = "startup"
        do {
            let listener = try await within { try await server.listen(port: 8080) }
            let address = try await server.address
            var clientConfiguration = TailcatClient.Configuration()
            clientConfiguration.directDiscovery = discovery
            clientConfiguration.relayFirst = !relayFirstServer
            if !relayFirstServer { clientConfiguration.directDiscovery = gatedDiscovery(gate, base: discovery) }
            let config = clientConfiguration
            let connected = try await within { try await TailcatClient.connect(to: address, configuration: config) }
            client = connected
            let initialPath = await connected.path
            XCTAssertEqual(initialPath, .relay)
            let stream = try await within { try await connected.openTCP(port: 8080) }
            let accepted = try await within { try await listener.accept() }
            try await exchange(stream, accepted, "encrypted before UDP discovery returns")
            let calls = await gate.calls
            XCTAssertEqual(calls, 1)

            await gate.release()
            stage = "first direct upgrade"
            try await within(15) {
                var consecutiveDirect = 0
                while consecutiveDirect < 20 {
                    try await self.exchange(stream, accepted, "continuous traffic during initial promotion")
                    if case .direct = await connected.path,
                       case .direct = try? await server.path(to: connected.publicKey) {
                        consecutiveDirect += 1
                    } else { consecutiveDirect = 0 }
                    try await Task.sleep(nanoseconds: 50_000_000)
                }
            }
            let oldPath = await connected.path
            stage = "direct traffic and voluntary reprobe"
            try await exchange(stream, accepted, "same TCP stream after direct upgrade")
            await connected.retryDirect()
            await server.retryDirect(to: connected.publicKey)
            let afterRetry = await connected.path
            XCTAssertEqual(afterRetry, oldPath, "Voluntary reprobe must not discard the live path")

            fixture.setAvailable(false)
            stage = "relay outage while direct"
            try await exchange(stream, accepted, "direct works while DERP is unavailable")
            fixture.setAvailable(true)

            await gate.block()
            stage = "network change"
            await connected.resetNetworkPaths()
            await server.resetNetworkPaths()
            let resetPath = await connected.path
            XCTAssertEqual(resetPath, .relay)
            try await exchange(stream, accepted, "same encrypted stream after socket invalidation")
            fixture.dropConnections()
            stage = "relay reconnect"
            try await exchange(stream, accepted, "same encrypted stream after DERP reconnect")
            await gate.release()
            stage = "new network direct upgrade"
            try await eventually {
                if case .direct = await connected.path { return true }; return false
            }
            try await exchange(stream, accepted, "same TCP stream after new network upgrade")
            stage = "one-sided socket rebind"
            await connected.resetNetworkPaths()
            try await exchange(stream, accepted, "same TCP stream after only the client changes networks")
            await connected.close(); await server.close()
        } catch {
            XCTFail("Failed during \(stage): \(error)")
            await gate.release(); await client?.close(); await server.close(); throw error
        }
    }

    func testDefaultStillWaitsForUDPButRelayFirstDoesNot() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        let gate = DiscoveryGate()
        let transport = NetworkTransport(privateKey: NaClBox.random(32), discoPrivateKey: NaClBox.random(32),
                                         relayNodes: [fixture.node], relayFirst: false, discovery: gatedDiscovery(gate))
        let started = expectation(description: "default start waits for UDP")
        started.isInverted = true
        let pending = Task { try await transport.start(); started.fulfill() }
        try await eventually { await gate.calls == 1 }
        await fulfillment(of: [started], timeout: 0.1)
        // Close never waits for an uninterruptible OS discovery operation.
        await transport.close()
        await gate.release()
        do { try await pending.value; XCTFail("Closed startup succeeded") } catch {}
    }

    func testNetworkChangesCoalesceAndRejectLateSocketAfterClose() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        let gate = DiscoveryGate()
        let transport = NetworkTransport(privateKey: NaClBox.random(32), discoPrivateKey: NaClBox.random(32),
                                         relayNodes: [fixture.node], relayFirst: true, discovery: gatedDiscovery(gate))
        try await within { try await transport.start() }
        try await eventually { await gate.calls == 1 }
        for _ in 0..<100 {
            await transport.resetNetwork()
            await transport.retryDirect()
        }
        let calls = await gate.calls
        XCTAssertEqual(calls, 1, "Only one blocked OS discovery worker may exist")
        await transport.close()
        await gate.release()
        try await Task.sleep(nanoseconds: 100_000_000)
        let endpoints = await transport.localEndpoints()
        XCTAssertTrue(endpoints.isEmpty)
        do { try await transport.start(); XCTFail("Closed transport restarted") } catch {}
    }

    func testUDPFailureDoesNotFailRelayFirstAndCanBeRetried() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        let attempts = DiscoveryGate()
        await attempts.release()
        var discovery = loopbackDiscovery()
        discovery.open = { receive in
            await attempts.wait()
            if await attempts.calls == 1 { throw TransportError.connectionFailed("simulated UDP unavailable") }
            return try UDPChannel(receive: receive)
        }
        let transport = NetworkTransport(privateKey: NaClBox.random(32), discoPrivateKey: NaClBox.random(32),
                                         relayNodes: [fixture.node], relayFirst: true, discovery: discovery)
        try await within { try await transport.start() }
        try await eventually { await attempts.calls == 1 }
        await transport.retryDirect()
        try await eventually { !(await transport.localEndpoints()).isEmpty }
        await transport.close()
    }

    func testDirectSendErrorFallsBackWithoutClosingRelay() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        let capture = SocketCapture()
        var discovery = loopbackDiscovery()
        discovery.open = { receive in
            let socket = try UDPChannel(receive: receive)
            await capture.set(socket)
            return socket
        }
        let aKey = NaClBox.random(32), bKey = NaClBox.random(32)
        let aDisco = NaClBox.random(32), bDisco = NaClBox.random(32)
        let aPublic = try NaClBox.publicKey(for: aKey), bPublic = try NaClBox.publicKey(for: bKey)
        let a = NetworkTransport(privateKey: aKey, discoPrivateKey: aDisco, relayNodes: [fixture.node], relayFirst: true, discovery: discovery)
        let b = NetworkTransport(privateKey: bKey, discoPrivateKey: bDisco, relayNodes: [fixture.node], relayFirst: false, discovery: loopbackDiscovery())
        do {
            try await a.start(); try await b.start()
            try await a.configurePeer(nodeKey: bPublic, discoKey: NaClBox.publicKey(for: bDisco), endpoints: b.localEndpoints())
            try await b.configurePeer(nodeKey: aPublic, discoKey: NaClBox.publicKey(for: aDisco), endpoints: a.localEndpoints())
            try await eventually { if case .direct = await a.path(to: bPublic) { return true }; return false }
            let receiver = Data(repeating: 7, count: 4)
            await a.observeAuthenticatedTransport(from: bPublic, viaRelay: false, receiver: receiver, counter: 2, allowFallback: true)
            await a.observeAuthenticatedTransport(from: bPublic, viaRelay: true, receiver: receiver, counter: 1, allowFallback: false)
            await a.observeAuthenticatedTransport(from: bPublic, viaRelay: true, receiver: receiver, counter: 3, allowFallback: false)
            let afterOldAndEmptyFrames = await a.path(to: bPublic)
            guard case .direct = afterOldAndEmptyFrames else { XCTFail("Older or ordinary relay frames changed the route"); throw TransportError.closed }
            await capture.socket?.close()
            let payload = Data("fallback on failed local UDP socket".utf8)
            let received = Task {
                for try await packet in b.packets where packet.payload == payload { return packet }
                throw TransportError.closed
            }
            try await a.send(to: bPublic, packet: payload)
            let packet = try await within { try await received.value }
            XCTAssertTrue(packet.viaRelay)
            let path = await a.path(to: bPublic)
            XCTAssertEqual(path, .relay)
            received.cancel()
            await a.close(); await b.close()
        } catch { await a.close(); await b.close(); throw error }
    }

    func testARelayBurstWaitsForTheEngineInsteadOfBeingDropped() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        let aKey = NaClBox.random(32), bKey = NaClBox.random(32)
        let aDisco = NaClBox.random(32), bDisco = NaClBox.random(32)
        let aPublic = try NaClBox.publicKey(for: aKey), bPublic = try NaClBox.publicKey(for: bKey)
        // Relay only: no UDP socket opens.
        let a = NetworkTransport(privateKey: aKey, discoPrivateKey: aDisco, relayNodes: [fixture.node], relayFirst: true, discovery: gatedDiscovery(DiscoveryGate()))
        let b = NetworkTransport(privateKey: bKey, discoPrivateKey: bDisco, relayNodes: [fixture.node], relayFirst: true, discovery: gatedDiscovery(DiscoveryGate()))
        do {
            try await a.start(); try await b.start()
            try await a.configurePeer(nodeKey: bPublic, discoKey: NaClBox.publicKey(for: bDisco))
            try await b.configurePeer(nodeKey: aPublic, discoKey: NaClBox.publicKey(for: aDisco))
            // A window of 800 full segments arrives while the engine is busy elsewhere.
            let count = 800
            for index in 0..<count {
                try await a.send(to: bPublic, packet: Data([4, 0, 0, 0, UInt8(index >> 8), UInt8(truncatingIfNeeded: index)]) + Data(repeating: 0, count: 1194))
            }
            try await Task.sleep(nanoseconds: 300_000_000)
            let received = try await within(5) { () -> [Int] in
                var indexes = [Int]()
                for try await packet in b.packets {
                    indexes.append(Int(packet.payload[4]) << 8 | Int(packet.payload[5]))
                    if indexes.count == count { break }
                }
                return indexes
            }
            XCTAssertEqual(received, Array(0..<count))
            let drops = await b.receiveDrops
            XCTAssertEqual(drops, 0)
            await a.close(); await b.close()
        } catch { await a.close(); await b.close(); throw error }
    }

    func testAClientRecoversFromAServerThatRestartedWithoutItsSession() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        var serverConfig = TailcatServer.Configuration()
        serverConfig.relayFirst = true
        serverConfig.region = DERPRegion(regionID: 1, nodes: [fixture.node])
        serverConfig.directDiscovery = gatedDiscovery(DiscoveryGate())
        serverConfig.privateKey = try NodePrivateKey.generate()
        serverConfig.presharedKey = try PresharedKey.generate()
        let first = TailcatServer(configuration: serverConfig), second = TailcatServer(configuration: serverConfig)
        let firstListener = try await first.listen(port: 8098)
        var config = TailcatClient.Configuration()
        config.relayFirst = true; config.directDiscovery = gatedDiscovery(DiscoveryGate())
        let client = try await TailcatClient.connect(to: first.address, configuration: config)
        do {
            let stream = try await client.openTCP(port: 8098)
            try await exchange(stream, try await firstListener.accept(), "before the restart")
            // The computer restarts with the same key while the phone is away:
            // it holds no session for the phone, and hasn't admitted it.
            await first.close()
            let listener = try await second.listen(port: 8098)
            let opening = Task { try await client.openTCP(port: 8098) }
            try await Task.sleep(nanoseconds: 300_000_000)
            // Fifteen seconds without an answer to what the client sent.
            await client.ageUnansweredForTesting(by: 15)
            let started = Date()
            let accepted = try await within(5) { try await listener.accept() }
            let fresh = try await within(5) { try await opening.value }
            try await exchange(fresh, accepted, "after the restart")
            print("SERVER-RESTART-RECOVERED \(Int(Date().timeIntervalSince(started) * 1000))ms")
            // The connection from before the restart is reset, rather than left hanging.
            do {
                _ = try await within(5) { try await stream.write(Data("stale".utf8)); return try await stream.read() }
                XCTFail("the connection from before the restart still answered")
            } catch TailcatError.timedOut { XCTFail("the connection from before the restart hung") } catch {}
            await client.close(); await second.close()
        } catch { await client.close(); await second.close(); throw error }
    }

    func testRejoiningAServerThatKeptItsSessionKeepsTheConnection() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        let gate = DiscoveryGate()
        let pair = try await wakePair(fixture, port: 8099, client: gatedDiscovery(gate))
        do {
            try await exchange(pair.stream, pair.accepted, "before")
            let initiations = fixture.relayedInitiations
            await pair.client.ageUnansweredForTesting(by: 15)
            try await Task.sleep(nanoseconds: 2_500_000_000)
            try await exchange(pair.stream, pair.accepted, "after rejoining")
            // The initiation that went with the meow, and its replay once meowed, which the server drops.
            XCTAssertEqual(fixture.relayedInitiations, initiations + 2)
            await gate.release(); await pair.client.close(); await pair.server.close()
        } catch { await gate.release(); await pair.client.close(); await pair.server.close(); throw error }
    }

    func testATickThatFollowsASuspensionDoesNotRejoin() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        let gate = DiscoveryGate()
        let pair = try await wakePair(fixture, port: 8102, client: gatedDiscovery(gate))
        do {
            try await exchange(pair.stream, pair.accepted, "before")
            let client = await pair.client.publicKey.rawValue
            fixture.drop(.max) { source, packet in source == client && packet.first == 4 }
            let writer = Task { try await pair.stream.write(Data("sent just before suspending".utf8)) }
            defer { writer.cancel() }
            try await Task.sleep(nanoseconds: 300_000_000)
            let initiations = fixture.relayedInitiations
            // The timer's first tick after twenty suspended seconds, before the app's wake:
            // nothing could have answered meanwhile, so nothing is unanswered yet.
            await pair.client.resumeForTesting(after: 20)
            try await Task.sleep(nanoseconds: 1_500_000_000)
            XCTAssertEqual(fixture.relayedInitiations, initiations)
            fixture.stopDropping()
            await gate.release(); await pair.client.close(); await pair.server.close()
        } catch { fixture.stopDropping(); await gate.release(); await pair.client.close(); await pair.server.close(); throw error }
    }

    func testDataOwedForFiveSecondsRejoinsWhileTheServerStillSends() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        let gate = DiscoveryGate()
        let pair = try await wakePair(fixture, port: 8100, client: gatedDiscovery(gate))
        do {
            try await exchange(pair.stream, pair.accepted, "before")
            // The server can no longer read the session the client sends on, as after a
            // handshake that crossed one of its own, yet what it sends still arrives.
            let client = await pair.client.publicKey.rawValue, lost = FirstReceiver()
            fixture.drop(.max) { source, packet in
                source == client && packet.first == 4 && packet.count >= 8 && lost.matches(Data(packet[4..<8]))
            }
            let writer = Task {
                for _ in 0..<40 { try await pair.accepted.write(Data("x".utf8)); try await Task.sleep(nanoseconds: 500_000_000) }
            }
            defer { writer.cancel() }
            let started = Date()
            // Unanswered for five seconds: a new handshake, rather than fifteen seconds
            // with nothing back, which the server's own packets keep from coming.
            let fresh = try await within(10) { try await pair.client.openTCP(port: 8100) }
            print("OWED-REJOIN \(Int(Date().timeIntervalSince(started) * 1000))ms")
            XCTAssertLessThan(Date().timeIntervalSince(started), 8)
            await fresh.close()
            await gate.release(); await pair.client.close(); await pair.server.close()
        } catch { await gate.release(); await pair.client.close(); await pair.server.close(); throw error }
    }

    func testAServerBackOnNewPortsIsRejoinedAtOnce() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        var serverConfig = TailcatServer.Configuration()
        serverConfig.relayFirst = true
        serverConfig.region = DERPRegion(regionID: 1, nodes: [fixture.node])
        serverConfig.directDiscovery = loopbackDiscovery()
        serverConfig.privateKey = try NodePrivateKey.generate()
        serverConfig.presharedKey = try PresharedKey.generate()
        let first = TailcatServer(configuration: serverConfig), second = TailcatServer(configuration: serverConfig)
        let firstListener = try await first.listen(port: 8104)
        var config = TailcatClient.Configuration()
        config.relayFirst = true; config.directDiscovery = loopbackDiscovery()
        let client = try await TailcatClient.connect(to: first.address, configuration: config)
        do {
            let stream = try await client.openTCP(port: 8104)
            try await exchange(stream, try await firstListener.accept(), "before the restart")
            try await eventually { if case .direct = await client.path { return true }; return false }
            // The computer restarts with the same key, on a new UDP port, while the phone is away.
            await first.close()
            let listener = try await second.listen(port: 8104)
            let opening = Task { try await client.openTCP(port: 8104) }
            try await Task.sleep(nanoseconds: 300_000_000)
            await client.resumeForTesting(after: 20)
            let started = Date()
            await client.wake()
            // As the Go server does for the clients it knows, the new process advertises its
            // new ports at once: the client learns of the restart before anything goes unanswered.
            try await second.inviteForTesting(await client.publicKey, discoKey: try await client.discoveryPublicKey)
            let accepted = try await within(5) { try await listener.accept() }
            let fresh = try await within(5) { try await opening.value }
            try await exchange(fresh, accepted, "after the restart")
            let elapsed = Date().timeIntervalSince(started)
            print("RESTART-REJOIN \(Int(elapsed * 1000))ms")
            XCTAssertLessThan(elapsed, 1)
            await client.close(); await second.close()
        } catch { await client.close(); await second.close(); throw error }
    }

    func testEveryClientOfARestartedServerIsToldToMeowAgain() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        var serverConfig = TailcatServer.Configuration()
        serverConfig.relayFirst = true
        serverConfig.region = DERPRegion(regionID: 1, nodes: [fixture.node])
        serverConfig.directDiscovery = gatedDiscovery(DiscoveryGate())
        serverConfig.privateKey = try NodePrivateKey.generate()
        serverConfig.presharedKey = try PresharedKey.generate()
        let first = TailcatServer(configuration: serverConfig), second = TailcatServer(configuration: serverConfig)
        let firstListener = try await first.listen(port: 8105)
        // Two clients of one server, as the app's for requests and for files: each has to
        // find out on its own, and neither gets an invitation.
        var clients = [TailcatClient]()
        do {
            for _ in 0..<2 {
                var config = TailcatClient.Configuration()
                config.relayFirst = true; config.directDiscovery = gatedDiscovery(DiscoveryGate())
                let client = try await TailcatClient.connect(to: first.address, configuration: config)
                clients.append(client)
                try await exchange(try await client.openTCP(port: 8105), try await firstListener.accept(), "before the restart")
            }
            await first.close()
            let listener = try await second.listen(port: 8105)
            let openings = clients.map { client in Task { try await client.openTCP(port: 8105) } }
            try await Task.sleep(nanoseconds: 300_000_000)
            for client in clients { await client.resumeForTesting(after: 20) }
            let started = Date()
            for client in clients { await client.wake() }
            for opening in openings {
                let accepted = try await within(5) { try await listener.accept() }
                _ = accepted
                _ = try await within(5) { try await opening.value }
            }
            let elapsed = Date().timeIntervalSince(started)
            print("MEOW-AGAIN-REJOIN \(Int(elapsed * 1000))ms")
            XCTAssertLessThan(elapsed, 1)
            for client in clients { await client.close() }; await second.close()
        } catch { for client in clients { await client.close() }; await second.close(); throw error }
    }

    func testAServerRestartedDuringAShortAbsenceIsRejoinedAtOnce() async throws {
        for away: TimeInterval in [5, 20] {
            let fixture = try LocalRelay(); defer { fixture.close() }
            var serverConfig = TailcatServer.Configuration()
            serverConfig.relayFirst = true
            serverConfig.region = DERPRegion(regionID: 1, nodes: [fixture.node])
            serverConfig.directDiscovery = loopbackDiscovery()
            serverConfig.privateKey = try NodePrivateKey.generate()
            serverConfig.presharedKey = try PresharedKey.generate()
            let first = TailcatServer(configuration: serverConfig), second = TailcatServer(configuration: serverConfig)
            let firstListener = try await first.listen(port: 8106)
            var config = TailcatClient.Configuration()
            config.relayFirst = true; config.directDiscovery = loopbackDiscovery()
            let client = try await TailcatClient.connect(to: first.address, configuration: config)
            do {
                try await exchange(try await client.openTCP(port: 8106), try await firstListener.accept(), "before the restart")
                try await eventually { if case .direct = await client.path { return true }; return false }
                // Away for less than a direct route lives: the client still sends direct, to
                // the old process's port, and the new one sends no invitation.
                await first.close()
                let listener = try await second.listen(port: 8106)
                let opening = Task { try await client.openTCP(port: 8106) }
                try await Task.sleep(nanoseconds: 300_000_000)
                await client.ageDirectForTesting(by: away)
                await client.resumeForTesting(after: away)
                let started = Date()
                await client.wake()
                _ = try await within(5) { try await listener.accept() }
                _ = try await within(5) { try await opening.value }
                let elapsed = Date().timeIntervalSince(started)
                print("SHORT-ABSENCE-REJOIN away=\(Int(away)) \(Int(elapsed * 1000))ms")
                XCTAssertLessThan(elapsed, 1, "away \(away) s")
                await client.close(); await second.close()
            } catch { await client.close(); await second.close(); throw error }
        }
    }

    func testBlockedSTUNDoesNotDelayEncryptedRelaySession() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        let gate = DiscoveryGate()
        var node = fixture.node
        node.stunPort = 3478
        var configuration = TailcatServer.Configuration()
        configuration.directDiscovery = loopbackDiscovery()
        configuration.region = DERPRegion(regionID: 1, nodes: [node])
        configuration.relayFirst = true
        configuration.directDiscovery.resolve = { _, _ in await gate.wait(); return [] }
        let server = TailcatServer(configuration: configuration)
        var client: TailcatClient?
        do {
            let listener = try await within { try await server.listen(port: 8081) }
            try await eventually { await gate.calls > 0 }
            let address = try await server.address
            var clientConfiguration = TailcatClient.Configuration()
            clientConfiguration.directDiscovery = loopbackDiscovery()
            let config = clientConfiguration
            let connected = try await within { try await TailcatClient.connect(to: address, configuration: config) }
            client = connected
            let stream = try await within { try await connected.openTCP(port: 8081) }
            let accepted = try await within { try await listener.accept() }
            try await exchange(stream, accepted, "WireGuard and TCP do not wait for STUN DNS")
            await server.close(); await connected.close(); await gate.release()
        } catch { await server.close(); await client?.close(); await gate.release(); throw error }
    }

    func testFastUDPStillBootstrapsWireGuardOverRelay() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        var configuration = TailcatServer.Configuration()
        configuration.directDiscovery = loopbackDiscovery()
        configuration.region = DERPRegion(regionID: 1, nodes: [fixture.node])
        configuration.relayFirst = true
        let server = TailcatServer(configuration: configuration)
        var client: TailcatClient?
        do {
            try await server.start()
            let address = try await server.address
            var clientConfiguration = TailcatClient.Configuration()
            clientConfiguration.directDiscovery = loopbackDiscovery()
            clientConfiguration.relayFirst = true
            let connected = try await TailcatClient.connect(to: address, configuration: clientConfiguration)
            client = connected
            try await eventually { if case .direct = await connected.path { return true }; return false }
            XCTAssertTrue(fixture.relayedInitialHandshakeAndConfirmation)
            await connected.close(); await server.close()
        } catch { await client?.close(); await server.close(); throw error }
    }

    func testStaleSocketCompletionCannotInstallIntoNewNetworkGeneration() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        let gate = DiscoveryGate(), capture = SocketCapture()
        var discovery = loopbackDiscovery()
        discovery.open = { receive in
            await gate.wait()
            let socket = try UDPChannel(receive: receive)
            await capture.set(socket)
            return socket
        }
        let transport = NetworkTransport(privateKey: NaClBox.random(32), discoPrivateKey: NaClBox.random(32),
                                         relayNodes: [fixture.node], relayFirst: true, discovery: discovery)
        try await transport.start()
        try await eventually { await gate.calls == 1 }
        await transport.resetNetwork()
        await transport.resetNetwork()
        await gate.release()
        try await eventually {
            let calls = await gate.calls, endpoints = await transport.localEndpoints()
            return calls >= 2 && !endpoints.isEmpty
        }
        let sockets = await capture.sockets
        XCTAssertGreaterThanOrEqual(sockets.count, 2)
        let latest = try XCTUnwrap(sockets.last)
        let endpoints = await transport.localEndpoints()
        XCTAssertTrue(try endpoints.allSatisfy { try DiscoveryEndpoint($0).port == latest.port })
        for socket in sockets.dropLast() {
            do {
                try await socket.send(Data([1]), to: DiscoveryEndpoint(host: "127.0.0.1", port: latest.port))
                XCTFail("Stale socket was not closed")
            } catch {}
        }
        await transport.close()
    }

    func testCloseCancelsPendingEncryptedRelayMarker() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        let gate = DiscoveryGate()
        let remote = try NaClBox.publicKey(for: NaClBox.random(32))
        let transport = NetworkTransport(privateKey: NaClBox.random(32), discoPrivateKey: NaClBox.random(32),
                                         relayNodes: [fixture.node], relayFirst: true, discovery: loopbackDiscovery())
        try await transport.start()
        try await transport.configurePeer(nodeKey: remote, discoKey: NaClBox.publicKey(for: NaClBox.random(32)))
        await transport.setRelayMarkerProvider { _ in
            await gate.wait()
            return Data(repeating: 0, count: 32)
        }
        await transport.resetNetwork()
        try await eventually { await gate.calls == 1 }
        let sending = Task { try await transport.send(to: remote, packet: Data([4, 0, 0, 0])) }
        await transport.close()
        await gate.release()
        do { try await within { try await sending.value }; XCTFail("Closed marker wait allowed an application send") }
        catch {}
        let endpoints = await transport.localEndpoints()
        XCTAssertTrue(endpoints.isEmpty)
    }

    func testNotificationDuringStartupIsAppliedOnceReady() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        let gate = DiscoveryGate()
        let transport = NetworkTransport(privateKey: NaClBox.random(32), discoPrivateKey: NaClBox.random(32),
                                         relayNodes: [fixture.node], relayFirst: false, discovery: gatedDiscovery(gate))
        let starting = Task { try await transport.start() }
        try await eventually { await gate.calls == 1 }
        await transport.resetNetwork()
        await gate.release()
        try await starting.value
        try await eventually { await gate.calls == 2 }
        await transport.close()
    }

    func testFreshInvitationInsideThrottleWindowIsNotDropped() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        let localKey = NaClBox.random(32), localDisco = NaClBox.random(32)
        let remoteKey = NaClBox.random(32), remoteDisco = NaClBox.random(32)
        let localPublic = try NaClBox.publicKey(for: localKey)
        let remotePublic = try NaClBox.publicKey(for: remoteKey)
        let localDiscoPublic = try NaClBox.publicKey(for: localDisco)
        let transport = NetworkTransport(privateKey: localKey, discoPrivateKey: localDisco,
                                         relayNodes: [fixture.node], relayFirst: true, discovery: loopbackDiscovery())
        let relay = DERPTransport(node: fixture.node, privateKey: remoteKey)
        var continuation: AsyncStream<(Data, DiscoveryEndpoint)>.Continuation!
        let received = AsyncStream<(Data, DiscoveryEndpoint)> { continuation = $0 }
        let output = continuation!
        let socket = try UDPChannel { packet, endpoint in output.yield((packet, endpoint)) }
        let endpoint = DiscoveryEndpoint(host: "127.0.0.1", port: socket.port)
        let responder = Task {
            for await (packet, source) in received {
                guard let opened = try? Discovery.open(packet, privateKey: remoteDisco),
                      case let .ping(transaction, _) = opened.message else { continue }
                let pong = try Discovery.seal(.pong(transaction: transaction, observed: source), to: localDiscoPublic, privateKey: remoteDisco)
                try await socket.send(pong, to: source)
            }
        }
        defer { responder.cancel(); output.finish(); socket.close() }
        do {
            try await transport.start(); try await relay.connect()
            try await transport.configurePeer(nodeKey: remotePublic, discoKey: NaClBox.publicKey(for: remoteDisco))
            // Socket discovery can first advertise nothing, then fresh addresses
            // a few milliseconds later. The second invitation must be retained.
            for endpoints in [[], [endpoint]] {
                let packet = try Discovery.seal(.callMeMaybe(endpoints), to: localDiscoPublic, privateKey: remoteDisco)
                try await relay.send(to: localPublic, packet: packet)
            }
            try await within(3) {
                while await transport.path(to: remotePublic) == .relay { try await Task.sleep(nanoseconds: 20_000_000) }
            }
            await transport.close(); await relay.close()
        } catch { await transport.close(); await relay.close(); throw error }
    }

    func testWithdrawnInvitationInvalidatesPendingPongWhileAlreadyOnRelay() async throws {
        try await exerciseRetiredChallenge(resetMarker: false)
    }

    func testResetMarkerInvalidatesPendingPongWhileAlreadyOnRelay() async throws {
        try await exerciseRetiredChallenge(resetMarker: true)
    }

    private func exerciseRetiredChallenge(resetMarker: Bool) async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        let localKey = NaClBox.random(32), localDisco = NaClBox.random(32)
        let remoteKey = NaClBox.random(32), remoteDisco = NaClBox.random(32)
        let localPublic = try NaClBox.publicKey(for: localKey)
        let remotePublic = try NaClBox.publicKey(for: remoteKey)
        let localDiscoPublic = try NaClBox.publicKey(for: localDisco)
        let transport = NetworkTransport(privateKey: localKey, discoPrivateKey: localDisco,
                                         relayNodes: [fixture.node], relayFirst: true, discovery: loopbackDiscovery())
        let relay = DERPTransport(node: fixture.node, privateKey: remoteKey)
        let oldPackets = AsyncStream<(Data, DiscoveryEndpoint)>.makeStream()
        let newPackets = AsyncStream<(Data, DiscoveryEndpoint)>.makeStream()
        let oldSocket = try UDPChannel { oldPackets.continuation.yield(($0, $1)) }
        let newSocket = try UDPChannel { newPackets.continuation.yield(($0, $1)) }
        let oldEndpoint = DiscoveryEndpoint(host: "127.0.0.1", port: oldSocket.port)
        let newEndpoint = DiscoveryEndpoint(host: "127.0.0.1", port: newSocket.port)
        let oldPing = Task {
            for await (packet, source) in oldPackets.stream {
                if let opened = try? Discovery.open(packet, privateKey: remoteDisco),
                   case let .ping(transaction, _) = opened.message { return (transaction, source) }
            }
            throw TransportError.closed
        }
        let newPing = Task {
            for await (packet, source) in newPackets.stream {
                if let opened = try? Discovery.open(packet, privateKey: remoteDisco),
                   case let .ping(transaction, _) = opened.message { return (transaction, source) }
            }
            throw TransportError.closed
        }
        defer {
            oldPing.cancel(); newPing.cancel()
            oldPackets.continuation.finish(); newPackets.continuation.finish()
            oldSocket.close(); newSocket.close()
        }
        do {
            try await transport.start(); try await relay.connect()
            try await transport.configurePeer(nodeKey: remotePublic, discoKey: NaClBox.publicKey(for: remoteDisco))
            let oldHint = try Discovery.seal(.callMeMaybe([oldEndpoint]), to: localDiscoPublic, privateKey: remoteDisco)
            try await relay.send(to: localPublic, packet: oldHint)
            let (transaction, source) = try await within { try await oldPing.value }
            let oldPong = try Discovery.seal(.pong(transaction: transaction, observed: source), to: localDiscoPublic, privateKey: remoteDisco)
            if resetMarker {
                await transport.observeAuthenticatedTransport(from: remotePublic, viaRelay: true,
                                                              receiver: Data(repeating: 9, count: 4),
                                                              counter: 1, allowFallback: true)
                try await oldSocket.send(oldPong, to: source)
                try await Task.sleep(nanoseconds: 200_000_000)
                let afterMarker = await transport.path(to: remotePublic)
                XCTAssertEqual(afterMarker, .relay, "A reset marker retires pending challenges even while already on relay")
            }
            let newHint = try Discovery.seal(.callMeMaybe([newEndpoint]), to: localDiscoPublic, privateKey: remoteDisco)
            try await relay.send(to: localPublic, packet: newHint)
            _ = try await within { try await newPing.value }
            try await oldSocket.send(oldPong, to: source)
            try await Task.sleep(nanoseconds: 200_000_000)
            let path = await transport.path(to: remotePublic)
            XCTAssertEqual(path, .relay, "A withdrawn endpoint's earlier challenge cannot restore direct")
            await transport.close(); await relay.close()
        } catch { await transport.close(); await relay.close(); throw error }
    }

    func testChangedInvitationKeepsStreamOnRelayWhileNewPongsAreBlocked() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        let filter = DiscoveryPacketFilter()
        var serverConfig = TailcatServer.Configuration()
        serverConfig.relayFirst = true
        serverConfig.region = DERPRegion(regionID: 1, nodes: [fixture.node])
        serverConfig.directDiscovery = loopbackDiscovery()
        let server = TailcatServer(configuration: serverConfig)
        var client: TailcatClient?
        do {
            let listener = try await server.listen(port: 8082)
            var config = TailcatClient.Configuration()
            config.relayFirst = true; config.directDiscovery = loopbackDiscovery()
            config.directDiscovery.open = { receive in
                try UDPChannel { packet, endpoint in
                    if !filter.drops(packet) { receive(packet, endpoint) }
                }
            }
            let connected = try await TailcatClient.connect(to: server.address, configuration: config)
            client = connected
            let stream = try await connected.openTCP(port: 8082)
            let accepted = try await listener.accept()
            try await eventually {
                if case .direct = await connected.path,
                   case .direct = try? await server.path(to: connected.publicKey) { return true }
                return false
            }
            try await exchange(stream, accepted, "established before peer migration")
            filter.setBlocked(true)
            await connected.resetNetworkPaths()
            try await eventually { (try? await server.path(to: connected.publicKey)) == .relay }
            try await exchange(stream, accepted, "new discovery blocked; same stream must use relay")
            let blockedPath = await connected.path
            XCTAssertEqual(blockedPath, .relay)
            filter.setBlocked(false)
            await connected.retryDirect(); await server.retryDirect()
            try await eventually {
                if case .direct = await connected.path { return true }; return false
            }
            try await exchange(stream, accepted, "upgrade only after fresh challenge succeeds")
            await connected.close(); await server.close()
        } catch { await client?.close(); await server.close(); throw error }
    }

    func testAuthenticatedRelayDataRecoversBeforeAnyNewUDPOrEndpointHint() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        let gate = DiscoveryGate(); await gate.release()
        var serverConfig = TailcatServer.Configuration()
        serverConfig.relayFirst = true
        serverConfig.region = DERPRegion(regionID: 1, nodes: [fixture.node])
        serverConfig.directDiscovery = loopbackDiscovery()
        let server = TailcatServer(configuration: serverConfig)
        var client: TailcatClient?
        var stage = "connect"
        do {
            let listener = try await server.listen(port: 8083)
            var config = TailcatClient.Configuration()
            config.relayFirst = true; config.directDiscovery = gatedDiscovery(gate)
            let connected = try await TailcatClient.connect(to: server.address, configuration: config)
            client = connected
            let stream = try await connected.openTCP(port: 8083)
            let accepted = try await listener.accept()
            stage = "initial direct"
            try await eventually {
                if case .direct = await connected.path,
                   case .direct = try? await server.path(to: connected.publicKey) { return true }
                return false
            }
            try await exchange(stream, accepted, "working direct stream")
            stage = "first blocked-discovery reset"
            await gate.block()
            await connected.resetNetworkPaths()
            // There is no new UDP socket, address announcement or matching Pong.
            // The ordered encrypted reset marker must make replies use DERP.
            try await exchange(stream, accepted, "relay traffic before any new endpoint hint")
            let responsePath = try await server.path(to: connected.publicKey)
            XCTAssertEqual(responsePath, .relay)
            await gate.release(); await connected.retryDirect(); await server.retryDirect()
            stage = "first revalidation"
            try await eventually {
                if case .direct = await connected.path,
                   case .direct = try? await server.path(to: connected.publicKey) { return true }
                return false
            }
            // A rapid second asymmetric change must still recover, even with
            // all new UDP/endpoint hints blocked.
            await gate.block()
            await connected.resetNetworkPaths()
            stage = "rapid second reset"
            try await within(5) {
                try await self.exchange(stream, accepted, "another rapid rebind without discovery")
            }
            let settlingResponsePath = try await server.path(to: connected.publicKey)
            XCTAssertEqual(settlingResponsePath, .relay)
            await gate.release(); await connected.retryDirect(); await server.retryDirect()
            stage = "continuous revalidation"
            try await within(15) {
                var consecutiveDirect = 0
                while consecutiveDirect < 60 {
                    try await self.exchange(stream, accepted, "traffic continues throughout direct recovery")
                    if case .direct = await connected.path,
                       case .direct = try? await server.path(to: connected.publicKey) {
                        consecutiveDirect += 1
                    } else { consecutiveDirect = 0 }
                    try await Task.sleep(nanoseconds: 50_000_000)
                }
            }
            let sourceKey = await connected.publicKey.rawValue
            stage = "invalid markers"
            let marker = try XCTUnwrap(fixture.lastKeepalive(from: sourceKey))
            let destination = try await server.address.serverPublicKey.rawValue
            try fixture.inject(marker, from: sourceKey, to: destination)
            var forged = marker
            forged[8] ^= 0x40; forged[31] ^= 0x01
            try fixture.inject(forged, from: sourceKey, to: destination)
            try await Task.sleep(nanoseconds: 200_000_000)
            let afterInvalidMarkers = try await server.path(to: connected.publicKey)
            guard case .direct = afterInvalidMarkers else {
                XCTFail("Replayed or forged encrypted reset marker changed a healthy route")
                throw TransportError.closed
            }
            await gate.block()
            await connected.resetNetworkPaths()
            stage = "third reset"
            try await exchange(stream, accepted, "a later one-sided rebind still falls back")
            let repeatedResponsePath = try await server.path(to: connected.publicKey)
            XCTAssertEqual(repeatedResponsePath, .relay)
            await gate.release()
            await connected.close(); await server.close()
        } catch {
            XCTFail("Marker recovery failed during \(stage): \(error)")
            await gate.release(); await client?.close(); await server.close(); throw error
        }
    }

    private func wakePair(_ fixture: LocalRelay, port: UInt16, client clientDiscovery: DirectDiscovery? = nil,
                          server serverDiscovery: DirectDiscovery? = nil) async throws
        -> (server: TailcatServer, client: TailcatClient, stream: TailcatConnection, accepted: TailcatConnection) {
        var serverConfig = TailcatServer.Configuration()
        serverConfig.relayFirst = true
        serverConfig.region = DERPRegion(regionID: 1, nodes: [fixture.node])
        serverConfig.directDiscovery = serverDiscovery ?? loopbackDiscovery()
        let server = TailcatServer(configuration: serverConfig)
        let listener = try await server.listen(port: port)
        var config = TailcatClient.Configuration()
        config.relayFirst = true; config.directDiscovery = clientDiscovery ?? loopbackDiscovery()
        let client = try await TailcatClient.connect(to: server.address, configuration: config)
        let stream = try await client.openTCP(port: port)
        return (server, client, stream, try await listener.accept())
    }

    func testWakeAndNetworkChangeKeepTheRelayRouteAndSessionThatAnswer() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        let pair = try await wakePair(fixture, port: 8090)
        do {
            try await eventually {
                if case .direct = await pair.client.path,
                   case .direct = try? await pair.server.path(to: pair.client.publicKey) { return true }
                return false
            }
            try await exchange(pair.stream, pair.accepted, "direct before waking")
            let route = await pair.client.path
            let connections = fixture.authenticatedConnections, initiations = fixture.relayedInitiations
            await pair.client.wake()
            try await Task.sleep(nanoseconds: 2_500_000_000)
            try await exchange(pair.stream, pair.accepted, "same stream after waking with nothing broken")
            await pair.client.notifyNetworkChanged()
            try await Task.sleep(nanoseconds: 2_500_000_000)
            try await exchange(pair.stream, pair.accepted, "same stream after a network change that broke nothing")
            let after = await pair.client.path
            XCTAssertEqual(after, route, "A direct route that answers is kept")
            XCTAssertEqual(fixture.authenticatedConnections, connections, "So is the relay connection")
            XCTAssertEqual(fixture.relayedInitiations, initiations, "And the session")
            await pair.client.close(); await pair.server.close()
        } catch { await pair.client.close(); await pair.server.close(); throw error }
    }

    func testWakeReplacesASilentRelayWithinItsDeadlineAndKeepsTheSession() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        let gate = DiscoveryGate() // No UDP: the relay is the only way.
        let pair = try await wakePair(fixture, port: 8091, client: gatedDiscovery(gate))
        do {
            try await exchange(pair.stream, pair.accepted, "relay before waking")
            let connections = fixture.authenticatedConnections, initiations = fixture.relayedInitiations
            fixture.silence(await pair.client.publicKey.rawValue)
            let woke = Date()
            await pair.client.wake()
            try await eventually { fixture.authenticatedConnections > connections }
            let elapsed = Date().timeIntervalSince(woke)
            print("SILENT-RELAY-RECOVERY \(Int(elapsed * 1000))ms")
            // Three of its round trips, at least a second.
            XCTAssertGreaterThanOrEqual(elapsed, 0.9); XCTAssertLessThan(elapsed, 2.5)
            try await exchange(pair.stream, pair.accepted, "same stream over the replaced relay")
            XCTAssertEqual(fixture.relayedInitiations, initiations, "The session is kept")
            await gate.release(); await pair.client.close(); await pair.server.close()
        } catch { await gate.release(); await pair.client.close(); await pair.server.close(); throw error }
    }

    /// A move nobody was told of: what a flow wrote into the dead relay
    /// connection goes again as soon as a check has it replaced, not when
    /// its timer, backed off meanwhile, next comes round.
    func testAReplacedRelayGetsWhatItsFlowsOweAtOnce() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        let gate = DiscoveryGate() // No UDP: the relay is the only way.
        let pair = try await wakePair(fixture, port: 8094, client: gatedDiscovery(gate))
        do {
            try await exchange(pair.stream, pair.accepted, "relay before the move")
            let connections = fixture.authenticatedConnections
            fixture.silence(await pair.client.publicKey.rawValue)
            let stream = pair.stream
            let write = Task { try await stream.write(Data("owed".utf8)) }
            // Sent again at 0.5, 1.5 and 3.5 s into the dead connection; next at 7.5 s.
            try await Task.sleep(nanoseconds: 4_000_000_000)
            let woke = Date()
            await pair.client.wake()
            var received = Data()
            while received.count < 4 { received.append(try await pair.accepted.read()) }
            let elapsed = Date().timeIntervalSince(woke)
            print("OWED-AFTER-RELAY-REPLACED \(Int(elapsed * 1000))ms")
            XCTAssertEqual(received, Data("owed".utf8))
            XCTAssertGreaterThan(fixture.authenticatedConnections, connections)
            // The check's second, the reconnect and a round trip; not the 3.5 s to the timer.
            XCTAssertLessThan(elapsed, 2.5)
            try await write.value
            try await exchange(pair.stream, pair.accepted, "same stream after")
            await gate.release(); await pair.client.close(); await pair.server.close()
        } catch { await gate.release(); await pair.client.close(); await pair.server.close(); throw error }
    }

    /// A slow relay that answers is kept: its check waits three of its
    /// round trips, measured as it connected, not a flat second.
    func testACheckKeepsALiveRelayWithASlowRoundTrip() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        fixture.latency = 1.2
        let gate = DiscoveryGate()
        let pair = try await wakePair(fixture, port: 8095, client: gatedDiscovery(gate))
        do {
            try await exchange(pair.stream, pair.accepted, "slow relay before the check")
            let connections = fixture.authenticatedConnections
            await pair.client.wake()
            try await Task.sleep(nanoseconds: 4_500_000_000)
            XCTAssertEqual(fixture.authenticatedConnections, connections, "a relay that answers in 1.2 s was replaced")
            try await exchange(pair.stream, pair.accepted, "same relay after the check")
            await gate.release(); await pair.client.close(); await pair.server.close()
        } catch { await gate.release(); await pair.client.close(); await pair.server.close(); throw error }
    }

    func testWakeDropsADirectRouteThatStoppedAnswering() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        let filter = DiscoveryPacketFilter()
        var discovery = loopbackDiscovery()
        discovery.open = { receive in try UDPChannel { packet, endpoint in if !filter.drops(packet) { receive(packet, endpoint) } } }
        let pair = try await wakePair(fixture, port: 8092, client: discovery)
        do {
            try await eventually {
                if case .direct = await pair.client.path,
                   case .direct = try? await pair.server.path(to: pair.client.publicKey) { return true }
                return false
            }
            filter.setBlocked(true)
            await pair.client.wake()
            try await Task.sleep(nanoseconds: 1_500_000_000)
            let during = await pair.client.path
            guard case .direct = during else { XCTFail("Dropped before its deadline"); throw TransportError.closed }
            try await eventually { await pair.client.path == .relay }
            try await exchange(pair.stream, pair.accepted, "same stream over the relay")
            await pair.client.close(); await pair.server.close()
        } catch { await pair.client.close(); await pair.server.close(); throw error }
    }

    func testDataRacingTheWakeProbeOfAnExpiredRouteStillGoesDirect() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        let filter = DiscoveryPacketFilter()
        var discovery = loopbackDiscovery()
        discovery.open = { receive in try UDPChannel { packet, endpoint in if !filter.drops(packet) { receive(packet, endpoint) } } }
        // Over loopback the relay is as fast as UDP: keep the relay reset marker
        // from landing at the server while it is answering the new Ping.
        var serverDiscovery = loopbackDiscovery()
        serverDiscovery.open = { receive in
            try UDPChannel { packet, endpoint in
                guard Discovery.isPacket(packet) else { return receive(packet, endpoint) }
                DispatchQueue.global().asyncAfter(deadline: .now() + 0.02) { receive(packet, endpoint) }
            }
        }
        let pair = try await wakePair(fixture, port: 8095, client: discovery, server: serverDiscovery)
        do {
            try await eventually {
                if case .direct = await pair.client.path,
                   case .direct = try? await pair.server.path(to: pair.client.publicKey) { return true }
                return false
            }
            // Startup discovery ends first, so the wake sends a probe of its own.
            try await Task.sleep(nanoseconds: 1_500_000_000)
            // Back after a minute: the route's last Pong is past its lifetime. The
            // wake's Pongs are still on their way when the first data goes out.
            await pair.client.ageDirectForTesting(by: 31)
            filter.setBlocked(true)
            await pair.client.wake()
            try await Task.sleep(nanoseconds: 150_000_000)
            filter.setBlocked(false)
            try await exchange(pair.stream, pair.accepted, "first data after waking")
            let sent = Date()
            try await within(0.5) {
                while true {
                    if case .direct = await pair.client.path { return }
                    try await Task.sleep(nanoseconds: 10_000_000)
                }
            }
            print("EXPIRED-ROUTE-DIRECT-AGAIN \(Int(Date().timeIntervalSince(sent) * 1000))ms")
            await pair.client.close(); await pair.server.close()
        } catch { await pair.client.close(); await pair.server.close(); throw error }
    }

    func testAClientSuspendedBeforeTheServerFirstAnswersKeepsItsServer() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        var serverConfig = TailcatServer.Configuration()
        serverConfig.relayFirst = true
        serverConfig.region = DERPRegion(regionID: 1, nodes: [fixture.node])
        serverConfig.directDiscovery = loopbackDiscovery()
        let server = TailcatServer(configuration: serverConfig)
        let listener = try await server.listen(port: 8096)
        let address = try await server.address
        // The server's answer to the key confirmation is still on its way when
        // the client is suspended: nothing authenticated has come back yet.
        fixture.dropKeepalives(from: address.serverPublicKey.rawValue, 1)
        var config = TailcatClient.Configuration()
        config.relayFirst = true; config.directDiscovery = loopbackDiscovery()
        let client = try await TailcatClient.connect(to: address, configuration: config)
        do {
            await client.ageForTesting(by: 31)
            // A timer tick runs before the first request after waking.
            try await Task.sleep(nanoseconds: 1_500_000_000)
            let (stream, accepted) = try await within {
                let stream = try await client.openTCP(port: 8096)
                return (stream, try await listener.accept())
            }
            try await exchange(stream, accepted, "first request after waking")
            await client.close(); await server.close()
        } catch { await client.close(); await server.close(); throw error }
    }

    func testAResetMarkerJustAfterAProbeIsRevalidatedAtOnce() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        let filter = DiscoveryPacketFilter()
        var discovery = loopbackDiscovery()
        discovery.open = { receive in try UDPChannel { packet, endpoint in if !filter.drops(packet) { receive(packet, endpoint) } } }
        var serverDiscovery = loopbackDiscovery()
        serverDiscovery.open = { receive in
            try UDPChannel { packet, endpoint in
                guard Discovery.isPacket(packet) else { return receive(packet, endpoint) }
                DispatchQueue.global().asyncAfter(deadline: .now() + 0.02) { receive(packet, endpoint) }
            }
        }
        let pair = try await wakePair(fixture, port: 8097, client: discovery, server: serverDiscovery)
        do {
            try await eventually {
                if case .direct = await pair.client.path,
                   case .direct = try? await pair.server.path(to: pair.client.publicKey) { return true }
                return false
            }
            try await Task.sleep(nanoseconds: 1_500_000_000)
            // The wake's invitation has the server probe; the client's own Pongs
            // are lost, so its first data sends the server a reset marker just after.
            await pair.client.ageDirectForTesting(by: 31)
            filter.setBlocked(true)
            await pair.client.wake()
            try await Task.sleep(nanoseconds: 150_000_000)
            filter.setBlocked(false)
            try await exchange(pair.stream, pair.accepted, "first data after waking")
            let sent = Date()
            try await within(0.5) {
                while true {
                    if case .direct = try await pair.server.path(to: pair.client.publicKey) { return }
                    try await Task.sleep(nanoseconds: 10_000_000)
                }
            }
            print("MARKER-DIRECT-AGAIN \(Int(Date().timeIntervalSince(sent) * 1000))ms")
            await pair.client.close(); await pair.server.close()
        } catch { await pair.client.close(); await pair.server.close(); throw error }
    }

    func testALostInitiationIsRetriedWithinASecondWhileDataWaits() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        let gate = DiscoveryGate()
        let pair = try await wakePair(fixture, port: 8094, client: gatedDiscovery(gate))
        do {
            try await exchange(pair.stream, pair.accepted, "before the suspension")
            let initiations = fixture.relayedInitiations
            await pair.client.ageForTesting(by: 181)
            fixture.dropInitiations(1)
            let sent = Date()
            try await exchange(pair.stream, pair.accepted, "first send after the session expired")
            let elapsed = Date().timeIntervalSince(sent)
            print("LOST-INITIATION-FIRST-ECHO \(Int(elapsed * 1000))ms")
            XCTAssertLessThan(elapsed, 2.5, "Not held for the five-second retry")
            XCTAssertEqual(fixture.relayedInitiations, initiations + 2)
            await gate.release(); await pair.client.close(); await pair.server.close()
        } catch { await gate.release(); await pair.client.close(); await pair.server.close(); throw error }
    }

    func testAnExpiredSessionHandshakesAtTheFirstSendNotTheNextIdleMark() async throws {
        let fixture = try LocalRelay(); defer { fixture.close() }
        let gate = DiscoveryGate()
        let pair = try await wakePair(fixture, port: 8093, client: gatedDiscovery(gate))
        do {
            try await exchange(pair.stream, pair.accepted, "before the suspension")
            let initiations = fixture.relayedInitiations
            await pair.client.ageForTesting(by: 181)
            let sent = Date()
            try await exchange(pair.stream, pair.accepted, "first send after the session expired")
            let elapsed = Date().timeIntervalSince(sent)
            print("EXPIRED-SESSION-FIRST-ECHO \(Int(elapsed * 1000))ms")
            XCTAssertLessThan(elapsed, 2, "Rekeyed at once")
            XCTAssertEqual(fixture.relayedInitiations, initiations + 1)
            await gate.release(); await pair.client.close(); await pair.server.close()
        } catch { await gate.release(); await pair.client.close(); await pair.server.close(); throw error }
    }
}
#endif
