import Foundation
import XCTest
@testable import Tailcat

private actor PacketLink {
    struct Faults: Sendable {
        var dropSYN = false
        var dropSYNACK = false
        var dropData = false
        var dropACK = false
        var dropFIN = false
        var reorder = false
        var duplicate = false
        var reorderACKs = false
        var dropReopenWindow = false
        var dropFINACK = false
        var rejectSYN = false
        var rejectCorruptSYN = false
    }

    private var a: IPStack?
    private var b: IPStack?
    private var faults: Faults
    private var held: Data?
    private var heldACK: Data?
    private var sawData = false
    private var sawFIN = false
    private(set) var sawZeroWindow = false
    private(set) var drops = 0
    private(set) var deliveredResets = 0
    private(set) var deliveredACKs = 0
    private var lastClientFIN: Data?

    init(faults: Faults = Faults()) { self.faults = faults }

    func attach(_ a: IPStack, _ b: IPStack) { self.a = a; self.b = b }

    func dropNextData() { faults.dropData = true }

    func replayClientFIN() async {
        if let packet = lastClientFIN { await b?.receive(packet) }
    }

    func send(_ data: Data, fromA: Bool) async {
        guard let target = fromA ? b : a else { return }
        if data.count >= 60 && data[6] == 6 {
            let flags = data[53]
            let count = data.count - 40 - Int(data[52] >> 4) * 4
            if flags & 0x12 == 0x02 && faults.rejectSYN {
                faults.rejectSYN = false
                await target.rejectTCP(peer: Data(data[8..<24]), body: Data(data.dropFirst(40)))
                return
            }
            if flags & 0x12 == 0x02 && faults.rejectCorruptSYN {
                faults.rejectCorruptSYN = false
                var body = Data(data.dropFirst(40))
                body[16] ^= 1
                await target.rejectTCP(peer: Data(data[8..<24]), body: body)
            }
            if data[54] == 0 && data[55] == 0 { sawZeroWindow = true }
            if fromA && flags & 1 != 0 { sawFIN = true; lastClientFIN = data }
            if !fromA && flags == 0x10 && sawZeroWindow &&
                (data[54] != 0 || data[55] != 0) && faults.dropReopenWindow {
                faults.dropReopenWindow = false; drops += 1; return
            }
            if !fromA && flags == 0x10 && sawFIN && faults.dropFINACK {
                faults.dropFINACK = false; drops += 1; return
            }
            if flags & 0x12 == 0x02 && faults.dropSYN {
                faults.dropSYN = false; drops += 1; return
            }
            if flags & 0x12 == 0x12 && faults.dropSYNACK {
                faults.dropSYNACK = false; drops += 1; return
            }
            if count > 0 { sawData = true }
            if count > 0 && fromA && faults.dropData {
                faults.dropData = false; drops += 1; return
            }
            if count == 0 && flags == 0x10 && !fromA && sawData && faults.dropACK {
                faults.dropACK = false; drops += 1; return
            }
            if flags & 1 != 0 && faults.dropFIN {
                faults.dropFIN = false; drops += 1; return
            }
            if !fromA && count == 0 && flags == 0x10 && sawData && faults.reorderACKs {
                if let previous = heldACK {
                    heldACK = nil
                    faults.reorderACKs = false
                    await target.receive(data)
                    await target.receive(previous)
                    return
                }
                heldACK = data
                return
            }
            if count > 0 && fromA && faults.reorder {
                if let previous = held {
                    held = nil
                    faults.reorder = false
                    await target.receive(data)
                    await target.receive(previous)
                    return
                }
                held = data
                return
            }
            if count > 0 && faults.duplicate { await target.receive(data) }
        }
        await target.receive(data)
        if data.count >= 60 && data[6] == 6 && data[53] & 0x04 != 0 { deliveredResets += 1 }
        if data.count >= 60 && data[6] == 6 && data[53] == 0x10 { deliveredACKs += 1 }
    }
}

private actor PacketCapture {
    var packet: Data?
    func record(_ packet: Data) { self.packet = packet }
}

private actor PacketLog {
    private(set) var packets: [Data] = []
    func record(_ packet: Data) { packets.append(packet) }
}

private actor CompletionFlag {
    private(set) var completed = false
    func finish() { completed = true }
}

final class IPStackTests: XCTestCase {
    private let addressA = Data([0xfd, 0x7a, 0x11, 0x5c, 0xa1, 0xe0] + [UInt8](repeating: 0, count: 9) + [1])
    private let addressB = Data([0xfd, 0x7a, 0x11, 0x5c, 0xa1, 0xe0] + [UInt8](repeating: 0, count: 9) + [2])

    private func pair(_ faults: PacketLink.Faults = .init(),
                      receiveCapacity: Int = 1 << 20) async -> (IPStack, IPStack, PacketLink) {
        let link = PacketLink(faults: faults)
        let a = IPStack(address: addressA, receiveCapacity: receiveCapacity) { await link.send($0, fromA: true) }
        let b = IPStack(address: addressB, receiveCapacity: receiveCapacity) { await link.send($0, fromA: false) }
        await link.attach(a, b)
        return (a, b, link)
    }

    private func deadline<T: Sendable>(_ operation: @escaping @Sendable () async throws -> T) async throws -> T {
        try await withThrowingTaskGroup(of: T.self) { group in
            group.addTask { try await operation() }
            group.addTask {
                try await Task.sleep(nanoseconds: 25_000_000_000)
                throw TailcatTransportError.timedOut
            }
            defer { group.cancelAll() }
            return try await group.next()!
        }
    }

    private func drain(_ stream: TailcatConnection) async throws -> Data {
        var result = Data()
        while true {
            let chunk = try await stream.read(maximumBytes: 7777)
            if chunk.isEmpty { return result }
            result.append(chunk)
        }
    }

    func testReadingABufferedResponseSendsNoWindowUpdatePerRead() async throws {
        let (a, b, link) = await pair()
        let listener = try await b.listen(port: 87)
        let peer = addressB
        let response = Data((0..<20_000).map { UInt8(truncatingIfNeeded: $0 * 7) })
        try await deadline {
            let client = try await a.connect(peer: peer, port: 87)
            let server = try await listener.accept()
            // Returns once the client has acknowledged every byte: all of it is buffered.
            try await server.write(response)
            try await server.closeWrite()
            let before = await link.deliveredACKs
            var received = Data()
            while true {
                let chunk = try await client.read(maximumBytes: 1000)
                if chunk.isEmpty { break }
                received.append(chunk)
            }
            XCTAssertEqual(received, response)
            // The window never ran low, so the peer is told nothing it needs.
            let updates = await link.deliveredACKs - before
            XCTAssertLessThanOrEqual(updates, 1)
        }
        await a.close(); await b.close()
    }

    func testHalfCloseDrainsRequestAndBufferedResponse() async throws {
        let (a, b, _) = await pair()
        let listener = try await b.listen(port: 80)
        let peer = addressB
        let request = Data((0..<180_000).map { UInt8(truncatingIfNeeded: $0) })
        let response = Data(repeating: 0x7f, count: 91_234)
        try await deadline {
            async let accepted = listener.accept()
            let client = try await a.connect(peer: peer, port: 80)
            let server = try await accepted
            XCTAssertEqual(client.remoteAddress, peer)
            XCTAssertEqual(client.remotePort, 80)
            let requestWriter = Task {
                try await client.write(request)
                try await client.closeWrite()
            }
            let received = try await self.drain(server)
            XCTAssertEqual(received, request)
            try await requestWriter.value
            let responseWriter = Task {
                try await server.write(response)
                try await server.closeWrite()
            }
            let receivedResponse = try await self.drain(client)
            XCTAssertEqual(receivedResponse, response)
            try await responseWriter.value
            // EOF remains observable after already buffered bytes are drained.
            let eof = try await client.read()
            XCTAssertTrue(eof.isEmpty)
        }
        await a.close(); await b.close()
    }

    func testThousandsOfShortRequestsKeepEventStreamAlive() async throws {
        let (a, b, _) = await pair()
        let listener = try await b.listen(port: 80)
        let peer = addressB
        try await deadline {
            let events = try await a.connect(peer: peer, port: 80)
            let eventServer = try await listener.accept()
            let ready = Data("event: ready\ndata: {}\n\n".utf8)
            try await eventServer.write(ready)
            let receivedReady = try await events.read()
            XCTAssertEqual(receivedReady, ready)
            var firstPort: UInt16?
            var lastPort: UInt16 = 0
            for index in 0..<1300 {
                let client = try await a.connect(peer: peer, port: 80)
                let server = try await listener.accept()
                firstPort = firstPort ?? client.localPort
                lastPort = client.localPort
                let request = Data("GET /kv/entry?key=\(index) HTTP/1.1\r\nHost: fixture\r\nConnection: close\r\n\r\n".utf8)
                try await client.write(request)
                let received = try await server.read()
                XCTAssertEqual(received, request)
                let response = Data("HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}".utf8)
                try await server.write(response)
                await server.close()
                let returned = try await self.drain(client)
                XCTAssertEqual(returned, response)
                await client.close()
                if index % 50 == 0 {
                    let event = Data("event: change\ndata: \(index)\n\n".utf8)
                    try await eventServer.write(event)
                    let receivedEvent = try await events.read()
                    XCTAssertEqual(receivedEvent, event)
                }
            }
            let oldestRetained = await a.hasTCPFlow(peer: peer, localPort: firstPort!, remotePort: 80)
            let newestRetained = await a.hasTCPFlow(peer: peer, localPort: lastPort, remotePort: 80)
            XCTAssertFalse(oldestRetained, "Closing state evicts oldest flows instead of refusing new requests")
            XCTAssertTrue(newestRetained, "Recent closing flows remain available for late FINs")
            let total = await a.connectionCount
            XCTAssertEqual(total, 1025, "One active SSE connection and at most 1024 closing flows")
            let final = Data("event: change\ndata: after-bulk\n\n".utf8)
            try await eventServer.write(final)
            let receivedFinal = try await events.read()
            XCTAssertEqual(receivedFinal, final)
            await events.close(); await eventServer.close()
        }
        await a.close(); await b.close()
    }

    func testClosingFlowsAcknowledgeLateFINAndDisconnectRemovesThem() async throws {
        let (a, b, link) = await pair()
        let listener = try await b.listen(port: 80)
        let client = try await a.connect(peer: addressB, port: 80)
        let server = try await listener.accept()
        try await client.closeWrite()
        let eof = try await server.read()
        XCTAssertTrue(eof.isEmpty)
        try await server.closeWrite()
        await server.close(); await client.close()
        let before = await link.deliveredACKs
        await link.replayClientFIN()
        for _ in 0..<100 {
            if await link.deliveredACKs > before { break }
            try await Task.sleep(nanoseconds: 1_000_000)
        }
        let after = await link.deliveredACKs
        let resets = await link.deliveredResets
        XCTAssertGreaterThan(after, before)
        XCTAssertEqual(resets, 0, "Retained closing state ACKs a retransmitted FIN without resetting")
        await a.disconnect(peer: addressB)
        let count = await a.connectionCount
        let retained = await a.hasTCPFlow(peer: addressB, localPort: client.localPort, remotePort: 80)
        XCTAssertEqual(count, 0)
        XCTAssertFalse(retained)
        await a.close(); await b.close()
    }

    func testFlowStatisticsCountOpenAndClosingFlows() async throws {
        let (a, b, _) = await pair()
        let listener = try await b.listen(port: 80)
        let empty = await a.flowStatistics
        XCTAssertEqual(empty, TailcatFlowStatistics(open: 0, closing: 0))
        var clients: [TailcatConnection] = []
        var servers: [TailcatConnection] = []
        for _ in 0..<3 {
            clients.append(try await a.connect(peer: addressB, port: 80))
            servers.append(try await listener.accept())
        }
        let open = await a.flowStatistics
        let accepted = await b.flowStatistics
        XCTAssertEqual(open, TailcatFlowStatistics(open: 3, closing: 0))
        XCTAssertEqual(accepted, TailcatFlowStatistics(open: 3, closing: 0))
        // Closed by the application: no longer open, still owed a FIN/ACK.
        await clients[0].close(); await clients[1].close()
        let closing = await a.flowStatistics
        XCTAssertEqual(closing, TailcatFlowStatistics(open: 1, closing: 2))
        let total = await a.connectionCount
        XCTAssertEqual(total, closing.open + closing.closing)
        await a.disconnect(peer: addressB)
        let gone = await a.flowStatistics
        XCTAssertEqual(gone, TailcatFlowStatistics(open: 0, closing: 0))
        XCTAssertEqual(TailcatFlowStatistics.openLimit, 1024)
        XCTAssertEqual(TailcatFlowStatistics.listenerLimit, 256)
        XCTAssertEqual(TailcatFlowStatistics.closingLimit, 1024)
        for server in servers { await server.close() }
        await a.close(); await b.close()
    }

    func testActiveConnectionBudgetsStillApply() async throws {
        let (a, b, _) = await pair()
        var streams: [TailcatConnection] = []
        for port in 80..<84 {
            let listener = try await b.listen(port: UInt16(port))
            for _ in 0..<256 {
                streams.append(try await a.connect(peer: addressB, port: UInt16(port)))
                _ = try await listener.accept()
            }
            if port == 80 {
                do {
                    _ = try await a.connect(peer: addressB, port: 80)
                    XCTFail("The live per-listener budget must remain bounded")
                } catch { XCTAssertEqual(error as? TailcatTransportError, .connectionRefused) }
            }
        }
        do {
            _ = try await a.connect(peer: addressB, port: 84)
            XCTFail("The live global budget must remain bounded")
        } catch { XCTAssertEqual(error as? TailcatTransportError, .resourceLimit) }
        let count = await a.connectionCount
        XCTAssertEqual(count, 1024)
        await a.close(); await b.close()
    }

    func testRetransmitsLostHandshakeDataACKAndFIN() async throws {
        let faults = PacketLink.Faults(dropSYN: true, dropSYNACK: true, dropData: true,
                                     dropACK: true, dropFIN: true)
        let (a, b, link) = await pair(faults)
        let listener = try await b.listen(port: 443)
        let peer = addressB
        let payload = Data(repeating: 0x31, count: 23_456)
        try await deadline {
            let client = try await a.connect(peer: peer, port: 443)
            let server = try await listener.accept()
            let writer = Task {
                try await client.write(payload)
                try await client.closeWrite()
            }
            let received = try await self.drain(server)
            XCTAssertEqual(received, payload)
            try await writer.value
            let count = await link.drops
            XCTAssertEqual(count, 5)
        }
        await a.close(); await b.close()
    }

    func testLateResetAfterBothFINsPreservesBufferedTailAndEOF() async throws {
        let (a, b, link) = await pair(receiveCapacity: 65_535)
        let listener = try await b.listen(port: 86)
        let peer = addressB
        // Nearly the whole receive window, so that reading reopens it by more
        // than a segment while the peer has less than two left to send.
        let response = Data((0..<64_000).map { UInt8(truncatingIfNeeded: $0 * 19) })
        try await deadline {
            let client = try await a.connect(peer: peer, port: 86)
            let server = try await listener.accept()
            try await client.write(Data([1, 2, 3]))
            try await client.closeWrite()
            let request = try await self.drain(server)
            XCTAssertEqual(request, Data([1, 2, 3]))
            try await server.write(response)
            try await server.closeWrite()
            // Forget the peer's fully closed flow while its response remains
            // buffered at the client. Reading reopens the receive window and
            // elicits an exact-sequence RST from the unknown-flow handler.
            await server.abort()
            let prefix = try await client.read(maximumBytes: 4096)
            XCTAssertEqual(prefix, Data(response.prefix(4096)))
            for _ in 0..<100 {
                if await link.deliveredResets > 0 { break }
                try await Task.sleep(nanoseconds: 10_000_000)
            }
            let resets = await link.deliveredResets
            XCTAssertEqual(resets, 1)
            let tail = try await self.drain(client)
            XCTAssertEqual(tail, Data(response.dropFirst(4096)))
        }
        await a.close(); await b.close()
    }

    func testReorderedAndDuplicatedSegmentsPreserveByteStream() async throws {
        let (a, b, _) = await pair(.init(reorder: true, duplicate: true, reorderACKs: true))
        let listener = try await b.listen(port: 123)
        let peer = addressB
        let bytes = Data((0..<400_000).map { UInt8(truncatingIfNeeded: $0 * 31) })
        try await deadline {
            let client = try await a.connect(peer: peer, port: 123)
            let server = try await listener.accept()
            let writer = Task {
                try await client.write(bytes)
                try await client.closeWrite()
            }
            let actual = try await self.drain(server)
            XCTAssertEqual(actual, bytes)
            try await writer.value
        }
        await a.close(); await b.close()
    }

    func testZeroWindowBackpressureAndReopen() async throws {
        let (a, b, link) = await pair(receiveCapacity: 65_535)
        let listener = try await b.listen(port: 321)
        let peer = addressB
        let bytes = Data(repeating: 0xa7, count: 300_000)
        try await deadline {
            let client = try await a.connect(peer: peer, port: 321)
            let server = try await listener.accept()
            let writer = Task {
                try await client.write(bytes)
                try await client.closeWrite()
            }
            for _ in 0..<200 {
                if await link.sawZeroWindow { break }
                try await Task.sleep(nanoseconds: 10_000_000)
            }
            let sawZero = await link.sawZeroWindow
            XCTAssertTrue(sawZero)
            let received = try await self.drain(server)
            XCTAssertEqual(received, bytes)
            try await writer.value
        }
        await a.close(); await b.close()
    }

    func testMultipleClientsTransferMegabytes() async throws {
        let (a, b, _) = await pair()
        let listener = try await b.listen(port: 999)
        let peer = addressB
        let payload = Data((0..<2_000_000).map { UInt8(truncatingIfNeeded: $0) })
        try await deadline {
            try await withThrowingTaskGroup(of: Void.self) { group in
                for _ in 0..<3 {
                    group.addTask {
                        let client = try await a.connect(peer: peer, port: 999)
                        try await client.write(payload)
                        try await client.closeWrite()
                    }
                    group.addTask {
                        let server = try await listener.accept()
                        let received = try await self.drain(server)
                        XCTAssertEqual(received, payload)
                    }
                }
                try await group.waitForAll()
            }
        }
        await a.close(); await b.close()
    }

    func testScaledWindowAcknowledgesAnUnreadResponsePastSixtyFourKilobytes() async throws {
        // Window scaling is negotiated whichever end opens the flow.
        for aConnects in [true, false] {
            let (a, b, link) = await pair()
            let (opener, listening) = aConnects ? (a, b) : (b, a)
            let peer = aConnects ? addressB : addressA
            let listener = try await listening.listen(port: 90)
            let response = Data((0..<900_000).map { UInt8(truncatingIfNeeded: $0 * 13) })
            try await deadline {
                let client = try await opener.connect(peer: peer, port: 90)
                let server = try await listener.accept()
                // Returns once the client has acknowledged every byte, none of it read.
                try await server.write(response)
                try await server.closeWrite()
                let received = try await self.drain(client)
                XCTAssertEqual(received, response)
            }
            let sawZero = await link.sawZeroWindow
            XCTAssertFalse(sawZero)
            await a.close(); await b.close()
        }
    }

    func testScaledZeroWindowAndLostUpdateRecover() async throws {
        // 300,000 bytes take a shift of 3, so windows round down to 8 bytes.
        let (a, b, link) = await pair(.init(dropReopenWindow: true), receiveCapacity: 300_000)
        let listener = try await b.listen(port: 91)
        let peer = addressB
        let bytes = Data((0..<1_000_000).map { UInt8(truncatingIfNeeded: $0 * 29) })
        try await deadline {
            let client = try await a.connect(peer: peer, port: 91)
            let server = try await listener.accept()
            let writer = Task {
                try await client.write(bytes)
                try await client.closeWrite()
            }
            for _ in 0..<200 {
                if await link.sawZeroWindow { break }
                try await Task.sleep(nanoseconds: 10_000_000)
            }
            let zero = await link.sawZeroWindow
            XCTAssertTrue(zero)
            // The first reopened window is lost; the sender's persist probe recovers.
            let received = try await self.drain(server)
            XCTAssertEqual(received, bytes)
            try await writer.value
            let drops = await link.drops
            XCTAssertEqual(drops, 1)
        }
        await a.close(); await b.close()
    }

    func testListenerScalesOnlyForAPeerThatOffersScaling() async throws {
        for peerShift: UInt8? in [nil, 7] {
            let log = PacketLog()
            let a = IPStack(address: addressA) { await log.record($0) }
            let listener = try await a.listen(port: 80)
            let scale: [UInt8] = peerShift.map { [1, 3, 3, $0] } ?? []
            await a.receive(segment(sequence: 100, ack: 0, flags: 0x02, window: 2000,
                                    options: [2, 4, 0x04, 0xc4] + scale))
            let synACK = try await sent(log, count: 1)[0]
            XCTAssertEqual(synACK[53], 0x12)
            XCTAssertEqual(windowScale(synACK), peerShift == nil ? nil : 5)
            XCTAssertEqual(window(synACK), 65_535)
            // Scaled by 2^7, the peer's ten opens 1,280 bytes; unscaled, 10.
            await a.receive(segment(sequence: 101, ack: sequenceNumber(synACK) &+ 1, flags: 0x10, window: 10))
            let server = try await deadline { try await listener.accept() }
            let writer = Task { try await server.write(Data(repeating: 1, count: 5000)) }
            let allowed = peerShift == nil ? 10 : 1280
            try await waitForPayload(log, allowed)
            let before = await log.packets.count
            await a.receive(segment(sequence: 101, ack: sequenceNumber(synACK) &+ 1, flags: 0x18,
                                    window: 10, payload: [UInt8](repeating: 2, count: 1000)))
            let ack = try await sent(log, count: before + 1)[before]
            XCTAssertEqual(acknowledgementNumber(ack), 1101)
            XCTAssertEqual(window(ack), peerShift == nil ? 65_535 : UInt16(((1 << 20) - 1000) >> 5))
            let total = payloadCount(await log.packets)
            XCTAssertEqual(total, allowed)
            await a.close()
            _ = try? await writer.value
        }
    }

    func testConnectorFallsBackToUnscaledWindowsWithoutPeerOption() async throws {
        for peerShift: UInt8? in [nil, 2] {
            let log = PacketLog()
            let a = IPStack(address: addressA) { await log.record($0) }
            let peer = addressB
            let connect = Task { try await a.connect(peer: peer, port: 80) }
            let syn = try await sent(log, count: 1)[0]
            XCTAssertEqual(syn[53], 0x02)
            XCTAssertEqual(windowScale(syn), 5)
            XCTAssertEqual(window(syn), 65_535)
            let local = UInt16(syn[40]) << 8 | UInt16(syn[41])
            let scale: [UInt8] = peerShift.map { [1, 3, 3, $0] } ?? []
            // A SYN-ACK's own window is never scaled.
            await a.receive(segment(ports: (80, local), sequence: 500, ack: sequenceNumber(syn) &+ 1, flags: 0x12,
                                    window: 2000, options: [2, 4, 0x04, 0xc4] + scale))
            let client = try await deadline { try await connect.value }
            let handshakeACK = try await sent(log, count: 2)[1]
            XCTAssertEqual(window(handshakeACK), peerShift == nil ? 65_535 : 32_768)
            let writer = Task { try await client.write(Data(repeating: 1, count: 10_000)) }
            try await waitForPayload(log, 2000)
            // The peer acknowledges both segments and opens 1,000 << shift.
            await a.receive(segment(ports: (80, local), sequence: 501, ack: sequenceNumber(syn) &+ 2001,
                                    flags: 0x10, window: 1000))
            let opened = peerShift == nil ? 1000 : 4000
            try await waitForPayload(log, 2000 + opened)
            await a.close()
            _ = try? await writer.value
        }
    }

    /// A segment from B to A, as a stack other than this one would send it.
    private func segment(ports: (UInt16, UInt16) = (4000, 80), sequence: UInt32, ack: UInt32, flags: UInt8,
                         window: UInt16, options: [UInt8] = [], payload: [UInt8] = []) -> Data {
        var tcp = [UInt8](repeating: 0, count: 20) + options + payload
        func put16(_ offset: Int, _ value: UInt16) {
            tcp[offset] = UInt8(value >> 8); tcp[offset + 1] = UInt8(truncatingIfNeeded: value)
        }
        put16(0, ports.0); put16(2, ports.1)
        put16(4, UInt16(sequence >> 16)); put16(6, UInt16(truncatingIfNeeded: sequence))
        put16(8, UInt16(ack >> 16)); put16(10, UInt16(truncatingIfNeeded: ack))
        tcp[12] = UInt8((20 + options.count) / 4) << 4
        tcp[13] = flags
        put16(14, window)
        let summed = [UInt8](addressB + addressA) + tcp + (tcp.count % 2 == 1 ? [0] : [])
        var sum = UInt32(tcp.count) + 6
        for index in stride(from: 0, to: summed.count, by: 2) { sum += UInt32(summed[index]) << 8 | UInt32(summed[index + 1]) }
        while sum >> 16 != 0 { sum = (sum & 0xffff) + (sum >> 16) }
        put16(16, ~UInt16(sum))
        var ip = [UInt8](repeating: 0, count: 40)
        ip[0] = 0x60; ip[4] = UInt8(tcp.count >> 8); ip[5] = UInt8(truncatingIfNeeded: tcp.count); ip[6] = 6; ip[7] = 64
        ip.replaceSubrange(8..<24, with: addressB)
        ip.replaceSubrange(24..<40, with: addressA)
        return Data(ip + tcp)
    }

    private func sent(_ log: PacketLog, count: Int) async throws -> [Data] {
        for _ in 0..<200 {
            let packets = await log.packets
            if packets.count >= count { return packets }
            try await Task.sleep(nanoseconds: 10_000_000)
        }
        XCTFail("Expected \(count) packets"); throw TailcatTransportError.timedOut
    }

    /// Waits for exactly this many payload bytes, and then for no more.
    private func waitForPayload(_ log: PacketLog, _ bytes: Int) async throws {
        for _ in 0..<200 {
            if payloadCount(await log.packets) >= bytes { break }
            try await Task.sleep(nanoseconds: 10_000_000)
        }
        try await Task.sleep(nanoseconds: 50_000_000)
        let total = payloadCount(await log.packets)
        XCTAssertEqual(total, bytes)
    }

    private func payloadCount(_ packets: [Data]) -> Int {
        packets.filter { $0[53] & 0x04 == 0 }.reduce(0) { $0 + $1.count - 40 - Int($1[52] >> 4) * 4 }
    }

    private func window(_ packet: Data) -> UInt16 { UInt16(packet[54]) << 8 | UInt16(packet[55]) }

    private func sequenceNumber(_ packet: Data) -> UInt32 {
        packet[44..<48].reduce(0) { $0 << 8 | UInt32($1) }
    }

    private func acknowledgementNumber(_ packet: Data) -> UInt32 {
        packet[48..<52].reduce(0) { $0 << 8 | UInt32($1) }
    }

    private func windowScale(_ packet: Data) -> UInt8? {
        var offset = 60
        let end = 40 + Int(packet[52] >> 4) * 4
        while offset + 1 < end {
            if packet[offset] == 1 { offset += 1; continue }
            if packet[offset] == 3 { return packet[offset + 2] }
            offset += Int(packet[offset + 1])
        }
        return nil
    }

    func testLostWindowUpdateAndFinalFINAcknowledgementRecover() async throws {
        let (a, b, link) = await pair(.init(dropReopenWindow: true, dropFINACK: true), receiveCapacity: 65_535)
        let listener = try await b.listen(port: 81)
        let peer = addressB
        let bytes = Data(repeating: 0x7b, count: 90_000)
        try await deadline {
            let client = try await a.connect(peer: peer, port: 81)
            let server = try await listener.accept()
            let writer = Task {
                try await client.write(bytes)
                try await client.closeWrite()
            }
            for _ in 0..<200 {
                if await link.sawZeroWindow { break }
                try await Task.sleep(nanoseconds: 10_000_000)
            }
            let zero = await link.sawZeroWindow
            XCTAssertTrue(zero)
            // Exactly one read advertises a reopened window; lose that update
            // and require the sender's persist probe to make progress.
            var received = try await server.read(maximumBytes: 65_535)
            XCTAssertEqual(received.count, 65_535)
            // Remaining data fits the reopened buffer. FIN's ACK is also lost,
            // so closeWrite must recover by retransmitting FIN after peer EOF.
            try await writer.value
            received.append(try await self.drain(server))
            XCTAssertEqual(received, bytes)
            let drops = await link.drops
            XCTAssertEqual(drops, 2)
        }
        await a.close(); await b.close()
    }

    func testDrainWaitsForACKsAndLeavesWriteSideOpen() async throws {
        let (a, b, link) = await pair(receiveCapacity: 65_535)
        let listener = try await b.listen(port: 82)
        let peer = addressB
        let payload = Data(repeating: 0x32, count: 100_000)
        try await deadline {
            let client = try await a.connect(peer: peer, port: 82)
            let server = try await listener.accept()
            let writer = Task { try await client.write(payload) }
            for _ in 0..<200 {
                if await link.sawZeroWindow { break }
                try await Task.sleep(nanoseconds: 10_000_000)
            }
            let flag = CompletionFlag()
            let drainer = Task { try await client.drain(); await flag.finish() }
            try await Task.sleep(nanoseconds: 10_000_000)
            let completed = await flag.completed
            XCTAssertFalse(completed)
            var received = Data()
            while received.count < payload.count { received.append(try await server.read()) }
            XCTAssertEqual(received, payload)
            try await writer.value
            try await drainer.value
            try await client.write(Data([1, 2, 3]))
            let trailing = try await server.read()
            XCTAssertEqual(trailing, Data([1, 2, 3]))
            await client.close()
            let eof = try await server.read()
            XCTAssertTrue(eof.isEmpty)
        }
        await a.close(); await b.close()
    }

    func testCloseFlushesQueuedBytesAndDiscardsIncomingWithoutReset() async throws {
        let (a, b, link) = await pair(receiveCapacity: 65_535)
        let listener = try await b.listen(port: 83)
        let peer = addressB
        let payload = Data(repeating: 0x42, count: 180_000)
        try await deadline {
            let client = try await a.connect(peer: peer, port: 83)
            let server = try await listener.accept()
            let writer = Task { try await client.write(payload) }
            for _ in 0..<200 {
                if await link.sawZeroWindow { break }
                try await Task.sleep(nanoseconds: 10_000_000)
            }
            await client.close()
            // Closing must ACK and discard incoming data, so the peer can
            // finish even when its response exceeds the receive window.
            let response = Task {
                try await server.write(Data(repeating: 0x19, count: 200_000))
                try await server.closeWrite()
            }
            let received = try await self.drain(server)
            XCTAssertEqual(received, payload)
            try await writer.value
            try await response.value
            do { _ = try await client.read(); XCTFail("Expected local close") }
            catch { XCTAssertEqual(error as? TailcatTransportError, .closed) }
        }
        await a.close(); await b.close()
    }

    func testRefusedConnectionAndCancellationWakeWaiters() async throws {
        let (a, b, _) = await pair()
        do {
            _ = try await deadline { try await a.connect(peer: self.addressB, port: 9) }
            XCTFail("Expected refusal")
        } catch { XCTAssertEqual(error as? TailcatTransportError, .connectionRefused) }
        let listener = try await b.listen(port: 23)
        let waiter = Task { try await listener.accept() }
        waiter.cancel()
        do { _ = try await waiter.value; XCTFail("Expected cancellation") }
        catch { XCTAssertTrue(error is CancellationError) }
        let client = try await a.connect(peer: addressB, port: 23)
        let server = try await listener.accept()
        let read = Task { try await server.read() }
        read.cancel()
        do { _ = try await read.value; XCTFail("Expected cancellation") }
        catch { XCTAssertTrue(error is CancellationError) }
        await client.close()
        let eof = try await deadline { try await server.read() }
        XCTAssertTrue(eof.isEmpty)
        await a.close(); await b.close()
    }

    func testUDPBoundariesSourceIdentityAndEmptyDatagram() async throws {
        let (a, b, _) = await pair()
        let sender = try await a.udp()
        let receiver = try await b.udp(port: 53)
        for payload in [Data(), Data([1, 2, 3]), Data(repeating: 0x55, count: 1232)] {
            try await sender.send(payload, to: addressB, port: 53)
            let received = try await deadline { try await receiver.receive() }
            XCTAssertEqual(received.data, payload)
            XCTAssertEqual(received.peer, addressA)
            XCTAssertEqual(received.port, sender.port)
        }
        do {
            try await sender.send(Data(repeating: 0, count: 1233), to: addressB, port: 53)
            XCTFail("Expected MTU rejection")
        } catch { XCTAssertEqual(error as? TailcatTransportError, .datagramTooLarge) }
        let pending = Task { try await receiver.receive() }
        await receiver.close()
        do { _ = try await pending.value; XCTFail("Expected close") }
        catch { XCTAssertEqual(error as? TailcatTransportError, .closed) }
        await a.close(); await b.close()
    }

    func testExplicitSYNRejectionAndAcceptedStreamAbort() async throws {
        let (a, b, _) = await pair(.init(rejectSYN: true))
        let listener = try await b.listen(port: 80)
        do {
            _ = try await deadline { try await a.connect(peer: self.addressB, port: 80) }
            XCTFail("Expected explicit refusal despite bound listener")
        } catch { XCTAssertEqual(error as? TailcatTransportError, .connectionRefused) }
        let stillListening = await b.hasTCPListener(port: 80)
        XCTAssertTrue(stillListening)
        let client = try await a.connect(peer: addressB, port: 80)
        let server = try await listener.accept()
        let active = await b.hasTCPFlow(peer: addressA, localPort: 80, remotePort: client.localPort)
        XCTAssertTrue(active)
        await server.abort()
        let removed = await b.hasTCPFlow(peer: addressA, localPort: 80, remotePort: client.localPort)
        XCTAssertFalse(removed)
        do {
            _ = try await deadline { try await client.read() }
            XCTFail("Expected peer reset")
        } catch { XCTAssertEqual(error as? TailcatTransportError, .connectionReset) }
        await a.close(); await b.close()
    }

    func testExplicitRejectionIgnoresBadTCPChecksum() async throws {
        let (a, b, _) = await pair(.init(rejectCorruptSYN: true))
        let listener = try await b.listen(port: 80)
        let client = try await deadline { try await a.connect(peer: self.addressB, port: 80) }
        let server = try await listener.accept()
        try await client.write(Data([1, 2, 3]))
        let received = try await server.read()
        XCTAssertEqual(received, Data([1, 2, 3]))
        await a.close(); await b.close()
    }

    func testRejectsCorruptChecksumWrongDestinationAndTruncatedIPv6() async throws {
        let capture = PacketCapture()
        let a = IPStack(address: addressA) { await capture.record($0) }
        let b = IPStack(address: addressB) { _ in }
        let sender = try await a.udp()
        let receiver = try await b.udp(port: 53)
        try await sender.send(Data([1, 2, 3]), to: addressB, port: 53)
        let captured = await capture.packet
        let packet = try XCTUnwrap(captured)
        var corrupt = packet
        corrupt[48] ^= 0xff
        await b.receive(corrupt)
        var wrongDestination = packet
        wrongDestination[39] ^= 1
        await b.receive(wrongDestination)
        await b.receive(Data(packet.dropLast()))
        var missingChecksum = packet
        missingChecksum[46] = 0; missingChecksum[47] = 0
        await b.receive(missingChecksum)
        // If any invalid packet were accepted, it would precede this marker.
        try await sender.send(Data([4, 5, 6]), to: addressB, port: 53)
        let marker = await capture.packet
        await b.receive(try XCTUnwrap(marker))
        let result = try await deadline { try await receiver.receive() }
        XCTAssertEqual(result.data, Data([4, 5, 6]))
        await a.close(); await b.close()
    }

    /// Waits until a stack's timer is disarmed or only due at least `after` seconds out.
    private func waitForQuietTimer(_ stack: IPStack, after: TimeInterval = .infinity) async throws {
        for _ in 0..<300 {
            guard let wake = await stack.scheduledWakeup else { return }
            if wake - ProcessInfo.processInfo.systemUptime >= after { return }
            try await Task.sleep(nanoseconds: 10_000_000)
        }
        XCTFail("Timer stayed armed for a near deadline")
    }

    func testIdleStackDoesNotTickAndStillRetransmitsAtDeadline() async throws {
        let (a, b, link) = await pair()
        let listener = try await b.listen(port: 8080)
        let peer = addressB
        try await deadline {
            let client = try await a.connect(peer: peer, port: 8080)
            let server = try await listener.accept()
            try await client.write(Data("hello".utf8))
            let hello = try await server.read(maximumBytes: 5)
            XCTAssertEqual(hello, Data("hello".utf8))
            // Acknowledged handshake and data leave no deadline behind.
            try await self.waitForQuietTimer(a)
            try await self.waitForQuietTimer(b)
            let idleA = await a.wakeups, idleB = await b.wakeups
            try await Task.sleep(nanoseconds: 1_000_000_000)
            let laterA = await a.wakeups, laterB = await b.wakeups
            let armedA = await a.scheduledWakeup, armedB = await b.scheduledWakeup
            XCTAssertEqual(laterA, idleA)
            XCTAssertEqual(laterB, idleB)
            XCTAssertNil(armedA)
            XCTAssertNil(armedB)

            // A segment lost after the idle period still goes out again at its 500 ms deadline.
            await link.dropNextData()
            let payload = Data(repeating: 0x5e, count: 900)
            let started = ProcessInfo.processInfo.systemUptime
            let writer = Task { try await client.write(payload) }
            var received = Data()
            while received.count < payload.count { received.append(try await server.read()) }
            let elapsed = ProcessInfo.processInfo.systemUptime - started
            try await writer.value
            XCTAssertEqual(received, payload)
            let drops = await link.drops
            XCTAssertEqual(drops, 1)
            XCTAssertGreaterThanOrEqual(elapsed, 0.5)
            XCTAssertLessThan(elapsed, 1.5)
        }
        await a.close(); await b.close()
    }

    func testClosedFlowSleepsUntilTwoMinuteCleanup() async throws {
        let (a, b, _) = await pair()
        let listener = try await b.listen(port: 80)
        let client = try await a.connect(peer: addressB, port: 80)
        let server = try await listener.accept()
        let closedAt = ProcessInfo.processInfo.systemUptime
        await client.close()
        let eof = try await deadline { try await server.read() }
        XCTAssertTrue(eof.isEmpty)
        await server.close()
        // Once both FINs are acknowledged, only TIME-WAIT cleanup remains.
        try await waitForQuietTimer(a, after: 60)
        let wake = await a.scheduledWakeup
        let closing = await a.flowStatistics.closing
        XCTAssertEqual(closing, 1)
        let delay = try XCTUnwrap(wake) - closedAt
        XCTAssertGreaterThanOrEqual(delay, 120)
        XCTAssertLessThan(delay, 125)
        await a.close(); await b.close()
    }

    func testUDPIdleTimeoutAndPersistentListeningSocket() async throws {
        let (a, b, _) = await pair()
        let expiring = try await a.udp(idleTimeout: 0.02)
        let persistent = try await b.udp(port: 53, idleTimeout: nil)
        do {
            _ = try await deadline { try await expiring.receive() }
            XCTFail("Expected idle timeout")
        } catch { XCTAssertEqual(error as? TailcatTransportError, .timedOut) }
        let fresh = try await a.udp()
        try await fresh.send(Data([0x42]), to: addressB, port: persistent.port)
        let datagram = try await deadline { try await persistent.receive() }
        XCTAssertEqual(datagram.data, Data([0x42]))
        await a.close(); await b.close()
    }

    func testDisconnectClosesPeerStreamsAndClearsQueuedDatagrams() async throws {
        let (a, b, _) = await pair()
        let listener = try await b.listen(port: 80)
        let client = try await a.connect(peer: addressB, port: 80)
        let server = try await listener.accept()
        let sender = try await a.udp()
        let receiver = try await b.udp(port: 53, idleTimeout: nil)
        try await sender.send(Data([1]), to: addressB, port: 53)
        await b.disconnect(peer: addressA)
        do { _ = try await server.read(); XCTFail("Expected close") }
        catch { XCTAssertEqual(error as? TailcatTransportError, .closed) }
        do { _ = try await deadline { try await client.read() }; XCTFail("Expected reset") }
        catch { XCTAssertEqual(error as? TailcatTransportError, .connectionReset) }
        try await sender.send(Data([2]), to: addressB, port: 53)
        let result = try await deadline { try await receiver.receive() }
        XCTAssertEqual(result.data, Data([2]))
        await a.close(); await b.close()
    }
}