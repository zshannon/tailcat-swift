import XCTest
@testable import Tailcat
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif
#if canImport(Glibc)
import Glibc
#endif

/// Serves maps for `tailcat-test://` URLs from memory and counts the requests.
private final class MapServer: URLProtocol, @unchecked Sendable {
    struct Request { let url: URL; let etag: String? }
    private static let lock = NSLock()
    nonisolated(unsafe) private static var maps: [URL: Data] = [:]
    nonisolated(unsafe) private static var log: [Request] = []
    static func serve(_ url: URL, _ data: Data?) { lock.lock(); maps[url] = data; lock.unlock() }
    static func requests(_ url: URL) -> [Request] { lock.lock(); defer { lock.unlock() }; return log.filter { $0.url == url } }

    override class func canInit(with request: URLRequest) -> Bool { request.url?.scheme == "tailcat-test" }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
    override func startLoading() {
        guard let url = request.url else { return }
        let etag = request.value(forHTTPHeaderField: "If-None-Match")
        Self.lock.lock(); Self.log.append(Request(url: url, etag: etag)); let data = Self.maps[url]; Self.lock.unlock()
        guard let data else { client?.urlProtocol(self, didFailWithError: URLError(.notConnectedToInternet)); return }
        let tag = "\"\(data.hashValue)\""
        let status = etag == tag ? 304 : 200
        let response = HTTPURLResponse(url: url, statusCode: status, httpVersion: "HTTP/1.1", headerFields: ["ETag": tag])!
        client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
        if status == 200 { client?.urlProtocol(self, didLoad: data) }
        client?.urlProtocolDidFinishLoading(self)
    }
    override func stopLoading() {}
}

final class DERPMapTests: XCTestCase {
    private let json = Data("""
    {"Regions":{"7":{"RegionID":7,"RegionCode":"test","Nodes":[
      {"HostName":"one.example"},{"HostName":"two.example"},{"HostName":"three.example"}
    ]}}}
    """.utf8)

    override class func setUp() { super.setUp(); URLProtocol.registerClass(MapServer.self) }

    private func testURL() throws -> URL { try XCTUnwrap(URL(string: "tailcat-test://map/\(UUID().uuidString)")) }
    private func map(_ regions: [DERPRegion]) throws -> Data {
        let body = try regions.map { "\"\($0.regionID)\":" + String(decoding: try JSONEncoder().encode($0), as: UTF8.self) }
        return Data("{\"Regions\":{\(body.joined(separator: ","))}}".utf8)
    }
    private func eventually(_ condition: () async -> Bool) async throws {
        for _ in 0..<200 { if await condition() { return }; try await Task.sleep(nanoseconds: 10_000_000) }
        XCTFail("condition not reached")
    }

    func testFreshCacheAvoidsNetworkAndResolvesCompactAddress() async throws {
        let url = try XCTUnwrap(URL(string: "unsupported://tailcat-map/fresh"))
        let cache = MemoryDERPMapCache()
        await cache.store(url: url, entry: DERPMapCacheEntry(data: json, etag: "opaque"))
        let key = try NodePublicKey(rawValue: Data(repeating: 1, count: 32))
        let address = try TailcatAddress(serverPublicKey: key, regionID: 7)
        let resolved = try await address.resolved(mapURL: url, cache: cache)
        XCTAssertEqual(resolved.regionID, 0)
        XCTAssertEqual(resolved.regions[0].nodes.map(\.hostName), ["one.example", "two.example"])
        let offline = try await resolved.expanded(mapURL: url, cache: MemoryDERPMapCache())
        XCTAssertEqual(offline, resolved)
    }

    func testStaleCacheSurvivesFetchFailureButMalformedCacheDoesNot() async throws {
        let url = try XCTUnwrap(URL(string: "unsupported://tailcat-map/stale"))
        let cache = MemoryDERPMapCache()
        await cache.store(url: url, entry: DERPMapCacheEntry(data: json, storedAt: Date().addingTimeInterval(-7200)))
        let map = try await DERPMap.fetch(url: url, cache: cache)
        XCTAssertEqual(map.regions[7]?.nodes.count, 3)
        await cache.store(url: url, entry: DERPMapCacheEntry(data: Data("invalid".utf8)))
        do { _ = try await DERPMap.fetch(url: url, cache: cache); XCTFail("malformed cache accepted") } catch {}
    }

    func testFileCacheKeepsMapsAcrossInstances() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        let url = try testURL(), other = try testURL()
        let first = FileDERPMapCache(directory: directory)
        let missing = await first.load(url: url)
        XCTAssertNil(missing)
        let storedAt = Date(timeIntervalSince1970: 1_700_000_000)
        try await first.store(url: url, entry: DERPMapCacheEntry(data: json, etag: "tag", storedAt: storedAt))
        let loaded = await FileDERPMapCache(directory: directory).load(url: url)
        XCTAssertEqual(loaded?.data, json); XCTAssertEqual(loaded?.etag, "tag"); XCTAssertEqual(loaded?.storedAt, storedAt)
        let unrelated = await FileDERPMapCache(directory: directory).load(url: other)
        XCTAssertNil(unrelated)
    }

    func testStaleMapIsUsedAtOnceAndRevalidatedInBackground() async throws {
        let url = try testURL()
        let current = try map([DERPRegion(regionID: 7, nodes: [DERPNode(hostName: "new.example")])])
        MapServer.serve(url, current)
        let cache = MemoryDERPMapCache()
        await cache.store(url: url, entry: DERPMapCacheEntry(data: json, etag: "old", storedAt: Date().addingTimeInterval(-7200)))
        let stale = try await DERPMap.fetch(url: url, cache: cache)
        XCTAssertEqual(stale.regions[7]?.nodes.first?.hostName, "one.example")
        try await eventually { await cache.load(url: url)?.data == current }
        XCTAssertEqual(MapServer.requests(url).map(\.etag), ["old"])
        let fresh = try await DERPMap.fetch(url: url, cache: cache)
        XCTAssertEqual(fresh.regions[7]?.nodes.first?.hostName, "new.example")
        XCTAssertEqual(MapServer.requests(url).count, 1, "a fresh map needs no request")
    }

    func testRefreshesBackOffWhileTheyFail() async throws {
        let url = try testURL()
        let cache = MemoryDERPMapCache()
        await cache.store(url: url, entry: DERPMapCacheEntry(data: json, storedAt: Date().addingTimeInterval(-7200)))
        for _ in 0..<5 { _ = try await DERPMap.fetch(url: url, cache: cache); try await Task.sleep(nanoseconds: 20_000_000) }
        let refreshed = await DERPMap.refresh(url: url, cache: cache, forServer: false)
        XCTAssertNil(refreshed)
        XCTAssertEqual(MapServer.requests(url).count, 1)
    }

    func testRegionMissingFromSavedMapIsFetched() async throws {
        let url = try testURL()
        MapServer.serve(url, try map([DERPRegion(regionID: 9, nodes: [DERPNode(hostName: "nine.example")])]))
        let cache = MemoryDERPMapCache()
        await cache.store(url: url, entry: DERPMapCacheEntry(data: json))
        let key = try NodePublicKey(rawValue: Data(repeating: 1, count: 32))
        let expanded = try await TailcatAddress(serverPublicKey: key, regionID: 9).expanded(mapURL: url, cache: cache)
        XCTAssertEqual(expanded.regions[0].nodes.map(\.hostName), ["nine.example"])
        XCTAssertEqual(MapServer.requests(url).count, 1)
    }

    #if canImport(Glibc)
    private struct Fixture {
        let relay: LocalRelay
        let server: TailcatServer
        let address: TailcatAddress
    }
    private func fixture() async throws -> Fixture {
        let relay = try LocalRelay()
        var configuration = TailcatServer.Configuration()
        configuration.region = DERPRegion(regionID: 1, nodes: [relay.node])
        let server = TailcatServer(configuration: configuration)
        try await server.start()
        var address = try await server.address
        address.regions = []; address.regionID = 1
        return Fixture(relay: relay, server: server, address: address)
    }
    private func connect(_ fixture: Fixture, url: URL, savedNode: DERPNode, trace: StartupTrace? = nil) async throws -> TailcatClient {
        let cache = MemoryDERPMapCache()
        await cache.store(url: url, entry: DERPMapCacheEntry(data: try map([DERPRegion(regionID: 1, nodes: [savedNode])])))
        var configuration = TailcatClient.Configuration()
        configuration.mapURL = url; configuration.mapCache = cache; configuration.connectTimeout = 10
        configuration.startupTrace = trace
        return try await TailcatClient.connect(to: fixture.address, configuration: configuration)
    }
    private func unusedPort() throws -> Int {
        let relay = try LocalRelay(); defer { relay.close() }
        return relay.port
    }

    func testOfflineWithSavedMapConnects() async throws {
        let fixture = try await fixture()
        defer { fixture.relay.close() }
        let url = try testURL()
        let client = try await connect(fixture, url: url, savedNode: fixture.relay.node)
        await client.close(); await fixture.server.close()
        XCTAssertTrue(MapServer.requests(url).isEmpty)
    }

    func testRefusedRelayFromSavedMapRetriesWithCurrentMap() async throws {
        let fixture = try await fixture()
        defer { fixture.relay.close() }
        let url = try testURL()
        MapServer.serve(url, try map([DERPRegion(regionID: 1, nodes: [fixture.relay.node])]))
        var wrong = fixture.relay.node; wrong.derpPort = try unusedPort()
        let trace = StartupTrace(), started = Date()
        let client = try await connect(fixture, url: url, savedNode: wrong, trace: trace)
        XCTAssertLessThan(Date().timeIntervalSince(started), 3, "a refused relay is retried without waiting")
        XCTAssertTrue(trace.snapshot().contains { $0.name == "relayMapRefreshed" })
        await client.close(); await fixture.server.close()
    }

    func testSilentRelayFromSavedMapRetriesWithCurrentMap() async throws {
        let fixture = try await fixture()
        defer { fixture.relay.close() }
        // Listens and never accepts or answers, like a stale address that blackholes the relay handshake.
        let silent = socket(AF_INET, Int32(SOCK_STREAM.rawValue), 0)
        defer { Glibc.close(silent) }
        var address = sockaddr_in(); address.sin_family = sa_family_t(AF_INET); address.sin_addr.s_addr = UInt32(0x7f000001).bigEndian
        var length = socklen_t(MemoryLayout<sockaddr_in>.size)
        let bound = withUnsafeMutablePointer(to: &address) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { bind(silent, $0, length) == 0 && getsockname(silent, $0, &length) == 0 }
        }
        XCTAssertTrue(bound); XCTAssertEqual(listen(silent, 8), 0)
        var node = fixture.relay.node; node.derpPort = Int(UInt16(bigEndian: address.sin_port))
        let url = try testURL()
        MapServer.serve(url, try map([DERPRegion(regionID: 1, nodes: [fixture.relay.node])]))
        let started = Date()
        let client = try await connect(fixture, url: url, savedNode: node)
        XCTAssertLessThan(Date().timeIntervalSince(started), 6, "not the 15 s relay timeout")
        await client.close(); await fixture.server.close()
    }

    func testUnchangedMapKeepsTheFailedError() async throws {
        let fixture = try await fixture()
        defer { fixture.relay.close() }
        let url = try testURL()
        var wrong = fixture.relay.node; wrong.derpPort = try unusedPort()
        MapServer.serve(url, try map([DERPRegion(regionID: 1, nodes: [wrong])]))
        do { _ = try await connect(fixture, url: url, savedNode: wrong); XCTFail("refused relay connected") } catch {}
        XCTAssertEqual(MapServer.requests(url).count, 1)
        await fixture.server.close()
    }
    #endif
}
