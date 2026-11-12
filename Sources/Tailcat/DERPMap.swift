// Copyright (c) Tailscale Inc & contributors
// SPDX-License-Identifier: BSD-3-Clause
import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

public struct DERPMapCacheEntry: Sendable, Codable {
    public let data: Data
    public let etag: String
    public let storedAt: Date
    public init(data: Data, etag: String = "", storedAt: Date = Date()) { self.data = data; self.etag = etag; self.storedAt = storedAt }
}

public protocol DERPMapCache: Sendable {
    func load(url: URL) async -> DERPMapCacheEntry?
    func store(url: URL, entry: DERPMapCacheEntry) async throws
}

public actor MemoryDERPMapCache: DERPMapCache {
    public static let shared = MemoryDERPMapCache()
    private var entries: [URL: DERPMapCacheEntry] = [:]
    public init() {}
    public func load(url: URL) -> DERPMapCacheEntry? { entries[url] }
    public func store(url: URL, entry: DERPMapCacheEntry) { entries[url] = entry }
}

/// Keeps maps across launches, one file per map URL in `directory`, which is created on the first store.
public struct FileDERPMapCache: DERPMapCache {
    public let directory: URL
    public init(directory: URL) { self.directory = directory }
    public func load(url: URL) async -> DERPMapCacheEntry? {
        guard let data = try? Data(contentsOf: file(for: url)) else { return nil }
        return try? JSONDecoder().decode(DERPMapCacheEntry.self, from: data)
    }
    public func store(url: URL, entry: DERPMapCacheEntry) async throws {
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        try JSONEncoder().encode(entry).write(to: file(for: url), options: .atomic)
    }
    private func file(for url: URL) -> URL {
        // FNV-1a: unlike hashValue, the same name in every launch.
        let hash = url.absoluteString.utf8.reduce(UInt64(14_695_981_039_346_656_037)) { ($0 ^ UInt64($1)) &* 1_099_511_628_211 }
        return directory.appendingPathComponent("derpmap-\(String(hash, radix: 16)).json")
    }
}

/// Refreshes run while a saved map is in use, so callers share one per URL and
/// they back off: 30 s after one that changed the map, doubling up to an hour
/// while they fail or change nothing. A relay that keeps failing can't make a fetch storm.
private actor DERPMapRefreshes {
    static let shared = DERPMapRefreshes()
    private var running: [URL: Task<(map: DERPMap, changed: Bool)?, Never>] = [:]
    private var backoff: [URL: (until: TimeInterval, interval: TimeInterval)] = [:]
    func run(_ url: URL, _ fetch: @escaping @Sendable () async throws -> (map: DERPMap, changed: Bool)) async -> DERPMap? {
        if let task = running[url] { return await task.value?.map }
        let previous = backoff[url] ?? (0, 15)
        guard ProcessInfo.processInfo.systemUptime >= previous.until else { return nil }
        let task = Task { try? await fetch() }
        running[url] = task
        let result = await task.value
        running[url] = nil
        let interval = result?.changed == true ? 30 : min(previous.interval * 2, 3600)
        backoff[url] = (ProcessInfo.processInfo.systemUptime + interval, interval)
        return result?.map
    }
}

public struct DERPMap: Sendable, Decodable {
    public static let defaultURL = URL(string: "https://tailcat.dev/derpmap.json")!
    public var regions: [Int: DERPRegion]
    public init(regions: [Int: DERPRegion]) { self.regions = regions }
    private enum CodingKeys: String, CodingKey { case regions = "Regions" }
    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        let map = try c.decode([String: DERPRegion].self, forKey: .regions)
        regions = [:]
        for (key, region) in map {
            guard let id = Int(key), id > 0, id == region.regionID else { throw TailcatError.invalidAddress("invalid DERP map region ID") }
            regions[id] = region
        }
    }
    /// A saved map is used at once, whatever its age; one older than an hour is
    /// ETag-revalidated in the background. Only without one does this wait for the network.
    public static func fetch(url: URL = defaultURL, cache: any DERPMapCache = MemoryDERPMapCache.shared, forServer: Bool = false) async throws -> DERPMap {
        try await load(url: url, cache: cache, forServer: forServer).map
    }

    /// Also says whether the map was saved, so may be out of date.
    static func load(url: URL, cache: any DERPMapCache, forServer: Bool) async throws -> (map: DERPMap, saved: Bool) {
        if let entry = await cache.load(url: url), let map = try? JSONDecoder().decode(DERPMap.self, from: entry.data) {
            if abs(Date().timeIntervalSince(entry.storedAt)) >= 3600 {
                Task.detached { _ = await refresh(url: url, cache: cache, forServer: forServer) }
            }
            return (map, true)
        }
        return (try await download(url: url, cache: cache, forServer: forServer).map, false)
    }

    /// Revalidate now, ignoring the age of the saved map. Nil while refreshes back off, or if this one failed.
    static func refresh(url: URL, cache: any DERPMapCache, forServer: Bool) async -> DERPMap? {
        await DERPMapRefreshes.shared.run(url) { try await download(url: url, cache: cache, forServer: forServer) }
    }

    private static func download(url: URL, cache: any DERPMapCache, forServer: Bool) async throws -> (map: DERPMap, changed: Bool) {
        let cached = await cache.load(url: url)
        let cachedMap = cached.flatMap { try? JSONDecoder().decode(DERPMap.self, from: $0.data) }
        var request = URLRequest(url: url); request.timeoutInterval = 10
        request.setValue(forServer ? "server" : "client", forHTTPHeaderField: "Tailcat-Mode")
        if cachedMap != nil, let etag = cached?.etag, !etag.isEmpty { request.setValue(etag, forHTTPHeaderField: "If-None-Match") }
        let (data, response) = try await URLSession.shared.data(for: request)
        try Task.checkCancellation()
        guard let http = response as? HTTPURLResponse else { throw TailcatError.noRelay }
        if http.statusCode == 304, let entry = cached, let map = cachedMap {
            try? await cache.store(url: url, entry: DERPMapCacheEntry(data: entry.data, etag: entry.etag))
            return (map, false)
        }
        guard http.statusCode == 200, data.count <= 4_194_304 else { throw TailcatError.noRelay }
        let map = try JSONDecoder().decode(DERPMap.self, from: data)
        guard !map.regions.isEmpty else { throw TailcatError.noRelay }
        try? await cache.store(url: url, entry: DERPMapCacheEntry(data: data, etag: http.value(forHTTPHeaderField: "ETag") ?? ""))
        return (map, data != cached?.data)
    }

    /// Select the first region to answer an HTTPS probe, bounding concurrent requests.
    public func nearestRegion() async throws -> DERPRegion {
        let candidates = regions.values.filter { $0.nodes.contains { !$0.stunOnly } }.sorted { $0.regionID < $1.regionID }
        guard !candidates.isEmpty else { throw TailcatError.noRelay }
        // Probe every region, with a short timeout. This deliberately avoids a control-plane dependency.
        return try await withThrowingTaskGroup(of: DERPRegion?.self) { group in
            for region in candidates {
                group.addTask {
                    guard let node = region.nodes.first(where: { !$0.stunOnly && !$0.hostName.isEmpty }),
                          var components = URLComponents(string: "https://\(node.hostName)/derp/latency-check") else { return nil }
                    if node.derpPort > 0 { components.port = node.derpPort }
                    guard let url = components.url else { return nil }
                    var request = URLRequest(url: url); request.timeoutInterval = 3; request.httpMethod = "HEAD"
                    do {
                        let (_, response) = try await URLSession.shared.data(for: request)
                        guard let http = response as? HTTPURLResponse, (200..<500).contains(http.statusCode) else { return nil }
                        return region
                    } catch { return nil }
                }
            }
            while let result = try await group.next() {
                if let region = result { group.cancelAll(); return region }
            }
            try Task.checkCancellation()
            return candidates[Int.random(in: candidates.indices)]
        }
    }
}

extension TailcatAddress {
    public func expanded(mapURL: URL = DERPMap.defaultURL, cache: any DERPMapCache = MemoryDERPMapCache.shared, forServer: Bool = false) async throws -> Self {
        try await expanding(mapURL: mapURL, cache: cache, forServer: forServer).address
    }
    /// Also says whether the relay came from a saved map, so may be out of date.
    func expanding(mapURL: URL, cache: any DERPMapCache, forServer: Bool) async throws -> (address: Self, saved: Bool) {
        if !regions.isEmpty {
            guard regions.count == 1, !regions[0].nodes.filter({ !$0.stunOnly }).isEmpty else { throw TailcatError.invalidAddress("exactly one usable region is required") }
            return (self, false)
        }
        guard regionID != 0 else { throw TailcatError.invalidAddress("missing region") }
        var (map, saved) = try await DERPMap.load(url: mapURL, cache: cache, forServer: forServer)
        // A region newer than the saved map.
        if saved, regionID > 0, map.regions[regionID] == nil,
           let current = await DERPMap.refresh(url: mapURL, cache: cache, forServer: forServer) { map = current; saved = false }
        let region: DERPRegion
        if regionID == -1 { region = try await map.nearestRegion() }
        else if let found = map.regions[regionID] { region = found }
        else { throw TailcatError.noRelay }
        var result = self; result.regions = [region]; return (result, saved)
    }
    /// Pin relay details so future connections require no DERP map fetch.
    public func resolved(mapURL: URL = DERPMap.defaultURL, cache: any DERPMapCache = MemoryDERPMapCache.shared) async throws -> Self {
        if !regions.isEmpty { return self }
        var result = try await expanded(mapURL: mapURL, cache: cache)
        result.regions[0].nodes = Array(result.regions[0].nodes.filter { !$0.stunOnly }.prefix(2)); result.regionID = 0
        return result
    }
}