// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "tailcat-swift",
    platforms: [.iOS(.v15), .macOS(.v12)],
    products: [
        .library(name: "Tailcat", targets: ["Tailcat"]),
        .library(name: "TailcatRequests", targets: ["TailcatRequests"])
    ],
    targets: [
        .target(name: "Tailcat"),
        .target(name: "TailcatRequests", dependencies: ["Tailcat"]),
        .testTarget(name: "TailcatRequestsTests", dependencies: ["TailcatRequests", "Tailcat"]),
        .testTarget(name: "TailcatTests", dependencies: ["Tailcat"])
    ]
)
