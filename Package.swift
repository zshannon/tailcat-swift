// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "tailcat-swift",
    platforms: [.iOS(.v15), .macOS(.v12)],
    products: [
        .library(name: "Tailcat", targets: ["Tailcat"]),
        .library(name: "TailcatRequests", targets: ["TailcatRequests"])
    ],
    dependencies: [
        .package(url: "https://github.com/pointfreeco/swift-clocks", from: "1.1.0")
    ],
    targets: [
        .target(name: "Tailcat"),
        .target(name: "TailcatRequests", dependencies: ["Tailcat"]),
        .testTarget(name: "TailcatRequestsTests", dependencies: [
            "TailcatRequests", "Tailcat", .product(name: "Clocks", package: "swift-clocks")
        ]),
        .testTarget(name: "TailcatTests", dependencies: ["Tailcat"])
    ]
)
