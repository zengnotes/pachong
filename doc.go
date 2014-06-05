//           +~~~~~~~~+
//           | Domain |
//           +~~~~~~~~+
//               ^|
//               ||
//               |v
//         +------------+
//         | Downloader |
//         +------------+
//            ^     |
//            |     |    +---------+
//            |     |--->| Storage |
//            |     |    +---------+
//            |     v
// +-----------+   +----------------+
// | Scheduler |   | Link Extractor |
// +-----------+   +----------------+
//            ^     |
//            |     |
//            |     v
//         +-----------+
//         | URL Queue |
//         +-----------+
package main
