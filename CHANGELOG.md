## 0.1.0
- Modernize to Dart 3 (`sdk: '>=3.0.0 <4.0.0'`).
- Switch lints to `package:lints/recommended.yaml` (drop deprecated `pedantic`);
  `dart analyze --fatal-infos` is clean.
- Idiomatize the codebase: lowerCamelCase constants/enum members, `for` loops
  instead of `forEach` literals, explicit types on previously-`var` locals, and
  removal of dead null checks / unnecessary `!` left over from null-safety
  migration.
- Fix `Node.offBucketEmpty` (it added the handler instead of removing it).
- Harden `info_hash` handling in `announce_peer` / `get_peers` requests: cast as
  nullable so a missing/invalid info_hash is rejected gracefully instead of
  throwing on a non-null cast.
- Replace the example's `torrent_model` dependency with an inline infohash so the
  package builds without extra sibling deps.
- Rewrite the test suite around `package:test` matchers (XOR distance, k-bucket
  add/find/remove, compact node/peer info, KRPC ping/find_node/get_peers/
  announce_peer round-trips); tests close all resources and run no network I/O.
- Add a GitHub Actions CI workflow (clone sibling path-deps, `pub get`, analyze,
  test).

## 0.0.5
- Initial version

## 0.0.6
- Add doc
- Change example

## 0.0.7
- fix a bug