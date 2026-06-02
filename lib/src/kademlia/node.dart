import 'dart:async';
import 'dart:io';

import 'package:dartorrent_common/dartorrent_common.dart';

import 'id.dart';
import 'bucket.dart';

/// Kademlia node
///
class Node {
  bool _disposed = false;

  final ID id;

  final int k;

  final int _cleanupTime;

  bool queried = false;

  Timer? _timer;

  final Set<void Function(Node)> _cleanupHandler = <void Function(Node)>{};

  final Set<void Function(int index)> _bucketEmptyHandler =
      <void Function(int index)>{};

  final CompactAddress? _compactAddress;

  List<Bucket?>? _buckets;

  /// O(1) membership index of nodes added under this (root) node, keyed by the
  /// child node id's bytes-as-string ([ID.toString]). This mirrors what the
  /// bit-tree holds but avoids the up-to-160-frame recursive [Bucket.findNode]
  /// walk on the per-datagram membership check (see [indexOf]). The bit-tree is
  /// still the source of truth for closest-node queries.
  final Map<String, Node> _knownNodes = <String, Node>{};

  InternetAddress get address => _compactAddress!.address;

  int get port => _compactAddress!.port;

  List<Bucket?>? get buckets {
    _buckets ??= _getBuckets();
    return _buckets;
  }

  final Map<String, String> token = <String, String>{};

  final Map<String, bool> announced = <String, bool>{};

  Node(this.id, this._compactAddress,
      [this._cleanupTime = 15 * 60, this.k = 8]) {
    resetCleanupTimer();
  }

  bool onTimeToCleanup(void Function(Node node) h) {
    return _cleanupHandler.add(h);
  }

  bool offTimeToCleanup(void Function(Node node) h) {
    return _cleanupHandler.remove(h);
  }

  void resetCleanupTimer() {
    _timer?.cancel();
    if (_cleanupTime != -1) {
      _timer = Timer(Duration(seconds: _cleanupTime), _cleanupMe);
    }
  }

  void _cleanupMe() {
    for (var element in _cleanupHandler) {
      Timer.run(() => element(this));
    }
  }

  List<Bucket?>? _getBuckets() {
    _buckets ??= List<Bucket?>.filled(id.byteLength * 8,null);
    return _buckets;
  }

  bool add(Node? node) {
    if (node == null) return false;
    var index = _getBucketIndex(node.id);
    if (index < 0) return false;
    var buckets = _getBuckets();
    var bucket = buckets![index];
    bucket ??= Bucket(index, k);
    buckets[index] = bucket;
    bucket.onEmpty(_whenBucketIsEmpty);
    var added = bucket.addNode(node) != null;
    if (added) {
      // Keep the O(1) membership index in sync with the tree. Drop the entry
      // when the child node is cleaned up so [indexOf] never returns a stale
      // node the tree has already evicted.
      _knownNodes[node.id.toString()] = node;
      node.onTimeToCleanup(_removeFromIndex);
    }
    return added;
  }

  void _removeFromIndex(Node node) {
    node.offTimeToCleanup(_removeFromIndex);
    var key = node.id.toString();
    if (identical(_knownNodes[key], node)) {
      _knownNodes.remove(key);
    }
  }

  /// O(1) membership lookup for [id] among nodes added under this node.
  ///
  /// Equivalent to [findNode] for the "is this id known?" question, but without
  /// the recursive bit-tree walk. Returns `this` when [id] equals this node's
  /// own id (mirroring [findNode]'s `index == -1` case).
  Node? indexOf(ID id) {
    if (id == this.id) return this;
    return _knownNodes[id.toString()];
  }

  Node? findNode(ID id) {
    if (_buckets == null || _buckets!.isEmpty) return null;
    var index = _getBucketIndex(id);
    if (index == -1) return this;
    var buckets = _buckets;
    var bucket = buckets![index];
    var tn = bucket?.findNode(id);
    return tn?.node;
  }

  Bucket? getIDBelongBucket(ID id) {
    if (_buckets == null || _buckets!.isEmpty) return null;
    var index = _getBucketIndex(id);
    if (index == -1) return null;
    return _buckets![index];
  }

  List<Node>? findClosestNodes(ID id) {
    if (_buckets == null || _buckets!.isEmpty) return null;
    var index = _getBucketIndex(id);
    if (index == -1) return <Node>[this];
    var bucket = _buckets![index];
    var re = <Node>[];
    while (index < _buckets!.length) {
      if (_fillNodeList(bucket, re, k)) break;
      index++;
      if (index >= _buckets!.length) break;
      bucket = _buckets![index];
    }
    return re;
  }

  void _whenBucketIsEmpty(Bucket b) {
    for (var element in _bucketEmptyHandler) {
      Timer.run(() => element(b.index));
    }
  }

  bool onBucketEmpty(void Function(int index) h) {
    return _bucketEmptyHandler.add(h);
  }

  bool offBucketEmpty(void Function(int index) h) {
    return _bucketEmptyHandler.remove(h);
  }

  bool _fillNodeList(Bucket? bucket, List<Node> target, int max) {
    if (bucket == null) return target.length >= max;
    for (var i = 0; i < bucket.nodes.length; i++) {
      if (target.length >= max) break;
      target.add(bucket.nodes[i]);
    }
    return target.length >= max;
  }

  int _getBucketIndex(ID id1) {
    return id.differentLength(id1) - 1;
  }

  void remove(Node node) {
    if (_buckets == null || _buckets!.isEmpty) return;
    var index = _getBucketIndex(node.id);
    var bucket = _buckets![index];
    bucket?.removeNode(node);
    _removeFromIndex(node);
  }

  void forEach(void Function(Node node)? processor) {
    var buckets = this.buckets;
    for (var i = 0; i < buckets!.length; i++) {
      var b = buckets[i];
      if (b == null) continue;
      var l = b.nodes.length;
      for (var i = 0; i < l; i++) {
        var node = b.nodes[i];
        if (processor != null) {
          processor(node);
        }
      }
    }
  }

  String? toContactEncodingString() {
    if (_compactAddress == null) return null;
    return '$id${_compactAddress!.toContactEncodingString()}';
  }

  @override
  String toString() {
    return 'Node[id:$id,Peer:${_compactAddress?.toString()}]';
  }

  bool get isDisposed => _disposed;

  void dispose() {
    if (isDisposed) return;
    _disposed = true;
    _timer?.cancel();
    _timer = null;

    _cleanupHandler.clear();
    token.clear();
    announced.clear();
    _knownNodes.clear();

    if (_buckets != null) {
      for (var i = 0; i < _buckets!.length; i++) {
        var b = _buckets![i];
        b?.offEmpty(_whenBucketIsEmpty);
        b?.dispose();
      }
    }
    _buckets = null;
  }
}
