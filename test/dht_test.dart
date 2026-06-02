import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:bencode_dart/bencode_dart.dart';
import 'package:dartorrent_common/dartorrent_common.dart';
import 'package:dht_dart/src/kademlia/bucket.dart';
import 'package:dht_dart/src/kademlia/distance.dart';
import 'package:dht_dart/src/kademlia/id.dart';
import 'package:dht_dart/src/kademlia/node.dart';
import 'package:dht_dart/src/kademlia/tree_node.dart';
import 'package:dht_dart/src/krpc/krpc_message.dart';
import 'package:test/test.dart';

const _idLen = 20;

/// Build a [Node] whose internal cleanup [Timer] is disabled (-1), so tests
/// don't leave pending timers / hanging sockets behind.
Node _node(ID id, [CompactAddress? addr]) => Node(id, addr, -1);

/// A random ASCII string of [len] chars. The KRPC layer ships ids/tids/tokens
/// as Dart strings through bencode, and bencode_dart now (correctly) UTF-8
/// encodes strings — so for a lossless round-trip in assertions we keep the
/// fixtures within the ASCII range (where UTF-8 bytes == code units).
String _ascii(int len) {
  final r = Random();
  return String.fromCharCodes(List<int>.generate(len, (_) => 33 + r.nextInt(94)));
}

void main() {
  group('ID', () {
    test('randomID has the requested byte length', () {
      expect(ID.randomID(_idLen).byteLength, _idLen);
      expect(ID.randomID(2).byteLength, 2);
    });

    test('differentLength is 0 for equal ids and full for opposite ids', () {
      final id = ID.randomID(_idLen);
      expect(id.differentLength(id), 0);

      // Bit-flip every byte -> the very first bit differs -> full length.
      final flipped = List<int>.generate(_idLen, (i) => id.getValueAt(i) ^ 0xFF);
      expect(id.differentLength(ID.createID(flipped)), _idLen * 8);
    });

    test('differentLength matches the shared-prefix-bit construction', () {
      // For every possible shared-prefix length, build an id that shares
      // exactly that many leading bits with a random id and assert the
      // reported "different length" (suffix length) is what we expect.
      void check(int sharedPrefixBits) {
        final expectedDiff = (_idLen * 8) - sharedPrefixBits;
        final id = ID.randomID(_idLen);
        final newId = List<int>.filled(_idLen, 0);
        final wholeSameBytes = sharedPrefixBits ~/ 8;
        final bitsIntoNextByte = sharedPrefixBits.remainder(8);

        var j = 0;
        for (; j < wholeSameBytes; j++) {
          newId[j] = id.getValueAt(j);
        }
        if (j >= id.byteLength) return;

        final src = id.getValueAt(j);
        var b = 0;
        // Copy the matching high bits.
        for (var i = 0; i < bitsIntoNextByte; i++) {
          b |= (128 >> i) & src;
        }
        // Flip the first differing bit (and any following ones that were 0).
        for (var i = bitsIntoNextByte; i < 8; i++) {
          final base = 128 >> i;
          if (src & base == 0) b |= base;
        }
        newId[j] = b;
        final r = Random();
        for (var i = j + 1; i < _idLen; i++) {
          newId[i] = r.nextInt(256);
        }
        expect(ID.createID(newId).differentLength(id), expectedDiff);
      }

      for (var sharedPrefixBits = 0;
          sharedPrefixBits < _idLen * 8;
          sharedPrefixBits++) {
        check(sharedPrefixBits);
      }
    });

    test('equality and XOR distance', () {
      final a = ID.createID(List<int>.filled(_idLen, 0));
      final b = ID.createID(List<int>.filled(_idLen, 0));
      expect(a == b, isTrue);
      expect(a.hashCode, b.hashCode);

      // Distance(a, a) is all-zero, Distance(a, b) is the XOR.
      final selfDistance = a.distanceBetween(a);
      for (var i = 0; i < selfDistance.byteLength; i++) {
        expect(selfDistance.getValue(i), 0);
      }

      final x = ID.createID(List<int>.generate(_idLen, (i) => i));
      final y = ID.createID(List<int>.generate(_idLen, (i) => 0xFF - i));
      final d = x.distanceBetween(y);
      for (var i = 0; i < _idLen; i++) {
        expect(d.getValue(i), i ^ (0xFF - i));
      }
    });

    test('Distance equality compares value-wise', () {
      final d1 = Distance([1, 2, 3]);
      final d2 = Distance([1, 2, 3]);
      final d3 = Distance([1, 2, 4]);
      expect(d1 == d2, isTrue);
      expect(d1 == d3, isFalse);
    });
  });

  group('CompactAddress (compact node/peer info)', () {
    test('IPv4 round-trip through bytes', () {
      final addr = CompactAddress(InternetAddress.tryParse('128.2.1.3')!, 12311);
      final parsed = CompactAddress.parseIPv4Address(addr.toBytes())!;
      expect(parsed.port, addr.port);
      expect(parsed.addressString, addr.addressString);
    });

    test('contact-encoding string is 6 bytes and round-trips', () {
      final addr = CompactAddress(InternetAddress.tryParse('128.2.1.3')!, 12311);
      final str = addr.toContactEncodingString()!;
      final bytes = latin1.encode(str);
      expect(bytes.length, 6);

      final parsed = CompactAddress.parseIPv4Address(bytes)!;
      expect(parsed.port, addr.port);
      expect(parsed.addressString, addr.addressString);
      expect(parsed.toContactEncodingString(), str);
    });

    test('parses many concatenated 6-byte addresses by offset', () {
      final r = Random();
      const n = 10;
      final bytes = <int>[];
      for (var i = 0; i < n * 6; i++) {
        bytes.add(r.nextInt(256));
      }
      for (var i = 0, offset = 0; i < n; i++, offset += 6) {
        final parsed = CompactAddress.parseIPv4Address(bytes, offset)!;
        final out = parsed.toBytes();
        for (var h = 0; h < 6; h++) {
          expect(out[h], bytes[offset + h]);
        }
      }
    });
  });

  group('Bucket (k-bucket binary tree)', () {
    late Bucket bucket;

    tearDown(() => bucket.dispose());

    test('bucketMaxSize grows then caps at the configured k', () {
      for (var i = 0; i < 160; i++) {
        final b = Bucket(i, 8);
        final expected = i > 62 ? 8 : min(8, pow(2, i).toInt());
        expect(b.bucketMaxSize, expected);
        b.dispose();
      }
      bucket = Bucket(0); // satisfy tearDown
    });

    test('add ignores null and tracks count', () {
      bucket = Bucket(5);
      expect(bucket.addNode(null), isNull);
      expect(bucket.isEmpty, isTrue);
      expect(bucket.count, 0);

      final n1 = _node(ID.randomID(_idLen));
      final n2 = _node(ID.randomID(_idLen));
      expect(bucket.addNode(n1), isNotNull);
      expect(bucket.addNode(n2), isNotNull);
      expect(bucket.addNode(null), isNull);
      expect(bucket.isNotEmpty, isTrue);
      expect(bucket.count, 2);
    });

    test('a node lands at the tree path matching its id bits', () {
      bucket = Bucket(5);
      final node = _node(ID.randomID(_idLen));
      final tn = bucket.addNode(node)!;

      // Walk up from the leaf collecting left(=1)/right(=0) edges.
      List<bool> pathToBits(TreeNode leaf) {
        final bits = <bool>[];
        var cur = leaf;
        while (cur.parent != null) {
          final parent = cur.parent!;
          bits.insert(0, parent.left == cur); // left => 1, right => 0
          cur = parent;
        }
        return bits;
      }

      final bits = pathToBits(tn);
      expect(bits.length, _idLen * 8);

      // Reconstruct the id bytes from the collected bits (little-endian byte
      // order, matching the tree construction in Bucket).
      final rebuilt = <int>[];
      for (var i = 0; i < _idLen; i++) {
        var v = 0;
        for (var j = 0; j < 8; j++) {
          v = (v << 1) | (bits[i * 8 + j] ? 1 : 0);
        }
        rebuilt.insert(0, v);
      }
      for (var i = 0; i < rebuilt.length; i++) {
        expect(rebuilt[i], node.id.getValueAt(i));
      }
    });

    test('findNode locates the exact node only', () {
      bucket = Bucket(5);
      final id1 = ID.randomID(_idLen);
      final id2 = ID.randomID(_idLen);
      final n1 = _node(id1);
      final n2 = _node(id2);
      bucket.addNode(n1);
      bucket.addNode(n2);

      expect(bucket.findNode(ID.randomID(_idLen)), isNull);
      expect(bucket.findNode(id1)!.node, n1);
      expect(bucket.findNode(id2)!.node, n2);
      expect(bucket.findNode(id2)!.node, isNot(n1));
    });

    test('removeNode by Node / TreeNode / ID and empties the bucket', () {
      bucket = Bucket(5);
      final id1 = ID.randomID(_idLen);
      final id2 = ID.randomID(_idLen);
      final id3 = ID.randomID(_idLen);

      expect(bucket.removeNode(id1), isNull);
      expect(bucket.isEmpty, isTrue);

      final n1 = _node(id1);
      final n2 = _node(id2);
      final n3 = _node(id3);
      final tn1 = bucket.addNode(n1);
      final tn2 = bucket.addNode(n2);
      final tn3 = bucket.addNode(n3);

      expect(bucket.removeNode(n1), tn1); // by Node
      expect(bucket.count, 2);
      expect(bucket.removeNode(tn2), tn2); // by TreeNode
      expect(bucket.count, 1);
      expect(bucket.removeNode(n3.id), tn3); // by ID
      expect(bucket.isEmpty, isTrue);

      expect(bucket.removeNode(id1), isNull);
    });
  });

  group('Node routing', () {
    test('differentLength is symmetric and bounded by one byte change', () {
      final id = ID.randomID(_idLen);
      final root = _node(id);
      addTearDown(root.dispose);
      expect(root.id.differentLength(id), 0);

      // Change only the last byte -> at most 8 bits differ.
      final bytes = [
        for (var i = 0; i < _idLen - 1; i++) id.getValueAt(i),
      ];
      final last = id.getValueAt(_idLen - 1);
      var newLast = Random().nextInt(256);
      while (newLast == last) {
        newLast = Random().nextInt(256);
      }
      bytes.add(newLast);
      expect(root.id.differentLength(ID.createID(bytes)) <= 8, isTrue);
    });

    test('findClosestNodes returns up to k nodes', () {
      final root = _node(ID.randomID(_idLen));
      addTearDown(root.dispose);
      for (var i = 0; i < 40; i++) {
        root.add(_node(ID.randomID(_idLen)));
      }
      final closest = root.findClosestNodes(ID.randomID(_idLen))!;
      expect(closest.length, 8); // default k
    });
  });

  group('KRPC messages', () {
    test('ping query', () {
      final tid = ID.randomID(2).toString();
      final nid = _ascii(_idLen);
      final obj = decode(pingMessage(tid, nid)!) as Map;
      expect(String.fromCharCodes(obj['y']), 'q');
      expect(String.fromCharCodes(obj['q']), 'ping');
      expect(String.fromCharCodes(obj['t']), tid);
      expect(String.fromCharCodes(obj['a']['id']), nid);
    });

    test('ping (pong) response', () {
      final tid = _ascii(2);
      final nid = _ascii(_idLen);
      final obj = decode(pongMessage(tid, nid)!) as Map;
      expect(String.fromCharCodes(obj['y']), 'r');
      expect(String.fromCharCodes(obj['t']), tid);
      expect(String.fromCharCodes(obj['r']['id']), nid);
    });

    test('find_node query', () {
      final tid = _ascii(2);
      final nid = _ascii(_idLen);
      final obj = decode(findNodeMessage(tid, nid, nid)!) as Map;
      expect(String.fromCharCodes(obj['y']), 'q');
      expect(String.fromCharCodes(obj['q']), 'find_node');
      expect(String.fromCharCodes(obj['t']), tid);
      expect(String.fromCharCodes(obj['a']['id']), nid);
      expect(String.fromCharCodes(obj['a']['target']), nid);
    });

    test('find_node response carries one compact-info blob per node', () {
      final tid = _ascii(2);
      final nid = _ascii(_idLen);
      final nodes = <Node>[
        _node(ID.randomID(_idLen),
            CompactAddress(InternetAddress.tryParse('120.0.0.1')!, 2222)),
        _node(ID.randomID(_idLen),
            CompactAddress(InternetAddress.tryParse('196.168.0.1')!, 2223)),
      ];
      addTearDown(() {
        for (final n in nodes) {
          n.dispose();
        }
      });

      final obj = decode(findNodeResponse(tid, nid, nodes)!) as Map;
      expect(String.fromCharCodes(obj['y']), 'r');
      expect(String.fromCharCodes(obj['t']), tid);
      expect(String.fromCharCodes(obj['r']['id']), nid);

      // Compact node info is binary carried as a byte-container String;
      // bencode_dart preserves it 1:1, so the blob comes back byte-exact.
      final nodeBytes = obj['r']['nodes'] as List<int>;
      expect(nodeBytes.length, 26 * nodes.length);

      final decoded = <Node>[];
      for (var i = 0; i < nodeBytes.length; i += 26) {
        final id = ID.createID(nodeBytes, i, 20);
        final addr = CompactAddress.parseIPv4Address(nodeBytes, i + 20);
        decoded.add(_node(id, addr));
      }
      addTearDown(() {
        for (final n in decoded) {
          n.dispose();
        }
      });
      expect(decoded.length, nodes.length);
      for (var i = 0; i < decoded.length; i++) {
        expect(decoded[i].id, nodes[i].id);
        expect(decoded[i].address, nodes[i].address);
        expect(decoded[i].port, nodes[i].port);
      }
    });

    test('get_peers query', () {
      final tid = _ascii(2);
      final nid = _ascii(_idLen);
      final infoHash = _ascii(_idLen);
      final obj = decode(getPeersMessage(tid, nid, infoHash)!) as Map;
      expect(String.fromCharCodes(obj['y']), 'q');
      expect(String.fromCharCodes(obj['q']), 'get_peers');
      expect(String.fromCharCodes(obj['t']), tid);
      expect(String.fromCharCodes(obj['a']['id']), nid);
      final ih = String.fromCharCodes(obj['a']['info_hash']);
      expect(ih.length, 20);
      expect(ih, infoHash);
    });

    test('get_peers response carries one value per peer', () {
      final tid = _ascii(2);
      final nid = _ascii(_idLen);
      final r = Random();
      const n = 10;
      final raw = <int>[];
      for (var i = 0; i < n * 6; i++) {
        raw.add(r.nextInt(256));
      }
      final peers = <CompactAddress>[
        for (var i = 0, off = 0; i < n; i++, off += 6)
          CompactAddress.parseIPv4Address(raw, off)!,
      ];

      final obj =
          decode(getPeersResponse(tid, nid, 'token', peers: peers)!) as Map;
      expect(String.fromCharCodes(obj['y']), 'r');
      expect(String.fromCharCodes(obj['t']), tid);
      expect(String.fromCharCodes(obj['r']['id']), nid);
      expect(String.fromCharCodes(obj['r']['token']), 'token');

      final values = obj['r']['values'] as List;
      expect(values.length, n);
      for (var i = 0; i < values.length; i++) {
        // Byte-container String is preserved 1:1, so the 6 bytes come back as-is.
        final bytes = values[i] as List<int>;
        final parsed = CompactAddress.parseIPv4Address(bytes)!;
        expect(parsed.addressString, peers[i].addressString);
        expect(parsed.port, peers[i].port);
      }
    });

    test('announce_peer query carries token, port and implied_port', () {
      final tid = _ascii(2);
      final nid = _ascii(_idLen);
      final infoHash = _ascii(_idLen);
      final obj =
          decode(announcePeerMessage(tid, nid, infoHash, 6881, 'tok')!) as Map;
      expect(String.fromCharCodes(obj['y']), 'q');
      expect(String.fromCharCodes(obj['q']), 'announce_peer');
      expect(String.fromCharCodes(obj['a']['id']), nid);
      expect(String.fromCharCodes(obj['a']['info_hash']), infoHash);
      expect(obj['a']['port'], 6881);
      expect(obj['a']['implied_port'], 1);
      expect(String.fromCharCodes(obj['a']['token']), 'tok');
    });

    test('error message', () {
      final tid = _ascii(2);
      final obj = decode(errorMessage(tid, 201, 'Generic Error')!) as Map;
      expect(String.fromCharCodes(obj['y']), 'e');
      expect(obj['e'][0], 201);
      expect(String.fromCharCodes(obj['e'][1]), 'Generic Error');
    });

    // KRPC carries binary (node ids, compact info) as Dart strings built via
    // String.fromCharCodes(bytes). bencode_dart preserves such byte-container
    // strings 1:1, so a 6-byte compact address (with a byte >= 0x80) stays
    // exactly 6 bytes on the wire — byte-exact interop, no recovery needed.
    test('binary-in-string compact address is preserved byte-exactly', () {
      final addr =
          CompactAddress(InternetAddress.tryParse('128.2.1.3')!, 12311);
      final original = latin1.encode(addr.toContactEncodingString()!);
      expect(original.length, 6);

      final obj = decode(
              getPeersResponse(_ascii(2), _ascii(_idLen), 'tok', peers: [addr])!)
          as Map;
      final onWire = (obj['r']['values'] as List).first as List<int>;
      expect(onWire.length, 6); // not widened
      expect(onWire, original); // byte-exact
    });
  });
}
