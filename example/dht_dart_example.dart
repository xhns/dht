import 'dart:developer' as dev;

import 'package:dartorrent_common/dartorrent_common.dart';
import 'package:dht_dart/dht_dart.dart';

void main() async {
  // A torrent infohash is a 20-byte string. Replace these bytes with the
  // infohash of the torrent you want to find peers for. (Parsing a real
  // `.torrent` file is left to a torrent-model package and is intentionally
  // omitted here to keep the example free of extra dependencies.)
  var infohashStr = String.fromCharCodes(List<int>.filled(20, 0));

  var dht = DHT();
  var peers = <CompactAddress>{};
  dht.announce(infohashStr, 22123);
  dht.onError((code, msg) {
    dev.log('Error happend:', error: '[$code]$msg');
  });
  dht.onNewPeer((peer, token) {
    if (peers.add(peer)) {
      dev.log(
          'Found new peer address : $peer  ， Have ${peers.length} peers already');
    }
  });

  await dht.bootstrap(udpTimeout: 5, cleanNodeTime: 5 * 60);

  Future.delayed(Duration(seconds: 10), () {
    dht.stop();
  });
}
