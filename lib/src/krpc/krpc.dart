import 'dart:async';
import 'dart:developer';
import 'dart:io';
import 'dart:typed_data';

import 'package:bencode_dart/bencode_dart.dart';
import 'package:dartorrent_common/dartorrent_common.dart';

import 'krpc_message.dart';
import '../kademlia/id.dart';
import '../kademlia/node.dart';

enum EVENT { ping, getPeers, findNode, announcePeer }

const timeOutTimeDefault = 15;

const genericError = 201;
const serverError = 202;
const protocolError = 203;
const unknownMethodError = 204;

typedef KRPCResponseHandler = void Function(
    List<int> nodeId, InternetAddress address, int port, dynamic data);

typedef KRPCQueryHandler = void Function(List<int> nodeId, String transactionId,
    InternetAddress address, int port, dynamic data);

typedef KRPCErrorHandler = void Function(
    InternetAddress address, int port, int code, String msg);

abstract class KRPC {
  /// Start KRPC service.
  ///
  /// Actually it start to UDP listening and init some parameters.
  Future start();

  /// Stop KRPC service.
  ///
  /// Close the UDP sockets and clean all handlers.
  Future stop([dynamic reason]);

  /// Local node id
  ID get nodeId;

  bool get isStopped;

  /// UDP port
  int get port;

  /// Code	Description
  /// - `201`	Generic Error
  /// - `202`	Server Error
  /// - `203`	Protocol Error, such as a malformed packet, invalid arguments, or bad token
  /// - `204`	Method Unknown
  void error(String tid, InternetAddress address, int port,
      [int code = 201, String msg = 'Generic Error']);

  bool? onError(KRPCErrorHandler handler);

  bool? offError(KRPCErrorHandler handler);

  /// send `ping` query to remote
  void ping(InternetAddress address, int port);

  bool? onPong(KRPCResponseHandler handler);

  bool? offPong(KRPCResponseHandler handler);

  /// send `ping` response to remote
  void pong(String tid, InternetAddress address, int port);

  bool? onPing(KRPCQueryHandler handler);

  bool? offPing(KRPCQueryHandler handler);

  /// send `find_node` query to remote
  void findNode(String targetId, InternetAddress address, int port);

  bool? onFindNodeResponse(KRPCResponseHandler handler);

  bool? offFindNodeResponse(KRPCResponseHandler handler);

  /// send `find_node` response to remote
  void responseFindNode(
      String tid, List<Node>? nodes, InternetAddress address, int port);

  bool? onFindNodeRequest(KRPCQueryHandler handler);

  bool? offFindNodeRequest(KRPCQueryHandler handler);

  /// send `get_peers` to remote
  void getPeers(String infoHash, InternetAddress address, int port);

  bool? onGetPeersRequest(KRPCQueryHandler handler);

  bool? offGetPeersRequest(KRPCQueryHandler handler);

  /// send `get_peers` response to remote
  void responseGetPeers(String tid, String infoHash, InternetAddress address,
      int port, String token,
      {Iterable<Node>? nodes, Iterable<CompactAddress>? peers});

  bool? onGetPeersReponse(KRPCResponseHandler handler);

  bool? offGetPeersResponse(KRPCResponseHandler handler);

  /// send `announce_peer` to remote
  void announcePeer(String infoHash, int peerPort, String token,
      InternetAddress address, int port,
      [bool impliedPort = true]);

  bool? onAnnouncePeerResponse(KRPCResponseHandler handler);

  bool? offAnnouncePeerResponse(KRPCResponseHandler handler);

  /// send `announce_peer` response to remote
  void responseAnnouncePeer(String tid, InternetAddress address, int port);

  bool? onAnnouncePeerRequest(KRPCQueryHandler handler);

  bool? offAnnouncePeerRequest(KRPCQueryHandler handler);

  /// Create a new KRPC service.
  factory KRPC.newService(ID nodeId,
      {int timeout = timeOutTimeDefault, int maxQuery = 24}) {
    var k = _KRPC(nodeId, timeout, maxQuery);
    return k;
  }
}

class _KRPC implements KRPC {
  int _globalTransactionId = 0;

  final _globalTransactionIdBuffer = Uint8List(2);

  bool _stopped = false;

  final ID _nodeId;

  final int _maxQuery;

  int _pendingQuery = 0;

  final int _timeOutTime;

  RawDatagramSocket? _socket;

  final Map<String, EVENT> _transactionsMap = <String, EVENT>{};

  final Map<String, dynamic> _transactionsValues = <String, dynamic>{};

  final Map<String, Timer> _timeoutMap = <String, Timer>{};

  _KRPC(this._nodeId, this._timeOutTime, this._maxQuery);

  final Map<EVENT, Set<KRPCResponseHandler>> _responseHandlers =
      <EVENT, Set<KRPCResponseHandler>>{};

  final Map<EVENT, Set<KRPCQueryHandler>> _queryHandlers =
      <EVENT, Set<KRPCQueryHandler>>{};

  final Set<KRPCErrorHandler> _errorHandlers = <KRPCErrorHandler>{};

  StreamController? _queryController;

  StreamSubscription? _querySub;

  @override
  void error(String tid, InternetAddress address, int port,
      [int code = 201, String msg = 'Generic Error']) {
    if (isStopped || _socket == null) return;
    var message = errorMessage(tid, code, msg);
    _socket?.send(message!, address, port);
  }

  @override
  bool offError(KRPCErrorHandler handler) {
    return _errorHandlers.remove(handler);
  }

  @override
  bool onError(KRPCErrorHandler handler) {
    return _errorHandlers.add(handler);
  }

  @override
  bool? offFindNodeRequest(KRPCQueryHandler handler) {
    return _queryHandlers[EVENT.findNode]?.remove(handler);
  }

  @override
  bool? onFindNodeRequest(KRPCQueryHandler handler) {
    _queryHandlers[EVENT.findNode] ??= <KRPCQueryHandler>{};
    return _queryHandlers[EVENT.findNode]?.add(handler);
  }

  @override
  bool? offFindNodeResponse(KRPCResponseHandler handler) {
    return _responseHandlers[EVENT.findNode]?.remove(handler);
  }

  @override
  bool? onFindNodeResponse(KRPCResponseHandler handler) {
    _responseHandlers[EVENT.findNode] ??= <KRPCResponseHandler>{};
    return _responseHandlers[EVENT.findNode]?.add(handler);
  }

  @override
  bool? onPong(KRPCResponseHandler handler) {
    _responseHandlers[EVENT.ping] ??= <KRPCResponseHandler>{};
    return _responseHandlers[EVENT.ping]?.add(handler);
  }

  @override
  bool? offPong(KRPCResponseHandler handler) {
    return _responseHandlers[EVENT.ping]?.remove(handler);
  }

  @override
  bool? onPing(KRPCQueryHandler handler) {
    _queryHandlers[EVENT.ping] ??= <KRPCQueryHandler>{};
    return _queryHandlers[EVENT.ping]?.add(handler);
  }

  @override
  bool? offPing(KRPCQueryHandler handler) {
    return _queryHandlers[EVENT.ping]?.remove(handler);
  }

  @override
  bool? onGetPeersRequest(KRPCQueryHandler handler) {
    _queryHandlers[EVENT.getPeers] ??= <KRPCQueryHandler>{};
    return _queryHandlers[EVENT.getPeers]?.add(handler);
  }

  @override
  bool? offGetPeersRequest(KRPCQueryHandler handler) {
    return _queryHandlers[EVENT.getPeers]?.remove(handler);
  }

  @override
  bool? offGetPeersResponse(KRPCResponseHandler handler) {
    return _responseHandlers[EVENT.getPeers]?.remove(handler);
  }

  @override
  bool? onGetPeersReponse(KRPCResponseHandler handler) {
    _responseHandlers[EVENT.getPeers] ??= <KRPCResponseHandler>{};
    return _responseHandlers[EVENT.getPeers]?.add(handler);
  }

  @override
  bool? offAnnouncePeerRequest(KRPCQueryHandler handler) {
    return _queryHandlers[EVENT.announcePeer]?.remove(handler);
  }

  @override
  bool? offAnnouncePeerResponse(KRPCResponseHandler handler) {
    return _responseHandlers[EVENT.announcePeer]?.remove(handler);
  }

  @override
  bool? onAnnouncePeerRequest(KRPCQueryHandler handler) {
    _queryHandlers[EVENT.announcePeer] ??= <KRPCQueryHandler>{};
    return _queryHandlers[EVENT.announcePeer]?.add(handler);
  }

  @override
  bool? onAnnouncePeerResponse(KRPCResponseHandler handler) {
    _responseHandlers[EVENT.announcePeer] ??= <KRPCResponseHandler>{};
    return _responseHandlers[EVENT.announcePeer]?.add(handler);
  }

  @override
  void responseAnnouncePeer(String tid, InternetAddress address, int port) {
    if (isStopped || _socket == null) return;
    var message = announcePeerResponse(tid, _nodeId.toString());
    _socket?.send(message!, address, port);
  }

  @override
  void announcePeer(String infoHash, int peerPort, String token,
      InternetAddress address, int port,
      [bool impliedPort = true]) {
    if (isStopped || _socket == null) return;
    var tid = _recordTransaction(EVENT.announcePeer);
    var message =
        announcePeerMessage(tid, _nodeId.toString(), infoHash, peerPort, token);
    _requestQuery(tid, message!, address, port);
  }

  @override
  void pong(String tid, InternetAddress address, int port) {
    if (isStopped || _socket == null) return;
    var message = pongMessage(tid, _nodeId.toString());
    _socket?.send(message!, address, port);
  }

  @override
  void responseFindNode(
      String tid, List<Node>? nodes, InternetAddress address, int port) {
    if (isStopped || _socket == null) return;
    var message = findNodeResponse(tid, _nodeId.toString(), nodes);
    if (message != null) {
      _socket?.send(message, address, port);
    }
  }

  @override
  void responseGetPeers(String tid, String infoHash, InternetAddress address,
      int port, String token,
      {Iterable<Node>? nodes, Iterable<CompactAddress>? peers}) {
    if (isStopped || _socket == null) return;
    var message = getPeersResponse(tid, _nodeId.toString(), token,
        nodes: nodes, peers: peers);
    _socket?.send(message!, address, port);
  }

  @override
  void ping(InternetAddress address, int port) async {
    if (isStopped || _socket == null) return;
    var tid = _recordTransaction(EVENT.ping);
    var message = pingMessage(tid, _nodeId.toString());
    _requestQuery(tid, message!, address, port);
  }

  @override
  void findNode(String targetId, InternetAddress address, int port) {
    if (isStopped || _socket == null) return;
    var tid = _recordTransaction(EVENT.findNode);
    var message = findNodeMessage(tid, _nodeId.toString(), targetId);
    _requestQuery(tid, message!, address, port);
  }

  @override
  void getPeers(String infoHash, InternetAddress address, int port) {
    if (isStopped || _socket == null) return;
    var tid = _recordTransaction(EVENT.getPeers);
    _transactionsValues[tid] = infoHash;
    var message = getPeersMessage(tid, _nodeId.toString(), infoHash);
    _requestQuery(tid, message!, address, port);
  }

  void _requestQuery(String transacationId, Uint8List message,
      InternetAddress address, int port) {
    _queryController ??= StreamController();
    _querySub ??= _queryController!.stream.listen(_processQueryRequest);
    // _totalPending++;
    // print('目前共有 $_totalPending 个请求');
    _queryController?.add({
      'message': message,
      'address': address,
      'port': port,
      'transacationId': transacationId
    });
  }

  void _processQueryRequest(dynamic event) {
    if (isStopped) return;
    _increasePendingQuery();
    var message = event['message'] as List<int>;
    var address = event['address'] as InternetAddress;
    var port = event['port'] as int;
    var tid = event['transacationId'] as String;
    // print('发送请求 $tid,目前pending请求数：$_pendingQuery');
    _timeoutMap[tid] =
        Timer(Duration(seconds: _timeOutTime), () => _fireTimeout(tid));
    _socket?.send(message, address, port);
  }

  String _recordTransaction(EVENT event) {
    var tid = createTransactionId();
    while (_transactionsMap[tid] != null) {
      tid = createTransactionId();
    }
    _transactionsMap[tid] = event;
    return tid;
  }

  void _fireTimeout(String id) {
    var event = _cleanTransaction(id);
    if (event != null) {
      _reducePendingQuery();
      // print('请求超时 $id,目前pending请求数：$_pendingQuery');
    }
  }

  EVENT? _cleanTransaction(String id) {
    var event = _transactionsMap.remove(id);
    _timeoutMap[id]?.cancel();
    _timeoutMap.remove(id);
    _transactionsValues.remove(id);
    return event;
  }

  String createTransactionId() {
    ++_globalTransactionId;
    if (_globalTransactionId == 65535) {
      _globalTransactionId = 0;
    }
    ByteData.view(_globalTransactionIdBuffer.buffer)
        .setUint16(0, _globalTransactionId);
    return String.fromCharCodes(_globalTransactionIdBuffer);
  }

  @override
  Future start() async {
    _socket ??= await RawDatagramSocket.bind(InternetAddress.anyIPv4, 6881);
    _socket!.listen((event) {
      if (event == RawSocketEvent.read) {
        var datagram = _socket!.receive();
        Timer.run(() {
          try {
            if(datagram != null) {
              _processReceiveData(
                  datagram.address, datagram.port, datagram.data);
            }
          } catch (e) {
            log('Process Receive Message Error $e',
                error: e, name: runtimeType.toString());
         }
        });
      }
    },
        onDone: () => stop('Remote/Local close the socket'),
        onError: (e) => stop(e));
    return _socket!.port;
  }

  void _reducePendingQuery() {
    _pendingQuery -= 1;
    if (_pendingQuery < _maxQuery && _querySub != null && _querySub!.isPaused) {
      _querySub?.resume();
    }
  }

  void _increasePendingQuery() {
    _pendingQuery += 1;
    if (_pendingQuery >= _maxQuery &&
        _querySub != null &&
        !_querySub!.isPaused) {
      _querySub?.pause();
    }
  }

  void _processReceiveData(
      InternetAddress address, int port, Uint8List bufferData) {
    _reducePendingQuery();

    // _totalPending--;
    // print('目前共有 $_totalPending 个请求');
    dynamic data;
    try {
      data = decode(bufferData);
    } catch (e) {
      _fireError(protocolError, null, 'Can\'t Decode Message', address, port);
      return;
    }
    if (data[transactionKey] == null || data[methodKey] == null) {
      _fireError(
          protocolError, null, 'Data Don\'t Contains y or t', address, port);
      return;
    }
    String? tid;
    try {
      tid = String.fromCharCodes(data[transactionKey], 0, 2);
    } catch (e) {
      log('解析Tid出错', error: e, name: runtimeType.toString());
    } //不就知道为什么有些Tid是4个字节的
    // print('请求响应 $tid ,目前pending请求数：$_pendingQuery');
    if (tid == null || tid.length != 2) {
      _fireError(
          protocolError, null, 'Incorret Transaction ID', address, port);
      return;
    }
    var additionalValues = _transactionsValues[tid];
    var event = _cleanTransaction(tid);
    String? method;
    try {
      method = String.fromCharCodes(data[methodKey], 0, 1);
    } catch (e) {
      log('解析Method出错', error: e, name: runtimeType.toString());
    }
    if (method == responseKey && data[responseKey] != null) {
      var idBytes = data[responseKey][idKey];
      if (idBytes == null) {
        _fireError(protocolError, tid, 'Incorrect Node ID', address, port);
        return;
      }
      var r = data[responseKey];
      if (additionalValues != null && r != null) {
        r['__additional'] = additionalValues;
      }
      // 处理远程发送的response
      if (event != null) {
        _fireResponse(event, idBytes, address, port, r);
      }
      return;
    }
    if (method == queryKey &&
        data[queryKey] != null &&
        data[queryKey].isNotEmpty) {
      var queryName = String.fromCharCodes(data[queryKey]);
      if (!queryKeys.contains(queryName)) {
        _fireError(
            serverError, tid, 'Unknown Query: $queryName', address, port);
        return;
      }
      var idBytes = data[argumentsKey][idKey];
      if (idBytes == null || idBytes.length != 20) {
        _fireError(protocolError, tid, 'Incorrect Node ID', address, port);
        return;
      }
      EVENT? event;
      if (queryName == methodPing) {
        event = EVENT.ping;
      }
      if (queryName == methodFindNode) {
        event = EVENT.findNode;
      }
      if (queryName == methodGetPeers) {
        event = EVENT.getPeers;
      }
      if (queryName == methodAnnouncePeer) {
        event = EVENT.announcePeer;
      }
      log('收到一个Query请求: $event  ， 来自 $address : $port');
      var arguments = data[argumentsKey];
      _fireQuery(event!, idBytes, tid, address, port, arguments);
      return;
    }
    if (method == errorKey) {
      var error = data[errorKey];
      if (error != null && error.length >= 2) {
        var code = error[0];
        var msg = 'unknown';
        if (error is List) msg = String.fromCharCodes(error[1]);
        _getError(tid, address, port, code, msg);
      }
      return;
    }
    _fireError(
        unknownMethodError, tid, 'Unknown Method: $method', address, port);
  }

  void _getError(
      String tid, InternetAddress address, int port, int code, String msg) {
    log('从 ${address.address}:$port 得到一个错误消息:',
        error: '[$code]$msg', name: runtimeType.toString());
    for (var element in _errorHandlers) {
      Timer.run(() => element(address, port, code, msg));
    }
  }

  /// Code	Description
  /// - `201`	Generic Error
  /// - `202`	Server Error
  /// - `203`	Protocol Error, such as a malformed packet, invalid arguments, or bad token
  /// - `204`	Method Unknown
  void _fireError(
      int code, String? tid, String msg, InternetAddress address, int port) {
    if (tid != null) {
      _transactionsMap.remove(tid);
      _timeoutMap[tid]?.cancel();
      _timeoutMap.remove(tid);
      error(tid, address, port, code, msg);
    } else {
      log('UnSend Error:', error: '[$code]$msg', name: runtimeType.toString());
    }
  }

  void _fireResponse(EVENT event, List<int> nodeIdBytes,
      InternetAddress address, int port, dynamic response) {
    var handlers = _responseHandlers[event];
    handlers?.forEach((handle) {
      Timer.run(() => handle(nodeIdBytes, address, port, response));
    });
  }

  void _fireQuery(EVENT event, List<int> nodeIdBytes, String transactionId,
      InternetAddress address, int port, dynamic arguments) {
    var handlers = _queryHandlers[event];
    handlers?.forEach((handle) {
      Timer.run(
          () => handle(nodeIdBytes, transactionId, address, port, arguments));
    });
  }

  @override
  Future stop([dynamic reason]) async {
    if (_stopped) return;
    _stopped = true;
    log('KRPC stopped , reason:', error: reason, name: runtimeType.toString());

    _socket?.close();
    _socket = null;
    _responseHandlers.clear();
    _queryHandlers.clear();
    _errorHandlers.clear();
    _globalTransactionId = 0;
    _globalTransactionIdBuffer[0] = 0;
    _globalTransactionIdBuffer[1] = 0;
    _pendingQuery = 0;
    _transactionsMap.clear();
    _transactionsValues.clear();
    for (var timer in _timeoutMap.values) {
      timer.cancel();
    }
    _timeoutMap.clear();
    try {
      await _querySub?.cancel();
    } finally {
      _querySub = null;
    }
    try {
      await _queryController?.close();
    } finally {
      _queryController = null;
    }
  }

  @override
  bool get isStopped => _stopped;

  @override
  ID get nodeId => _nodeId;

  @override
  int get port => _socket!.port;
}
