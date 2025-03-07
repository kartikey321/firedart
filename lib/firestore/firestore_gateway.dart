import 'dart:async';

import 'package:firedart/generated/google/firestore/v1/common.pb.dart';
import 'package:firedart/generated/google/firestore/v1/document.pb.dart' as fs;
import 'package:firedart/generated/google/firestore/v1/firestore.pbgrpc.dart';
import 'package:firedart/generated/google/firestore/v1/query.pb.dart';
import 'package:grpc/grpc.dart';

import '../firedart.dart';

typedef RequestAuthenticator = Future<void>? Function(
    Map<String, String> metadata, String uri);

class FirestoreGateway {
  final RequestAuthenticator? _authenticator;

  late final String database;

  late final String documentDatabase;

  final Map<String, _ListenStreamWrapper> _listenStreamCache;

  late FirestoreClient _client;

  late ClientChannel _channel;

  FirestoreGateway(
    String projectId, {
    String? databaseId,
    RequestAuthenticator? authenticator,
    Emulator? emulator,
  })  : _authenticator = authenticator,
        _listenStreamCache = <String, _ListenStreamWrapper>{} {
    database = 'projects/$projectId/databases/${databaseId ?? '(default)'}';
    documentDatabase = '$database/documents';
    _setupClient(emulator: emulator);
  }
  Future<Page<Document>> getCollection(
      String path, int pageSize, String nextPageToken) async {
    var request = ListDocumentsRequest()
      ..parent = path.substring(0, path.lastIndexOf('/'))
      ..collectionId = path.substring(path.lastIndexOf('/') + 1)
      ..pageSize = pageSize
      ..pageToken = nextPageToken;
    var response =
        await _client.listDocuments(request).catchError(_handleError);
    var documents =
        response.documents.map((rawDocument) => Document(this, rawDocument));
    return Page(documents, response.nextPageToken);
  }

  Stream<List<Document>> streamCollection(String path) {
    if (_listenStreamCache.containsKey(path)) {
      return _mapCollectionStream(_listenStreamCache[path]!);
    }

    var selector = StructuredQuery_CollectionSelector()
      ..collectionId = path.substring(path.lastIndexOf('/') + 1);
    var query = StructuredQuery()..from.add(selector);
    final queryTarget = Target_QueryTarget()
      ..parent = path.substring(0, path.lastIndexOf('/'))
      ..structuredQuery = query;
    final target = Target()..query = queryTarget;
    final request = ListenRequest()
      ..database = documentDatabase
      ..addTarget = target;

    _listenStreamCache[path] = _ListenStreamWrapper.create(
        request,
        (requestStream) => _client.listen(requestStream,
            options: CallOptions(
                metadata: {'google-cloud-resource-prefix': documentDatabase})),
        onDone: () => _listenStreamCache.remove(path));

    return _mapCollectionStream(_listenStreamCache[path]!);
  }

  Future<Document> createDocument(
      String path, String? documentId, fs.Document document) async {
    var split = path.split('/');
    var parent = split.sublist(0, split.length - 1).join('/');
    var collectionId = split.last;

    var request = CreateDocumentRequest()
      ..parent = parent
      ..collectionId = collectionId
      ..documentId = documentId ?? ''
      ..document = document;

    var response =
        await _client.createDocument(request).catchError(_handleError);
    return Document(this, response);
  }

  Future<Document> getDocument(
    String path, {
    List<int>? transaction,
  }) async {
    var getDocumentRequest = GetDocumentRequest()..name = path;

    if (transaction != null) {
      getDocumentRequest.transaction = transaction;
    }

    var rawDocument =
        await _client.getDocument(getDocumentRequest).catchError(_handleError);
    return Document(this, rawDocument);
  }

  Future<void> updateDocument(
      String path, fs.Document document, bool update) async {
    document.name = path;

    var request = UpdateDocumentRequest()..document = document;

    if (update) {
      var mask = DocumentMask();
      document.fields.keys.forEach(mask.fieldPaths.add);
      request.updateMask = mask;
    }

    await _client.updateDocument(request).catchError(_handleError);
  }

  Future<void> deleteDocument(String path) => _client
      .deleteDocument(DeleteDocumentRequest()..name = path)
      .catchError(_handleError);

  Stream<Document?> streamDocument(String path) {
    if (_listenStreamCache.containsKey(path)) {
      return _mapDocumentStream(_listenStreamCache[path]!.stream);
    }

    final documentsTarget = Target_DocumentsTarget()..documents.add(path);
    final target = Target()..documents = documentsTarget;
    final request = ListenRequest()
      ..database = documentDatabase
      ..addTarget = target;

    _listenStreamCache[path] = _ListenStreamWrapper.create(
        request,
        (requestStream) => _client.listen(requestStream,
            options: CallOptions(
                metadata: {'google-cloud-resource-prefix': documentDatabase})),
        onDone: () => _listenStreamCache.remove(path));

    return _mapDocumentStream(_listenStreamCache[path]!.stream);
  }

  Future<List<Document>> runQuery(
      StructuredQuery structuredQuery, String fullPath) async {
    final runQuery = RunQueryRequest()
      ..structuredQuery = structuredQuery
      ..parent = fullPath.substring(0, fullPath.lastIndexOf('/'));
    final response = _client.runQuery(runQuery);
    return await response
        .where((event) => event.hasDocument())
        .map((event) => Document(this, event.document))
        .toList();
  }

  Stream<List<Document>> streamQuery(String parent, StructuredQuery query) {
    // Generate a unique key for the query to manage stream cache
    String key = '$parent:${query.hashCode}';

    if (_listenStreamCache.containsKey(key)) {
      return _mapCollectionStream(_listenStreamCache[key]!);
    }

    final queryTarget = Target_QueryTarget()
      ..parent = parent
      ..structuredQuery = query;
    final target = Target()..query = queryTarget;
    final request = ListenRequest()
      ..database = database
      ..addTarget = target;

    _listenStreamCache[key] = _ListenStreamWrapper.create(
      request,
      (requestStream) => _client.listen(
        requestStream,
        options: CallOptions(
          metadata: {'google-cloud-resource-prefix': database},
        ),
      ),
      onDone: () => _listenStreamCache.remove(key),
    );

    return _mapCollectionStream(_listenStreamCache[key]!);
  }

  Future<T> runTransaction<T>(
    TransactionHandler<T> transactionHandler, {
    int maxAttempts = 5,
    int attempt = 1,
  }) async {
    try {
      return await _runTransaction(transactionHandler);
    } on GrpcError catch (e) {
      if (e.code == StatusCode.aborted && attempt < maxAttempts) {
        return await runTransaction(
          transactionHandler,
          attempt: attempt++,
          maxAttempts: maxAttempts,
        );
      } else {
        rethrow;
      }
    }
  }

  Future<T> _runTransaction<T>(
    TransactionHandler<T> transactionHandler,
  ) async {
    try {
      // Get fresh token
      final token = await FirebaseAuth.instance.tokenProvider.idToken;

      final transactionRequest = BeginTransactionRequest()..database = database;

      final options = CallOptions(metadata: {
        'authorization': 'Bearer $token',
        'google-cloud-resource-prefix': database,
        'x-goog-request-params': 'database=$database'
      });

      print('Starting transaction with database: $database');
      print('Token being used: $token');

      var transactionResponse = await _client
          .beginTransaction(transactionRequest, options: options)
          .catchError((e) {
        print('Detailed error in beginTransaction: $e');
        return _handleError(e);
      });

      var transactionId = transactionResponse.transaction;
      var transaction = Transaction(this, transactionId);

      final result = await transactionHandler(transaction);

      var commitRequest = CommitRequest()
        ..database = database
        ..transaction = transactionId
        ..writes.addAll(transaction.mutations);

      await _client
          .commit(commitRequest, options: options)
          .catchError(_handleError);

      return result;
    } catch (e, stack) {
      print('Full transaction error: $e');
      print('Stack trace: $stack');
      rethrow;
    }
  }

  void close() {
    _listenStreamCache.forEach((_, stream) => stream.close());
    _listenStreamCache.clear();
    _channel.shutdown();
  }

  void _setupClient({Emulator? emulator}) async {
    final token = FirebaseAuth.instance.tokenProvider.idToken;

    final callOptions = CallOptions(metadata: {
      'authorization': 'Bearer ${await token}',
      'google-cloud-resource-prefix': database,
      'x-goog-request-params': 'database=$database'
    });

    _listenStreamCache.clear();
    _channel = emulator == null
        ? ClientChannel(
            'firestore.googleapis.com',
            options: ChannelOptions(),
          )
        : ClientChannel(
            emulator.host,
            port: emulator.port,
            options: ChannelOptions(
              credentials: ChannelCredentials.insecure(),
            ),
          );
    _client = FirestoreClient(
      _channel,
      options: callOptions,
    );
  }

  void _handleError(e) {
    if (e is GrpcError &&
        [
          StatusCode.unknown,
          StatusCode.unimplemented,
          StatusCode.internal,
          StatusCode.unavailable,
          StatusCode.unauthenticated,
          StatusCode.dataLoss,
        ].contains(e.code)) {
      _setupClient();
    }
    throw e;
  }

  Stream<List<Document>> _mapCollectionStream(
      _ListenStreamWrapper listenRequestStream) {
    return listenRequestStream.stream
        .where((response) =>
            response.hasDocumentChange() ||
            response.hasDocumentRemove() ||
            response.hasDocumentDelete())
        .map((response) {
      if (response.hasDocumentChange()) {
        listenRequestStream.documentMap[response.documentChange.document.name] =
            Document(this, response.documentChange.document);
      } else {
        listenRequestStream.documentMap
            .remove(response.documentDelete.document);
      }
      return listenRequestStream.documentMap.values.toList();
    });
  }

  Stream<Document?> _mapDocumentStream(
      Stream<ListenResponse> listenRequestStream) {
    return listenRequestStream
        .where((response) =>
            response.hasDocumentChange() ||
            response.hasDocumentRemove() ||
            response.hasDocumentDelete())
        .map((response) => response.hasDocumentChange()
            ? Document(this, response.documentChange.document)
            : null);
  }
}

/// The number of retries to attempt when a stream throws an error.
const maxStreamReconnectRetries = 5;

class _ListenStreamWrapper {
  final void Function() onDone;

  final _errors = <_ErrorAndStackTrace>[];
  final ListenRequest _listenRequest;
  final Stream<ListenResponse> Function(Stream<ListenRequest>)
      responseStreamFactory;
  late StreamSubscription<ListenResponse>? _responseStreamSubscription;
  late StreamController<ListenRequest> _listenRequestStreamController;
  late StreamController<ListenResponse> _listenResponseStreamController;
  late Map<String, Document> _documentMap;

  Stream<ListenResponse> get stream => _listenResponseStreamController.stream;

  Map<String, Document> get documentMap => _documentMap;

  _ListenStreamWrapper.create(this._listenRequest, this.responseStreamFactory,
      {required this.onDone}) {
    _documentMap = <String, Document>{};
    _listenResponseStreamController =
        StreamController<ListenResponse>.broadcast(
      onListen: () {
        // Only when the response stream is listened to, we start the request stream.
        _retry();
      },
      onCancel: () {
        // We close the request stream if there are no more listeners to the response stream.
        _errors.clear();
        _responseStreamSubscription?.cancel();
        close();
      },
    );
  }

  void _retry() {
    _listenRequestStreamController = StreamController<ListenRequest>();
    final responseStream = responseStreamFactory(
      _listenRequestStreamController.stream,
    );

    _responseStreamSubscription = responseStream.listen(
      (value) {
        // When we receive a new event, we reset the errors, because
        // max connection retries are only incremented for consecutive errors.
        _errors.clear();
        _listenResponseStreamController.add(value);
      },
      onDone: _listenResponseStreamController.close,
      onError: (error, stackTrace) {
        _responseStreamSubscription!.cancel();
        _responseStreamSubscription = null;

        _errors.add(_ErrorAndStackTrace(error, stackTrace));

        if (_errors.length == maxStreamReconnectRetries) {
          for (var e in _errors) {
            _listenResponseStreamController.addError(e.error, e.stackTrace);
          }
          close();
        } else {
          _retry();
        }
      },
    );
    _listenRequestStreamController.add(_listenRequest);
  }

  void close() {
    _listenRequestStreamController.close();
    _listenResponseStreamController.close();
    onDone();
  }
}

class _ErrorAndStackTrace {
  final Object error;
  final StackTrace? stackTrace;

  _ErrorAndStackTrace(this.error, this.stackTrace);
}
