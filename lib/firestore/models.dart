import 'dart:collection';

import 'package:firedart/generated/google/firestore/v1/document.pb.dart' as fs;
import 'package:firedart/generated/google/firestore/v1/query.pb.dart';
import 'package:firedart/generated/google/protobuf/wrappers.pb.dart';
import 'package:firedart/generated/google/type/latlng.pb.dart';
import 'package:grpc/grpc.dart';

import '../generated/google/firestore/v1/common.pb.dart';
import '../generated/google/firestore/v1/write.pb.dart';
import 'firestore_gateway.dart';
import 'type_util.dart';

abstract class Reference {
  final FirestoreGateway _gateway;
  final String path;

  String get id => path.substring(path.lastIndexOf('/') + 1);

  String get fullPath => '${_gateway.documentDatabase}/$path';

  Reference(this._gateway, String path)
      : path = _trimSlashes(path.startsWith(_gateway.documentDatabase)
            ? path.substring(_gateway.documentDatabase.length + 1)
            : path);

  factory Reference.create(FirestoreGateway gateway, String path) {
    return _trimSlashes(path).split('/').length % 2 == 0
        ? DocumentReference(gateway, path)
        : CollectionReference(gateway, path);
  }

  @override
  bool operator ==(other) =>
      other is Reference &&
      runtimeType == other.runtimeType &&
      fullPath == other.fullPath;

  @override
  int get hashCode => Object.hash(runtimeType, fullPath);

  @override
  String toString() {
    return '$runtimeType: $path';
  }

  fs.Document _encodeMap(Map<String, dynamic> map) {
    var document = fs.Document();
    map.forEach((key, value) {
      document.fields[key] = TypeUtil.encode(value);
    });
    return document;
  }

  static String _trimSlashes(String path) {
    path = path.startsWith('/') ? path.substring(1) : path;
    return path.endsWith('/') ? path.substring(0, path.length - 2) : path;
  }
}

class CollectionReference extends Reference {
  final FirestoreGateway gateway;

  /// Constructs a [CollectionReference] using [FirestoreGateway] and path.
  ///
  /// Throws [Exception] if path contains odd amount of '/'.
  CollectionReference(this.gateway, String path) : super(gateway, path) {
    if (fullPath.split('/').length % 2 == 1) {
      throw Exception('Path is not a collection: $path');
    }
  }

  QueryReference where(
    String fieldPath, {
    dynamic isEqualTo,
    dynamic isLessThan,
    dynamic isLessThanOrEqualTo,
    dynamic isGreaterThan,
    dynamic isGreaterThanOrEqualTo,
    dynamic arrayContains,
    List<dynamic>? arrayContainsAny,
    List<dynamic>? whereIn,
    bool isNull = false,
  }) {
    return QueryReference(gateway, path).where(fieldPath,
        isEqualTo: isEqualTo,
        isLessThan: isLessThan,
        isLessThanOrEqualTo: isLessThanOrEqualTo,
        isGreaterThan: isGreaterThan,
        isGreaterThanOrEqualTo: isGreaterThanOrEqualTo,
        arrayContains: arrayContains,
        arrayContainsAny: arrayContainsAny,
        whereIn: whereIn,
        isNull: isNull);
  }

  /// Returns [CollectionReference] that's additionally sorted by the specified
  /// [fieldPath].
  ///
  /// The field is a [String] representing a single field name.
  /// After a [CollectionReference] order by call, you cannot add any more [orderBy]
  /// calls.
  QueryReference orderBy(String fieldPath, {bool descending = false}) =>
      QueryReference(gateway, path).orderBy(fieldPath, descending: descending);

  /// Returns [CollectionReference] that's additionally limited to only return up
  /// to the specified number of documents.
  QueryReference limit(int count) => QueryReference(gateway, path).limit(count);

  DocumentReference document(String id) =>
      DocumentReference(_gateway, '$path/$id');

  Future<Page<Document>> get(
          {int pageSize = 1024, String nextPageToken = ''}) =>
      _gateway.getCollection(fullPath, pageSize, nextPageToken);

  Stream<List<Document>> get stream {
    var fullCollectionPath = fullPath;
    var parent =
        fullCollectionPath.substring(0, fullCollectionPath.lastIndexOf('/'));
    var collectionId =
        fullCollectionPath.substring(fullCollectionPath.lastIndexOf('/') + 1);

    var query = StructuredQuery()
      ..from.add(
          StructuredQuery_CollectionSelector()..collectionId = collectionId);

    return _gateway.streamQuery(parent, query);
  }

  /// Create a document with a random id.
  Future<Document> add(Map<String, dynamic> map) =>
      _gateway.createDocument(fullPath, null, _encodeMap(map));
}

class DocumentReference extends Reference {
  DocumentReference(FirestoreGateway gateway, String path)
      : super(gateway, path) {
    if (fullPath.split('/').length % 2 == 0) {
      throw Exception('Path is not a document: $path');
    }
  }

  CollectionReference collection(String id) {
    return CollectionReference(_gateway, '$path/$id');
  }

  Future<Document> get() => _gateway.getDocument(fullPath);

  @Deprecated('Use the stream getter instead')
  Stream<Document?> subscribe() => stream;

  Stream<Document?> get stream => _gateway.streamDocument(fullPath);

  /// Check if a document exists.
  Future<bool> get exists async {
    try {
      await get();
      return true;
    } on GrpcError catch (e) {
      if (e.code == StatusCode.notFound) {
        return false;
      } else {
        rethrow;
      }
    }
  }

  /// Create a document if it doesn't exist, otherwise throw exception.
  Future<Document> create(Map<String, dynamic> map) => _gateway.createDocument(
      fullPath.substring(0, fullPath.lastIndexOf('/')), id, _encodeMap(map));

  /// Create or update a document.
  /// In the case of an update, any fields not referenced in the payload will be deleted.
  Future<void> set(Map<String, dynamic> map) async =>
      _gateway.updateDocument(fullPath, _encodeMap(map), false);

  /// Create or update a document.
  /// In case of an update, fields not referenced in the payload will remain unchanged.
  Future<void> update(Map<String, dynamic> map) =>
      _gateway.updateDocument(fullPath, _encodeMap(map), true);

  /// Deletes a document.
  Future<void> delete() async => await _gateway.deleteDocument(fullPath);
}

class Document {
  final FirestoreGateway _gateway;
  final fs.Document _rawDocument;

  Document(this._gateway, this._rawDocument);

  String get id => path.substring(path.lastIndexOf('/') + 1);

  String get path =>
      _rawDocument.name.substring(_rawDocument.name.indexOf('/documents') + 10);

  DateTime get createTime => _rawDocument.createTime.toDateTime();

  DateTime get updateTime => _rawDocument.updateTime.toDateTime();

  Map<String, dynamic> get map =>
      _rawDocument.fields.map((key, _) => MapEntry(key, this[key]));

  DocumentReference get reference => DocumentReference(_gateway, path);

  dynamic operator [](String key) {
    if (!_rawDocument.fields.containsKey(key)) return null;
    return TypeUtil.decode(_rawDocument.fields[key]!, _gateway);
  }

  @override
  String toString() => '$path $map';
}

class GeoPoint {
  final double latitude;
  final double longitude;

  const GeoPoint(this.latitude, this.longitude);

  /// Creates the [GeoPoint] instance using [LatLng].
  GeoPoint.fromLatLng(LatLng value) : this(value.latitude, value.longitude);

  @override
  String toString() => 'lat: $latitude, lon: $longitude';

  /// Creates the [LatLng] instance corresponding this geo point.
  LatLng toLatLng() => LatLng()
    ..latitude = latitude
    ..longitude = longitude;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is GeoPoint &&
          runtimeType == other.runtimeType &&
          latitude == other.latitude &&
          longitude == other.longitude;

  @override
  int get hashCode => latitude.hashCode ^ longitude.hashCode;
}

class Page<T> extends ListBase<T> {
  final _list = <T>[];
  final String nextPageToken;

  bool get hasNextPage => nextPageToken.isNotEmpty;

  @override
  int get length => _list.length;

  @override
  set length(int newLength) => _list.length = newLength;

  @override
  T operator [](int index) => _list[index];

  @override
  void operator []=(int index, T value) => _list[index] = value;

  Page(Iterable<T> iterable, this.nextPageToken) {
    _list.addAll(iterable);
  }
}

class QueryReference extends Reference {
  final StructuredQuery _structuredQuery = StructuredQuery();

  QueryReference(super.gateway, super.path) {
    _structuredQuery.from
        .add(StructuredQuery_CollectionSelector()..collectionId = id);
  }

  QueryReference where(
    String fieldPath, {
    dynamic isEqualTo,
    dynamic isLessThan,
    dynamic isLessThanOrEqualTo,
    dynamic isGreaterThan,
    dynamic isGreaterThanOrEqualTo,
    dynamic arrayContains,
    List<dynamic>? arrayContainsAny,
    List<dynamic>? whereIn,
    bool isNull = false,
  }) {
    if (isEqualTo != null) {
      _addFilter(fieldPath, isEqualTo,
          operator: StructuredQuery_FieldFilter_Operator.EQUAL);
    }
    if (isLessThan != null) {
      _addFilter(fieldPath, isLessThan,
          operator: StructuredQuery_FieldFilter_Operator.LESS_THAN);
    }
    if (isLessThanOrEqualTo != null) {
      _addFilter(fieldPath, isLessThanOrEqualTo,
          operator: StructuredQuery_FieldFilter_Operator.LESS_THAN_OR_EQUAL);
    }
    if (isGreaterThan != null) {
      _addFilter(fieldPath, isGreaterThan,
          operator: StructuredQuery_FieldFilter_Operator.GREATER_THAN);
    }
    if (isGreaterThanOrEqualTo != null) {
      _addFilter(fieldPath, isGreaterThanOrEqualTo,
          operator: StructuredQuery_FieldFilter_Operator.GREATER_THAN_OR_EQUAL);
    }
    if (arrayContains != null) {
      _addFilter(fieldPath, arrayContains,
          operator: StructuredQuery_FieldFilter_Operator.ARRAY_CONTAINS);
    }
    if (arrayContainsAny != null) {
      _addFilter(fieldPath, arrayContainsAny,
          operator: StructuredQuery_FieldFilter_Operator.ARRAY_CONTAINS_ANY);
    }
    if (whereIn != null) {
      _addFilter(fieldPath, whereIn,
          operator: StructuredQuery_FieldFilter_Operator.IN);
    }
    if (isNull) {
      _addFilter(fieldPath, null);
    }

    return this;
  }

  Stream<List<Document>> get stream {
    var collectionPath = fullPath;
    var parent = collectionPath.substring(0, collectionPath.lastIndexOf('/'));
    return _gateway.streamQuery(parent, _structuredQuery);
  }

  /// Returns [QueryReference] that's additionally sorted by the specified
  /// [fieldPath].
  ///
  /// The field is a [String] representing a single field name.
  /// After a [QueryReference] order by call, you cannot add any more [orderBy]
  /// calls.
  QueryReference orderBy(
    String fieldPath, {
    bool descending = false,
  }) {
    final order = StructuredQuery_Order();
    order.field_1 = StructuredQuery_FieldReference()..fieldPath = fieldPath;
    order.direction = descending
        ? StructuredQuery_Direction.DESCENDING
        : StructuredQuery_Direction.ASCENDING;
    _structuredQuery.orderBy.add(order);
    return this;
  }

  /// Returns [QueryReference] that's additionally limited to only return up
  /// to the specified number of documents.
  QueryReference limit(int count) {
    _structuredQuery.limit = Int32Value()..value = count;
    return this;
  }

  Future<List<Document>> get() => _gateway.runQuery(_structuredQuery, fullPath);

  void _addFilter(String fieldPath, dynamic value,
      {StructuredQuery_FieldFilter_Operator? operator}) {
    var queryFilter = StructuredQuery_Filter();
    if (value == null || operator == null) {
      var filter = StructuredQuery_UnaryFilter();
      filter.op = StructuredQuery_UnaryFilter_Operator.IS_NULL;
      filter.field_2 = StructuredQuery_FieldReference()..fieldPath = fieldPath;

      queryFilter.unaryFilter = filter;
    } else {
      var filter = StructuredQuery_FieldFilter();
      filter.op = operator;
      filter.value = TypeUtil.encode(value);

      final fieldReference = StructuredQuery_FieldReference()
        ..fieldPath = fieldPath;
      filter.field_1 = fieldReference;

      queryFilter.fieldFilter = filter;
    }

    StructuredQuery_CompositeFilter compositeFilter;
    if (_structuredQuery.hasWhere() &&
        _structuredQuery.where.hasCompositeFilter()) {
      compositeFilter = _structuredQuery.where.compositeFilter;
    } else {
      compositeFilter = StructuredQuery_CompositeFilter()
        ..op = StructuredQuery_CompositeFilter_Operator.AND;
    }

    compositeFilter.filters.add(queryFilter);
    _structuredQuery.where = StructuredQuery_Filter()
      ..compositeFilter = compositeFilter;
  }
}

/// Signature of a transaction callback.
typedef TransactionHandler<T> = Future<T> Function(Transaction transaction);

/// Transaction class which is created from a call to [runTransaction()].
class Transaction {
  final FirestoreGateway _gateway;
  final List<int> _transaction;

  Transaction(this._gateway, this._transaction);

  final List<Write> _mutations = <Write>[];

  /// An immutable list of the [Write]s that have been added to this transaction.
  UnmodifiableListView<Write> get mutations => UnmodifiableListView(_mutations);

  /// Reads the document referenced by the provided [path].
  ///
  /// If the document does not exist, the operation throws a [GrpcError] with
  /// [StatusCode.notFound].
  Future<Document> get(String path) async {
    return _gateway.getDocument(
      _fullPath(path),
      transaction: _transaction,
    );
  }

  /// Deletes the document referred by the provided [path].
  ///
  /// If the document does not exist, the operation does nothing and returns
  /// normally.
  void delete(String path) {
    _mutations.add(
      Write(delete: _fullPath(path)),
    );
  }

  /// Updates fields provided in [data] for the document referred to by [path].
  ///
  /// Only the fields specified in [data] will be updated. Fields that
  /// are not specified in [data] will not be changed.
  ///
  /// If the document does not yet exist, it will be created.
  void update(String path, Map<String, dynamic> data) {
    _mutations.add(
      Write(
        updateMask: DocumentMask(fieldPaths: data.keys),
        update: fs.Document(
          name: _fullPath(path),
          fields: _encodeMap(data),
        ),
      ),
    );
  }

  /// Sets fields provided in [data] for the document referred to by [path].
  ///
  /// All fields will be overwritten with the provided [data]. This means
  /// that all fields that are not specified in [data] will be deleted.
  ///
  /// If the document does not yet exist, it will be created.
  void set(String path, Map<String, dynamic> data) {
    _mutations.add(
      Write(
        updateMask: null,
        update: fs.Document(
          name: _fullPath(path),
          fields: _encodeMap(data),
        ),
      ),
    );
  }

  String _fullPath(String path) => '${_gateway.documentDatabase}/$path';

  Map<String, fs.Value> _encodeMap(Map<String, dynamic> map) {
    return map.map((key, value) => MapEntry(key, TypeUtil.encode(value)));
  }
}
