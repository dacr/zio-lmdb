/*
 * Copyright 2026 David Crosson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.lmdb.query

import zio._
import zio.stream._
import zio.lmdb._

/** A fluent builder for querying LMDB collections.
  *
  * @param collection
  *   The collection to query
  * @param ops
  *   Optional read operations context for shared transactions
  * @param keyPredicate
  *   Filter applied to keys before deserializing values
  * @param valuePredicate
  *   Filter applied to deserialized values
  * @param startKey
  *   Optional key to start the query from
  * @param isBackward
  *   Whether to iterate backwards
  * @param maxLimit
  *   Maximum number of results to return
  */
case class QueryBuilder[K, T](
  collection: LMDBCollection[K, T],
  ops: Option[LMDBReadOps] = None,
  keyPredicate: K => Boolean = (_: K) => true,
  valuePredicate: T => Boolean = (_: T) => true,
  startKey: Option[K] = None,
  isBackward: Boolean = false,
  maxLimit: Option[Int] = None
) {

  /** Add a filter condition on the key. Multiple calls are combined with AND.
    * @param f
    *   The predicate to apply to the key
    */
  def whereKey(f: K => Boolean): QueryBuilder[K, T] =
    copy(keyPredicate = k => keyPredicate(k) && f(k))

  /** Add a filter condition on the value. Multiple calls are combined with AND.
    * @param f
    *   The predicate to apply to the value
    */
  def whereValue(f: T => Boolean): QueryBuilder[K, T] =
    copy(valuePredicate = v => valuePredicate(v) && f(v))

  /** Start the query after the specified key.
    * @param k
    *   The key to start after
    */
  def startAfter(k: K): QueryBuilder[K, T] =
    copy(startKey = Some(k))

  /** Iterate backwards through the collection. */
  def reverse(): QueryBuilder[K, T] =
    copy(isBackward = true)

  /** Limit the maximum number of results returned.
    * @param n
    *   The maximum number of results
    */
  def limit(n: Int): QueryBuilder[K, T] =
    copy(maxLimit = Some(n))

  /** Execute the query and collect the results into a List.
    * @return
    *   A ZIO effect containing the list of matching values
    */
  def toList: IO[CollectErrors, List[T]] =
    ops match {
      case Some(o) => o.collect(collection.name, keyPredicate, valuePredicate, startKey, isBackward, maxLimit)(collection.kodec, collection.codec)
      case None    => collection.collect(keyPredicate, valuePredicate, startKey, isBackward, maxLimit)
    }

  /** Execute the query and return a ZStream of the results. Note: The stream does not currently support the `limit` parameter directly at the LMDB level, so it applies the limit using ZStream.take if a limit is set.
    * @return
    *   A ZStream of matching values
    */
  def toStream: ZStream[Any, StreamErrors, T] = {
    val stream = ops match {
      case Some(o) => o.stream(collection.name, keyPredicate, startKey, isBackward)(collection.kodec, collection.codec).filter(valuePredicate)
      case None    => collection.stream(keyPredicate, startKey, isBackward).filter(valuePredicate)
    }
    maxLimit match {
      case Some(n) => stream.take(n)
      case None    => stream
    }
  }

  /** Execute the query and return a ZStream of key-value pairs.
    * @return
    *   A ZStream of matching key-value pairs
    */
  def toStreamWithKeys: ZStream[Any, StreamErrors, (K, T)] = {
    val stream = ops match {
      case Some(o) => o.streamWithKeys(collection.name, keyPredicate, startKey, isBackward)(collection.kodec, collection.codec).filter { case (_, v) => valuePredicate(v) }
      case None    => collection.streamWithKeys(keyPredicate, startKey, isBackward).filter { case (_, v) => valuePredicate(v) }
    }
    maxLimit match {
      case Some(n) => stream.take(n)
      case None    => stream
    }
  }

  /** Join this query with another collection using a key extracted from the left value. This is useful for many-to-one or one-to-one relationships where the left record contains the primary key of the right record.
    *
    * @param rightCollection
    *   The collection to join with
    * @param extractKey
    *   Function to extract the key for the right collection from the left value
    * @tparam RK
    *   The key type of the right collection
    * @tparam RT
    *   The value type of the right collection
    * @return
    *   A JoinedQueryBuilder that will yield pairs of (LeftValue, RightValue)
    */
  def joinByKey[RK, RT](rightCollection: LMDBCollection[RK, RT])(extractKey: T => RK): JoinedQueryBuilder[T, RT] = {
    val joinedStream = toStream.mapZIO { leftValue =>
      val rightKey = extractKey(leftValue)
      val fetchResult = ops match {
        case Some(o) => o.fetch(rightCollection.name, rightKey)(rightCollection.kodec, rightCollection.codec)
        case None    => rightCollection.fetch(rightKey)
      }
      fetchResult.map {
        case Some(rightValue) => Some((leftValue, rightValue))
        case None             => None
      }
    }.collectSome

    JoinedQueryBuilder(joinedStream, ops)
  }

  /** Join this query with another collection using an index. This is useful for one-to-many relationships where an index maps the left record's key (or a value derived from it) to multiple keys in the right collection.
    *
    * @param rightCollection
    *   The collection to join with
    * @param index
    *   The index to use for the join
    * @param extractIndexKey
    *   Function to extract the index key from the left value
    * @tparam IK
    *   The key type of the index
    * @tparam RK
    *   The target key type of the index (which is the primary key of the right collection)
    * @tparam RT
    *   The value type of the right collection
    * @return
    *   A JoinedQueryBuilder that will yield pairs of (LeftValue, RightValue)
    */
  def joinByIndex[IK, RK, RT](
    rightCollection: LMDBCollection[RK, RT],
    index: LMDBIndex[IK, RK]
  )(extractIndexKey: T => IK): JoinedQueryBuilder[T, RT] = {
    val joinedStream = toStream.flatMap { leftValue =>
      val indexKey = extractIndexKey(leftValue)
      val indexedStream = ops match {
        case Some(o) => o.indexed(index.name, indexKey)(index.keyCodec, index.toKeyCodec)
        case None    => index.indexed(indexKey)
      }
      indexedStream
        .mapZIO { case (_, rightKey) =>
          val fetchResult = ops match {
            case Some(o) => o.fetch(rightCollection.name, rightKey)(rightCollection.kodec, rightCollection.codec)
            case None    => rightCollection.fetch(rightKey)
          }
          fetchResult.map {
            case Some(rightValue) => Some((leftValue, rightValue))
            case None             => None
          }
        }
        .collectSome
    }

    JoinedQueryBuilder(joinedStream, ops)
  }
}

/** A fluent builder for querying LMDB indexes.
  *
  * @param index
  *   The index to query
  * @param indexKey
  *   The key to start the index query from
  * @param limitToKey
  *   Whether to limit results to the specified index key
  * @param ops
  *   Optional read operations context for shared transactions
  * @param targetKeyPredicate
  *   Filter applied to the target keys found in the index
  * @param maxLimit
  *   Maximum number of results to return
  */
case class IndexQueryBuilder[IK, RK](
  index: LMDBIndex[IK, RK],
  indexKey: IK,
  limitToKey: Boolean = true,
  ops: Option[LMDBReadOps] = None,
  targetKeyPredicate: RK => Boolean = (_: RK) => true,
  maxLimit: Option[Int] = None
) {

  /** Add a filter condition on the target key. Multiple calls are combined with AND.
    * @param f
    *   The predicate to apply to the target key
    */
  def whereTargetKey(f: RK => Boolean): IndexQueryBuilder[IK, RK] =
    copy(targetKeyPredicate = rk => targetKeyPredicate(rk) && f(rk))

  /** Limit the maximum number of results returned.
    * @param n
    *   The maximum number of results
    */
  def limit(n: Int): IndexQueryBuilder[IK, RK] =
    copy(maxLimit = Some(n))

  /** Allow the query to continue past the initial index key to subsequent keys. */
  def continue(): IndexQueryBuilder[IK, RK] =
    copy(limitToKey = false)

  /** Execute the query and return a ZStream of the target keys.
    * @return
    *   A ZStream of matching target keys
    */
  def toStream: ZStream[Any, IndexErrors, RK] = {
    val stream = ops match {
      case Some(o) => o.indexed(index.name, indexKey, limitToKey)(index.keyCodec, index.toKeyCodec)
      case None    => index.indexed(indexKey, limitToKey)
    }
    val filtered = stream.map(_._2).filter(targetKeyPredicate)
    maxLimit match {
      case Some(n) => filtered.take(n)
      case None    => filtered
    }
  }

  /** Execute the query and collect the target keys into a List.
    * @return
    *   A ZIO effect containing the list of matching target keys
    */
  def toList: IO[IndexErrors, List[RK]] =
    toStream.runCollect.map(_.toList)

  /** Join this index query with a collection to fetch the actual records.
    *
    * @param collection
    *   The collection to join with
    * @tparam T
    *   The value type of the collection
    * @return
    *   A JoinedQueryBuilder that will yield pairs of (TargetKey, Record)
    */
  def join[T](collection: LMDBCollection[RK, T]): JoinedQueryBuilder[RK, T] = {
    val joinedStream = toStream.mapZIO { rk =>
      val fetchResult = ops match {
        case Some(o) => o.fetch(collection.name, rk)(collection.kodec, collection.codec)
        case None    => collection.fetch(rk)
      }
      fetchResult.map {
        case Some(t) => Some((rk, t))
        case None    => None
      }
    }.collectSome

    JoinedQueryBuilder(joinedStream, ops)
  }
}

/** A builder for queries that have been joined.
  *
  * @param stream
  *   The underlying stream of joined results
  * @param ops
  *   Optional read operations context for shared transactions
  * @tparam L
  *   The type of the left value
  * @tparam R
  *   The type of the right value
  */
case class JoinedQueryBuilder[L, R](
  stream: ZStream[Any, StorageUserError | StorageSystemError, (L, R)],
  ops: Option[LMDBReadOps] = None
) {

  /** Filter the joined results.
    * @param f
    *   The predicate to apply to the joined pair
    */
  def filter(f: (L, R) => Boolean): JoinedQueryBuilder[L, R] =
    copy(stream = stream.filter { case (l, r) => f(l, r) })

  /** Execute the query and collect the results into a List.
    * @return
    *   A ZIO effect containing the list of matching pairs
    */
  def toList: IO[StorageUserError | StorageSystemError, List[(L, R)]] =
    stream.runCollect.map(_.toList)

  /** Execute the query and return a ZStream of the results.
    * @return
    *   A ZStream of matching pairs
    */
  def toStream: ZStream[Any, StorageUserError | StorageSystemError, (L, R)] = stream

  /** Join this query with another collection using a key extracted from the current joined pair.
    *
    * @param rightCollection
    *   The collection to join with
    * @param extractKey
    *   Function to extract the key for the right collection from the current pair
    * @tparam RK
    *   The key type of the right collection
    * @tparam RT
    *   The value type of the right collection
    * @return
    *   A JoinedQueryBuilder that will yield tuples of ((LeftValue, RightValue), NewRightValue)
    */
  def joinByKey[RK, RT](rightCollection: LMDBCollection[RK, RT])(extractKey: ((L, R)) => RK): JoinedQueryBuilder[(L, R), RT] = {
    val joinedStream = stream.mapZIO { pair =>
      val rightKey = extractKey(pair)
      val fetchResult = ops match {
        case Some(o) => o.fetch(rightCollection.name, rightKey)(rightCollection.kodec, rightCollection.codec)
        case None    => rightCollection.fetch(rightKey)
      }
      fetchResult.map {
        case Some(rightValue) => Some((pair, rightValue))
        case None             => None
      }
    }.collectSome

    JoinedQueryBuilder(joinedStream, ops)
  }

  /** Join this query with another collection using an index.
    *
    * @param rightCollection
    *   The collection to join with
    * @param index
    *   The index to use for the join
    * @param extractIndexKey
    *   Function to extract the index key from the current pair
    * @tparam IK
    *   The key type of the index
    * @tparam RK
    *   The target key type of the index (which is the primary key of the right collection)
    * @tparam RT
    *   The value type of the right collection
    * @return
    *   A JoinedQueryBuilder that will yield tuples of ((LeftValue, RightValue), NewRightValue)
    */
  def joinByIndex[IK, RK, RT](
    rightCollection: LMDBCollection[RK, RT],
    index: LMDBIndex[IK, RK]
  )(extractIndexKey: ((L, R)) => IK): JoinedQueryBuilder[(L, R), RT] = {
    val joinedStream = stream.flatMap { pair =>
      val indexKey = extractIndexKey(pair)
      val indexedStream = ops match {
        case Some(o) => o.indexed(index.name, indexKey)(index.keyCodec, index.toKeyCodec)
        case None    => index.indexed(indexKey)
      }
      indexedStream
        .mapZIO { case (_, rightKey) =>
          val fetchResult = ops match {
            case Some(o) => o.fetch(rightCollection.name, rightKey)(rightCollection.kodec, rightCollection.codec)
            case None    => rightCollection.fetch(rightKey)
          }
          fetchResult.map {
            case Some(rightValue) => Some((pair, rightValue))
            case None             => None
          }
        }
        .collectSome
    }

    JoinedQueryBuilder(joinedStream, ops)
  }
}

object QueryBuilder {

  /** Extension methods to easily create a QueryBuilder from an LMDBCollection */
  implicit class LMDBCollectionQueryOps[K, T](val collection: LMDBCollection[K, T]) extends AnyVal {

    /** Start building a query for this collection */
    def query: QueryBuilder[K, T] = QueryBuilder(collection)
  }

  /** Extension methods to easily create a QueryBuilder from an LMDBCollectionReadOps */
  implicit class LMDBCollectionReadOpsQueryOps[K, T](val readOps: LMDBCollectionReadOps[K, T]) extends AnyVal {

    /** Start building a query for this collection within an existing transaction */
    def query: QueryBuilder[K, T] = QueryBuilder(readOps.collection, Some(readOps.ops))
  }

  /** Extension methods to easily create a QueryBuilder from an LMDBCollectionWriteOps */
  implicit class LMDBCollectionWriteOpsQueryOps[K, T](val writeOps: LMDBCollectionWriteOps[K, T]) extends AnyVal {

    /** Start building a query for this collection within an existing transaction */
    def query: QueryBuilder[K, T] = QueryBuilder(writeOps.collection, Some(writeOps.ops))
  }

  /** Extension methods to easily create an IndexQueryBuilder from an LMDBIndex */
  implicit class LMDBIndexQueryOps[IK, RK](val index: LMDBIndex[IK, RK]) extends AnyVal {

    /** Start building a query for this index starting at the specified key */
    def query(key: IK): IndexQueryBuilder[IK, RK] = IndexQueryBuilder(index, key)
  }

  /** Extension methods to easily create an IndexQueryBuilder from an LMDBIndexReadOps */
  implicit class LMDBIndexReadOpsQueryOps[IK, RK](val indexOps: LMDBIndexReadOps[IK, RK]) extends AnyVal {

    /** Start building a query for this index within an existing transaction starting at the specified key */
    def query(key: IK): IndexQueryBuilder[IK, RK] = IndexQueryBuilder(indexOps.index, key, ops = Some(indexOps.ops))
  }

  /** Extension methods to easily create an IndexQueryBuilder from an LMDBIndexWriteOps */
  implicit class LMDBIndexWriteOpsQueryOps[IK, RK](val indexOps: LMDBIndexWriteOps[IK, RK]) extends AnyVal {

    /** Start building a query for this index within an existing transaction starting at the specified key */
    def query(key: IK): IndexQueryBuilder[IK, RK] = IndexQueryBuilder(indexOps.index, key, ops = Some(indexOps.ops))
  }
}
