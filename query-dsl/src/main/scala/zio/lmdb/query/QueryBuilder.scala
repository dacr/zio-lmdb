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
    collection.collect(keyPredicate, valuePredicate, startKey, isBackward, maxLimit)

  /** Execute the query and return a ZStream of the results. Note: The stream does not currently support the `limit` parameter directly at the LMDB level, so it applies the limit using ZStream.take if a limit is set.
    * @return
    *   A ZStream of matching values
    */
  def toStream: ZStream[Any, StreamErrors, T] = {
    val stream = collection.stream(keyPredicate, startKey, isBackward).filter(valuePredicate)
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
    val stream = collection.streamWithKeys(keyPredicate, startKey, isBackward).filter { case (_, v) => valuePredicate(v) }
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
      rightCollection.fetch(rightKey).map {
        case Some(rightValue) => Some((leftValue, rightValue))
        case None             => None
      }
    }.collectSome

    JoinedQueryBuilder(joinedStream)
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
      index
        .indexed(indexKey)
        .mapZIO { case (_, rightKey) =>
          rightCollection.fetch(rightKey).map {
            case Some(rightValue) => Some((leftValue, rightValue))
            case None             => None
          }
        }
        .collectSome
    }

    JoinedQueryBuilder(joinedStream)
  }
}

/** A builder for queries that have been joined.
  *
  * @param stream
  *   The underlying stream of joined results
  * @tparam L
  *   The type of the left value
  * @tparam R
  *   The type of the right value
  */
case class JoinedQueryBuilder[L, R](stream: ZStream[Any, StorageUserError | StorageSystemError, (L, R)]) {

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
      rightCollection.fetch(rightKey).map {
        case Some(rightValue) => Some((pair, rightValue))
        case None             => None
      }
    }.collectSome

    JoinedQueryBuilder(joinedStream)
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
      index
        .indexed(indexKey)
        .mapZIO { case (_, rightKey) =>
          rightCollection.fetch(rightKey).map {
            case Some(rightValue) => Some((pair, rightValue))
            case None             => None
          }
        }
        .collectSome
    }

    JoinedQueryBuilder(joinedStream)
  }
}

object QueryBuilder {

  /** Extension methods to easily create a QueryBuilder from an LMDBCollection */
  implicit class LMDBCollectionQueryOps[K, T](val collection: LMDBCollection[K, T]) extends AnyVal {

    /** Start building a query for this collection */
    def query: QueryBuilder[K, T] = QueryBuilder(collection)
  }
}
