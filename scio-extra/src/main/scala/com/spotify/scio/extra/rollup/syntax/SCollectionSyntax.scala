/*
 * Copyright 2021 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.extra.rollup.syntax

import com.spotify.scio.ScioContext
import com.spotify.scio.annotations.experimental
import com.spotify.scio.coders.{BeamCoders, Coder}
import com.spotify.scio.smb.syntax.SortMergeBucketScioContextSyntax
import com.spotify.scio.values.SCollection
import com.twitter.algebird.Group
import org.apache.beam.sdk.extensions.smb.{SortedBucketIO, TargetParallelism}

trait SCollectionSyntax extends SortMergeBucketScioContextSyntax {

  implicit final class RollupOps[U, D, R, M](self: SCollection[(U, D, R, M)]) {

    /**
     * Takes an [[SCollection]] with elements consisting of three sets of dimensions and one measure
     * and returns an [[SCollection]] tuple, where the key is a set of dimensions and the value the
     * summed measure combined with a distinct count.
     *
     * This is to be used when doing a count distinct for one key over a set of dimensions, when
     * that key can be present in multiple elements in the final dataset, such that there is a need
     * to provide additional rollups over the non-unique dimensions where distinct counts are not
     * summable.
     *
     * U - Unique key, this is what we want to count distinct occurrences of D - Dimensions that
     * should not be rolled up (these are either unique per U or we are not expected to sum U over
     * these dimensions, eg. a metric for different dates) R - Dimensions that should be rolled up M
     *   - Additional measure that is summable over all dimensions
     *
     * @param rollupFunction
     *   A function takes one element with dimensions of type R and returns a set of R with one
     *   element for each combination of rollups that we want to provide
     */
    def rollupAndCount(
      rollupFunction: R => Set[R]
    )(implicit g: Group[M]): SCollection[((D, R), (M, Long))] = {
      implicit val (coderU, coderD, coderR, coderM) = BeamCoders.getTuple4Coders(self)

      val doubleCounting = self
        .withName("RollupAndCountDuplicates")
        .transform {
          _.map { case (_, dims, rollupDims, measure) =>
            ((dims, rollupDims), (measure, 1L))
          }.sumByKey
            .flatMap { case (dims @ (_, rollupDims), measure) =>
              rollupFunction(rollupDims)
                .map((x: R) => dims.copy(_2 = x) -> measure)
            }
        }

      val correctingCounts = self
        .withName("RollupAndCountCorrection")
        .transform {
          _.map { case (uniqueKey, dims, rollupDims, _) =>
            ((uniqueKey, dims), rollupDims)
          }.groupByKey
            .filterValues(_.size > 1)
            .flatMapValues { values =>
              val rollupMap = collection.mutable.Map.empty[R, Long]
              for (r <- values) {
                for (newDim <- rollupFunction(r)) {
                  rollupMap(newDim) = rollupMap.getOrElse(newDim, 1L) - 1L
                }
              }
              rollupMap.iterator.filter(_._2 < 0L)
            }
            .map { case ((_, dims), (rollupDims, count)) => ((dims, rollupDims), (g.zero, count)) }
        }

      SCollection
        .unionAll(List(doubleCounting, correctingCounts))
        .withName("RollupAndCountCorrected")
        .sumByKey

    }
  }

  implicit final class SortMergeRollupOps(@transient private val self: ScioContext)
      extends Serializable {

    /**
     * SMB-optimized variant of [[RollupOps.rollupAndCount]] that eliminates the expensive
     * correction branch shuffle by leveraging sort-merge bucket co-location.
     *
     * In the general [[RollupOps.rollupAndCount]], the correction branch requires a `groupByKey` on
     * `(uniqueKey, D)` to detect double-counted users across rollup dimensions — this shuffle is
     * typically the dominant cost. When the input is SMB-keyed by the unique key,
     * `sortMergeGroupByKey` already delivers all records per key to the same worker, so corrections
     * can be computed locally without a shuffle.
     *
     * Streams through the lazy SMB iterable once per key. Main records are emitted as the iterator
     * is consumed; only `(D -> List[R])` is accumulated in memory for computing corrections.
     * After the iterator is exhausted, correction records are emitted.
     *
     * @param keyClass
     *   SMB key class (= unique key to count distinct, typically userId)
     * @param read
     *   SMB read configuration
     * @param extractFn
     *   per-record extraction function: given a key and one record, produce a `(D, R, M)` tuple
     * @param rollupFunction
     *   rollup expansion function (same as in [[RollupOps.rollupAndCount]])
     * @param targetParallelism
     *   SMB read parallelism
     */
    @experimental
    def sortMergeRollupAndCount[K: Coder, V: Coder, D: Coder, R: Coder, M: Coder](
      keyClass: Class[K],
      read: SortedBucketIO.Read[V],
      extractFn: (K, V) => (D, R, M),
      rollupFunction: R => Set[R],
      targetParallelism: TargetParallelism = TargetParallelism.auto()
    )(implicit g: Group[M]): SCollection[((D, R), (M, Long))] = {

      val tagged = self
        .sortMergeGroupByKey(keyClass, read, targetParallelism)
        .withName("SortMergeRollupAndCount")
        .flatMap { case (key, values) =>
          // Accumulate only (D → List[R]) for corrections — measures not needed (g.zero)
          val correctionState = new collection.mutable.HashMap[D, collection.mutable.ArrayBuffer[R]]()

          // Phase 1: stream through lazy iterator, yield main records one at a time.
          // Side-effect: accumulate R values per D for correction computation.
          val mainIter = values.iterator.map { v =>
            val (d, r, m) = extractFn(key, v)
            correctionState.getOrElseUpdate(d, new collection.mutable.ArrayBuffer[R]()) += r
            (true, ((d, r), (m, 1L)))
          }

          // Phase 2: after main iterator is exhausted, compute and yield corrections.
          // Uses lazy iterator — corrections are computed on first access after main is done.
          val correctionIter = new Iterator[(Boolean, ((D, R), (M, Long)))] {
            private var inner: Iterator[(Boolean, ((D, R), (M, Long)))] = _

            private def ensureInit(): Unit = {
              if (inner == null) {
                inner = correctionState.iterator.flatMap { case (d, rs) =>
                  if (rs.size <= 1) Iterator.empty
                  else {
                    val rollupMap = collection.mutable.Map.empty[R, Long]
                    for (r <- rs; r2 <- rollupFunction(r)) {
                      rollupMap(r2) = rollupMap.getOrElse(r2, 1L) - 1L
                    }
                    rollupMap.iterator.collect {
                      case (r2, count) if count < 0L =>
                        (false, ((d, r2), (g.zero, count)))
                    }
                  }
                }
              }
            }

            override def hasNext: Boolean = { ensureInit(); inner.hasNext }
            override def next(): (Boolean, ((D, R), (M, Long))) = { ensureInit(); inner.next() }
          }

          mainIter ++ correctionIter
        }

      // Branch 1 (main): sum metric first (massive reduction), then expand rollups.
      val main = tagged
        .withName("SortMergeRollupAndCountDuplicates")
        .transform {
          _.filter(_._1)
            .map(_._2)
            .sumByKey
            .flatMap { case (dims @ (_, rollupDims), measure) =>
              rollupFunction(rollupDims)
                .map((x: R) => dims.copy(_2 = x) -> measure)
            }
        }

      // Branch 2 (correction): already in rolled-up space, no expansion needed.
      val corrections = tagged
        .withName("SortMergeRollupAndCountCorrection")
        .transform {
          _.filter(!_._1)
            .map(_._2)
        }

      SCollection
        .unionAll(List(main, corrections))
        .withName("SortMergeRollupAndCountCorrected")
        .sumByKey
    }
  }

}
