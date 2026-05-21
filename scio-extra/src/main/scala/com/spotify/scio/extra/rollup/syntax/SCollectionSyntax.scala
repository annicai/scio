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
     * Emits tagged records from a single flatMap: main records (un-expanded) go through `sumByKey`
     * then rollup expansion; correction records (already in rolled-up space) skip expansion and go
     * directly to the final `sumByKey`.
     *
     * @param keyClass
     *   SMB key class (= unique key to count distinct, typically userId)
     * @param read
     *   SMB read configuration
     * @param perKeyFn
     *   per-key extraction function: given a key and all its records, produce `(D, R, M)` tuples.
     *   The lazy SMB iterable is consumed once by this function.
     * @param rollupFunction
     *   rollup expansion function (same as in [[RollupOps.rollupAndCount]])
     * @param targetParallelism
     *   SMB read parallelism
     */
    @experimental
    def sortMergeRollupAndCount[K: Coder, V: Coder, D: Coder, R: Coder, M: Coder](
      keyClass: Class[K],
      read: SortedBucketIO.Read[V],
      perKeyFn: (K, Iterable[V]) => Seq[(D, R, M)],
      rollupFunction: R => Set[R],
      targetParallelism: TargetParallelism = TargetParallelism.auto()
    )(implicit g: Group[M]): SCollection[((D, R), (M, Long))] = {

      // Single flatMap consumes the lazy SMB iterable once via perKeyFn.
      // Emits tagged records: true = main (un-expanded), false = correction (already expanded).
      val tagged = self
        .sortMergeGroupByKey(keyClass, read, targetParallelism)
        .withName("SortMergeRollupAndCount")
        .flatMap { case (key, values) =>
          val tuples = perKeyFn(key, values)

          // Main: un-expanded (D, R) with measure and userCount=1
          val main = tuples.iterator.map { case (d, r, m) =>
            (true, ((d, r), (m, 1L)))
          }

          // Correction: group by D locally (no shuffle), compute corrections
          // in rolled-up R space for users with multiple R values per D
          val corrections = tuples
            .groupBy(_._1)
            .iterator
            .flatMap { case (d, byD) =>
              if (byD.size <= 1) Iterator.empty
              else {
                val rollupMap = collection.mutable.Map.empty[R, Long]
                for ((_, r, _) <- byD; r2 <- rollupFunction(r)) {
                  rollupMap(r2) = rollupMap.getOrElse(r2, 1L) - 1L
                }
                rollupMap.iterator.collect {
                  case (r2, count) if count < 0L =>
                    (false, ((d, r2), (g.zero, count)))
                }
              }
            }

          main ++ corrections
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
