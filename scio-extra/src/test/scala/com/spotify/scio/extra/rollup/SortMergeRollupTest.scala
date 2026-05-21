/*
 * Copyright 2025 Spotify AB.
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

package com.spotify.scio.extra.rollup

import com.spotify.scio.avro.{Account, AccountStatus}
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.io.TextIO
import com.spotify.scio.smb.SmbIO
import com.spotify.scio.testing._
import com.twitter.algebird.Group
import com.twitter.algebird.macros.caseclass
import org.apache.beam.sdk.extensions.smb.AvroSortedBucketIO
import org.apache.beam.sdk.values.TupleTag

object SortMergeRollupTest {
  case class Measure(amount: Long)
  implicit val measureGroup: Group[Measure] = caseclass.group

  case class RollupDims(accountType: Option[String])

  def groupingSets(dims: RollupDims): Set[RollupDims] =
    (for {
      accountType <- List(dims.copy(accountType = None), dims)
    } yield accountType).toSet

  def avroRead(path: String): AvroSortedBucketIO.Read[Account] =
    AvroSortedBucketIO
      .read(new TupleTag[Account]("accounts"), classOf[Account])
      .from(path)

  def extractFn(
    @annotation.unused key: Integer,
    a: Account
  ): (String, RollupDims, Measure) =
    (
      a.getName.toString,
      RollupDims(accountType = Some(a.getType.toString)),
      Measure(a.getAmount.toLong)
    )

  def mkAccount(id: Int, tpe: String, name: String, amount: Double): Account =
    Account
      .newBuilder()
      .setId(id)
      .setType(tpe)
      .setName(name)
      .setAmount(amount)
      .setAccountStatus(AccountStatus.Active)
      .build()
}

object SortMergeRollupJob {
  import SortMergeRollupTest._

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sc.sortMergeRollupAndCount(
      classOf[Integer],
      avroRead(args("input")),
      extractFn,
      groupingSets
    ).map { case ((name, dims), (measure, count)) =>
        s"$name|${dims.accountType.getOrElse("ALL")}|${measure.amount}|$count"
      }
      .saveAsTextFile(args("output"))

    sc.run().waitUntilDone()
  }
}

class SortMergeRollupTest extends PipelineSpec {
  import SortMergeRollupTest._

  "sortMergeRollupAndCount" should "not double-count a user with multiple rollup dimensions" in {
    val input = Seq(
      mkAccount(1, "checking", "fixed-dim", 100.0),
      mkAccount(1, "savings", "fixed-dim", 200.0)
    )

    JobTest[SortMergeRollupJob.type]
      .args("--input=gs://input", "--output=gs://output")
      .input(SmbIO[Integer, Account]("gs://input", _.getId), input)
      .output(TextIO("gs://output")) { out =>
        out should containInAnyOrder(
          Seq(
            "fixed-dim|checking|100|1",
            "fixed-dim|savings|200|1",
            "fixed-dim|ALL|300|1"
          )
        )
      }
      .run()
  }

  it should "correctly count multiple users across cohorts" in {
    val input = Seq(
      mkAccount(1, "checking", "fixed-dim", 100.0),
      mkAccount(1, "savings", "fixed-dim", 200.0),
      mkAccount(2, "savings", "fixed-dim", 300.0)
    )

    JobTest[SortMergeRollupJob.type]
      .args("--input=gs://input", "--output=gs://output")
      .input(SmbIO[Integer, Account]("gs://input", _.getId), input)
      .output(TextIO("gs://output")) { out =>
        out should containInAnyOrder(
          Seq(
            "fixed-dim|checking|100|1",
            "fixed-dim|savings|500|2",
            "fixed-dim|ALL|600|2"
          )
        )
      }
      .run()
  }

  it should "work with empty input" in {
    JobTest[SortMergeRollupJob.type]
      .args("--input=gs://input", "--output=gs://output")
      .input(SmbIO[Integer, Account]("gs://input", _.getId), Seq.empty[Account])
      .output(TextIO("gs://output"))(_.should(beEmpty))
      .run()
  }

  it should "handle single user single cohort" in {
    val input = Seq(mkAccount(1, "checking", "fixed-dim", 100.0))

    JobTest[SortMergeRollupJob.type]
      .args("--input=gs://input", "--output=gs://output")
      .input(SmbIO[Integer, Account]("gs://input", _.getId), input)
      .output(TextIO("gs://output")) { out =>
        out should containInAnyOrder(
          Seq(
            "fixed-dim|checking|100|1",
            "fixed-dim|ALL|100|1"
          )
        )
      }
      .run()
  }

  it should "separate on fixed dimensions" in {
    val input = Seq(
      mkAccount(1, "checking", "group-A", 100.0),
      mkAccount(1, "checking", "group-B", 200.0)
    )

    JobTest[SortMergeRollupJob.type]
      .args("--input=gs://input", "--output=gs://output")
      .input(SmbIO[Integer, Account]("gs://input", _.getId), input)
      .output(TextIO("gs://output")) { out =>
        out should containInAnyOrder(
          Seq(
            "group-A|checking|100|1",
            "group-A|ALL|100|1",
            "group-B|checking|200|1",
            "group-B|ALL|200|1"
          )
        )
      }
      .run()
  }

  it should "produce identical results to rollupAndCount" in {
    val smbInput = Seq(
      mkAccount(1, "web", "2020-01-01", 100.0),
      mkAccount(1, "mobile", "2020-01-01", 200.0),
      mkAccount(2, "speaker", "2020-01-01", 200.0)
    )

    // Compute expected via rollupAndCount for comparison
    val rollupExpected = runWithData(
      Seq(
        (1: Integer, "2020-01-01", RollupDims(Some("web")), Measure(100L)),
        (1: Integer, "2020-01-01", RollupDims(Some("mobile")), Measure(200L)),
        (2: Integer, "2020-01-01", RollupDims(Some("speaker")), Measure(200L))
      )
    )(_.rollupAndCount(groupingSets)).map { case ((name, dims), (measure, count)) =>
      s"$name|${dims.accountType.getOrElse("ALL")}|${measure.amount}|$count"
    }

    JobTest[SortMergeRollupJob.type]
      .args("--input=gs://input", "--output=gs://output")
      .input(SmbIO[Integer, Account]("gs://input", _.getId), smbInput)
      .output(TextIO("gs://output")) { out =>
        out should containInAnyOrder(rollupExpected)
      }
      .run()
  }
}
