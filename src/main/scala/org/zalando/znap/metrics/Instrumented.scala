/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.metrics
import nl.grons.metrics.scala.MetricName

trait Instrumented extends nl.grons.metrics.scala.InstrumentedBuilder {

  override lazy val metricBaseName: MetricName = MetricName("")

  val metricRegistry = AppMetrics.metricRegistry
}
