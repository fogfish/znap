/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.metrics

import java.util.concurrent.TimeUnit

import com.codahale.metrics.{Slf4jReporter, JmxReporter}

object AppMetrics {
  val metricRegistry = new com.codahale.metrics.MetricRegistry()

  val reporter = JmxReporter.forRegistry(metricRegistry)
    .convertRatesTo(TimeUnit.SECONDS)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .build()
  reporter.start()

  val logReporter = Slf4jReporter.forRegistry(metricRegistry)
    .convertRatesTo(TimeUnit.SECONDS)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .build()
  logReporter.start(1, TimeUnit.MINUTES)
}
