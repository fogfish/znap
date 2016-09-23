/**
  *
  * Copyright (C) 2016 Zalando SE
  *
  * This software may be modified and distributed under the terms
  * of the MIT license.  See the LICENSE file for details.
  */
package org.zalando.znap.metrics

import com.codahale.metrics.JmxReporter

object AppMetrics {
  val metricRegistry = new com.codahale.metrics.MetricRegistry()

  val reporter = JmxReporter
    .forRegistry(metricRegistry)
    .build()
  reporter.start()
}
