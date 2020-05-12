package org.apache.ratis.logservice.metrics;

import org.apache.ratis.metrics.MetricRegistryInfo;
import org.apache.ratis.metrics.RatisMetricRegistry;
import org.apache.ratis.metrics.RatisMetrics;

import com.codahale.metrics.Timer;

import static org.apache.ratis.logservice.metrics.LogServiceMetrics.RATIS_LOG_SERVICE_METRICS;

public final class LogServiceMetaDataMetrics extends RatisMetrics {
  public static final String RATIS_LOG_SERVICE_META_DATA_METRICS = "metadata_statemachine";
  public static final String RATIS_LOG_SERVICE_META_DATA_METRICS_DESC =
      "Ratis log service metadata metrics";

  public LogServiceMetaDataMetrics(String serverId) {
    registry = getMetricRegistryForLogServiceMetaData(serverId);
  }

  private RatisMetricRegistry getMetricRegistryForLogServiceMetaData(String serverId) {
    return create(new MetricRegistryInfo(serverId,
        RATIS_LOG_SERVICE_METRICS,
        RATIS_LOG_SERVICE_META_DATA_METRICS, RATIS_LOG_SERVICE_META_DATA_METRICS_DESC));
  }

  public Timer getTimer(String name) {
    return registry.timer(name);
  }
}
