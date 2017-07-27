package org.apache.hadoop.hbase.client.metrics.reporter;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.MetricsConnection;
import org.apache.hadoop.hbase.metrics.Interns;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

/** TODO:
 * 1) Implement the getMetrics system.
 * 2) Provide interaction and registration with MetricsSystem.
 * 3) Access methods for fetching and setting state.
 * 4) Debugging, testing, documentation.
 */

/**
 * Listens for HBase client metrics and reports them back to Hadoop Metrics2.
 */
@InterfaceAudience.Private
public class HadoopMetrics2Reporter extends ScheduledReporter implements MetricsSource {

  // private static final Logger LOG = LoggerFactory.getLogger(HadoopMetrics2Reporter.class);

  // hadoop metrics2 metadata.
  private String name, recordContext;

  // hadoop metrics2 metrics system
  private MetricsSystem metrics2System;

  /** TODO
   * Constructs a new HadoopMetrics2Reporter.
   * @param recordContext 
   */
  private HadoopMetrics2Reporter(MetricsSystem system, MetricRegistry registry, String name,
      String recordContext, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit) {
    super(registry, name, filter, rateUnit, durationUnit);
    this.name = name;
    this.recordContext = recordContext;
    this.metrics2System = system;
    this.metrics2System.register(name, "metrics from HBase client connection", this);
  }

  /**
   * Returns a new {@link Builder} for {@link JmxReporter}.
   *
   * @param registry the registry to report
   * @return a {@link Builder} instance for a {@link JmxReporter}
   */
  public static Builder forRegistry(MetricRegistry registry) {
    return new Builder(registry);
  }

  /**
   * Properly shutdown the reporter. (unregister from metrics system)
   */
  @Override
  public void close() {
    super.stop();
    metrics2System.unregisterSource(name);
  }

  /**
   * A builder to create {@link HadoopMetrics2Reporter} instances.
   */
  public static class Builder {

    private final MetricRegistry registry;
    private MetricFilter filter;
    private TimeUnit rateUnit;
    private TimeUnit durationUnit;
    private String recordContext;

    private Builder(MetricRegistry registry) {
      this.registry = registry;
      this.filter = MetricFilter.ALL;
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
      this.recordContext = "hbase.client";
    }

    /**
     * Convert rates to the given time unit. 
     * Defaults to {@link TimeUnit#SECONDS}.
     *
     * @param rateUnit a unit of time
     * @return {@code this}
     */
    public Builder convertRatesTo(TimeUnit rateUnit) {
      this.rateUnit = rateUnit;
      return this;
    }

    /**
     * Convert durations to the given time unit. 
     * Defaults to {@link TimeUnit#MILLISECONDS}.
     *
     * @param durationUnit a unit of time
     * @return {@code this}
     */
    public Builder convertDurationsTo(TimeUnit durationUnit) {
      this.durationUnit = durationUnit;
      return this;
    }

    /**
     * Only report metrics which match the given filter. 
     * Defaults to {@link MetricFilter#ALL}.
     *
     * @param filter a {@link MetricFilter}
     * @return {@code this}
     */
    public Builder filter(MetricFilter filter) {
      this.filter = filter;
      return this;
    }

    /**
     * A "context" name that will be added as a tag on each emitted metric record. 
     * Defaults to no "context" attribute on each record.
     *
     * @param recordContext The "context" tag
     * @return {@code this}
     */
    public Builder recordContext(String recordContext) {
      this.recordContext = recordContext;
      return this;
    }

    /**
     * Builds a {@link HadoopMetrics2Reporter} with the given properties, 
     * making metrics available to the Hadoop Metrics2 framework (any configured {@link MetricsSource}s.
     *
     * @param system The Hadoop Metrics2 system instance.
     *
     * @return a {@link HadoopMetrics2Reporter}
     */
    public HadoopMetrics2Reporter build(String name, MetricsSystem system) {
      return new HadoopMetrics2Reporter(system, registry, name, recordContext, filter, rateUnit,
          durationUnit);
    }

    /**
     * Builds a {@link HadoopMetrics2Reporter} with the given properties, 
     * making metrics available to the Hadoop Metrics2 framework (any configured {@link MetricsSource}s.
     * MetricsSystem is the DefaultMetricsSystem from metrics2.
     * @return a {@link HadoopMetrics2Reporter}
     */
    public HadoopMetrics2Reporter build(String name) {
      return new HadoopMetrics2Reporter(DefaultMetricsSystem.instance(), registry, name,
          recordContext, filter, rateUnit, durationUnit);
    }

  }

  /** Metrics Reporting **/

  @SuppressWarnings("rawtypes")
  private SortedMap<String, Gauge> gauges = new TreeMap<String, Gauge>();
  private SortedMap<String, Counter> counters = new TreeMap<String, Counter>();
  private SortedMap<String, Histogram> histograms = new TreeMap<String, Histogram>();
  private SortedMap<String, Meter> meters = new TreeMap<String, Meter>();
  private SortedMap<String, Timer> timers = new TreeMap<String, Timer>();

  @Override
  @SuppressWarnings("rawtypes")
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(this.name);
    builder.setContext(this.recordContext);

    for (String key : gauges.keySet()) {
      Gauge gauge = gauges.get(key);
      builder.addGauge(Interns.info(metricName(key), "gauge"), (double) gauge.getValue());
    }

    for (String key : counters.keySet()) {
      Counter counter = counters.get(key);
      builder.addCounter(Interns.info(metricName(key), "counter"), counter.getCount());
    }

    for (String key : histograms.keySet()) {
      Histogram histogram = histograms.get(key);
      addSnapshot(histogram.getSnapshot(), builder, metricName(key), "histogram");
    }

    for (String key : meters.keySet()) {
      Meter meter = meters.get(key);
      //addSnapshot(meter.getSnapshot(), builder, "histogram");
      // TODO: add meters to builder.
    }

    for (String key : timers.keySet()) {
      Timer timer = timers.get(key);
      addSnapshot(timer.getSnapshot(), builder, metricName(key), "timer");
    }

  }

  @Override
  @SuppressWarnings("rawtypes")
  public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters,
      SortedMap<String, Timer> timers) {
    this.gauges = gauges;
    this.counters = counters;
    this.histograms = histograms;
    this.meters = meters;
    this.timers = timers;
  }

  private String metricName(String name) {
    String classPrefix = String.format("%s.", MetricsConnection.class.getName());
    String namePostfix = String.format(".%s", this.name);
    return name.replace(classPrefix, "").replace(namePostfix, "");
  }

  private void addSnapshot(Snapshot snapshot, MetricsRecordBuilder builder, String name,
      String description) {
    builder.addGauge(Interns.info(name + "_mean", description),
        convertDuration(snapshot.getMean()));
    builder.addGauge(Interns.info(name + "_min", description), convertDuration(snapshot.getMin()));
    builder.addGauge(Interns.info(name + "_max", description), convertDuration(snapshot.getMax()));
    builder.addGauge(Interns.info(name + "_median", description),
        convertDuration(snapshot.getMedian()));
    builder.addGauge(Interns.info(name + "_stddev", description),
        convertDuration(snapshot.getStdDev()));

    builder.addGauge(Interns.info(name + "_75thpercentile", description),
        convertDuration(snapshot.get75thPercentile()));
    builder.addGauge(Interns.info(name + "_95thpercentile", description),
        convertDuration(snapshot.get95thPercentile()));
    builder.addGauge(Interns.info(name + "_98thpercentile", description),
        convertDuration(snapshot.get98thPercentile()));
    builder.addGauge(Interns.info(name + "_99thpercentile", description),
        convertDuration(snapshot.get99thPercentile()));
    builder.addGauge(Interns.info(name + "_999thpercentile", description),
        convertDuration(snapshot.get999thPercentile()));

  }

}
