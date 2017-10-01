/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Message;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.DynamicMetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounter;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.hadoop.metrics2.lib.MutableHistogram;
import org.apache.hadoop.metrics2.lib.MutableMetric;
import org.apache.hadoop.metrics2.lib.MutableTimeHistogram;

import org.apache.yetus.audience.InterfaceAudience;

import com.google.common.annotations.VisibleForTesting;

/**
 * Class that transitions metrics from MetricsClient into the metrics subsystem.
 *
 * Implements BaseSource through BaseSourceImpl, following the pattern.
 */
@InterfaceAudience.Private
public class MetricsClientSourceImpl extends BaseSourceImpl implements MetricsClientSource {

  /** mutable metrics models **/

  private String metricsJmxContext;
  private DynamicMetricsRegistry registry;
  private static AtomicLong TICKET = new AtomicLong(0);

  @VisibleForTesting
  protected final MutableFastCounter metaCacheHits;
  @VisibleForTesting
  protected final MutableFastCounter metaCacheMisses;
  @VisibleForTesting
  protected final MutableFastCounter metaCacheNumClearServers;
  @VisibleForTesting
  protected final MutableFastCounter metaCacheNumClearRegions;
  @VisibleForTesting
  protected final CallTracker getTracker;
  @VisibleForTesting
  protected final CallTracker scanTracker;
  @VisibleForTesting
  protected final CallTracker appendTracker;
  @VisibleForTesting
  protected final CallTracker deleteTracker;
  @VisibleForTesting
  protected final CallTracker incrementTracker;
  @VisibleForTesting
  protected final CallTracker putTracker;
  @VisibleForTesting
  protected final CallTracker multiTracker;
  @VisibleForTesting
  protected final RunnerStats runnerStats;

  @VisibleForTesting
  protected final MutableFastCounter hedgedReadOps;
  @VisibleForTesting
  protected final MutableFastCounter hedgedReadWin;
  @VisibleForTesting
  protected final MutableHistogram concurrentCallsPerServerHistogram;

  @VisibleForTesting
  protected final Map<String, RegionStats> regionStats;
  @VisibleForTesting
  protected final Function<String, RegionStats> newRegionStat =
      path -> new RegionStats(registry, path, "region stats for " + path);

  @VisibleForTesting
  protected final ConcurrentMap<String, MutableFastCounter> cacheDroppingExceptions;
  @VisibleForTesting
  protected final Function<String, MutableFastCounter> newCacheCounter =
      name -> registry.newCounter(name, "cache dropping exception.", 0l);

  /**
   * Constructs a new MetricsClientSourceImpl.
   */
  public MetricsClientSourceImpl() {
    // Will initialize with the default client connection metrics jmx context!!!!
    // Must call setJmxContext to reregister with proper context.
    super(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT,
        METRICS_DEFAULT_JMX_CONTEXT + TICKET.getAndIncrement());
    this.registry = super.getMetricsRegistry();
    this.metricsJmxContext = super.metricsJmxContext;

    // meta cache metrics
    this.metaCacheHits = this.registry.newCounter(META_CACHE_HITS_KEY, META_CACHE_HITS_DESC, 0l);
    this.metaCacheMisses = this.registry.newCounter(META_CACHE_MISSES_KEY, META_CACHE_MISSES_DESC, 0l);
    this.metaCacheNumClearServers = this.registry.newCounter(META_CACHE_CLEAR_SERVERS_KEY,
        META_CACHE_CLEAR_SERVERS_DESC, 0l);
    this.metaCacheNumClearRegions = this.registry.newCounter(META_CACHE_CLEAR_REGIONS_KEY,
        META_CACHE_CLEAR_REGIONS_DESC, 0l);

    // rpc call trackers
    this.getTracker = new CallTracker(this.registry, "Get", "rpc metrics for GET calls");
    this.scanTracker = new CallTracker(this.registry, "Scan", "rpc metrics for SCAN calls");
    this.appendTracker = new CallTracker(this.registry, "Append", "rpc metrics for APPEND calls");
    this.deleteTracker = new CallTracker(this.registry, "Delete", "rpc metrics for DELETE calls");
    this.incrementTracker = new CallTracker(this.registry, "Increment", "rpc metrics for INCREMENT calls");
    this.putTracker = new CallTracker(this.registry, "Put", "rpc metrics for PUT calls");
    this.multiTracker = new CallTracker(this.registry, "Multi", "rpc metrics for Multi-commit transaction calls");

    this.hedgedReadOps = this.registry.newCounter(HEDGED_READ_OPS_KEY, HEDGED_READ_OPS_DESC, 0l);
    this.hedgedReadWin = this.registry.newCounter(HEDGED_READ_WIN_KEY, HEDGED_READ_WIN_DESC, 0l);
    this.concurrentCallsPerServerHistogram = this.registry.newHistogram(CONCURRENT_CALLS_KEY, CONCURRENT_CALLS_DESC);

    this.runnerStats = new RunnerStats(this.registry);
    this.regionStats = new ConcurrentHashMap<>();
    this.cacheDroppingExceptions = new ConcurrentHashMap<>();
  }

  @Override
  public void shutdown() {
    DefaultMetricsSystem.instance().unregisterSource(this.metricsJmxContext);
  }

  @VisibleForTesting
  protected static class RunnerStats {
    final MutableFastCounter normalRunners;
    final MutableFastCounter delayRunners;
    final MutableHistogram delayIntervalHistogram;

    private RunnerStats(DynamicMetricsRegistry registry) {
      this.normalRunners = registry.newCounter(RUNNERS_NORMAL_COUNT_KEY, RUNNERS_NORMAL_COUNT_DESC, 0l);
      this.delayRunners = registry.newCounter(RUNNERS_DELAY_COUNT_KEY, RUNNERS_DELAY_COUNT_DESC, 0l);
      this.delayIntervalHistogram = registry.newHistogram(RUNNERS_DELAY_HIST_KEY, RUNNERS_DELAY_HIST_DESC);
    }

    public void incrNormalRunners() {
      this.normalRunners.incr();
    }

    public void incrDelayRunners() {
      this.delayRunners.incr();
    }

    public void updateDelayInterval(long interval) {
      this.delayIntervalHistogram.add(interval);
    }
  }

  @VisibleForTesting
  protected static final class CallTracker {
    private final String name;
    @VisibleForTesting
    final MutableTimeHistogram callTimer;
    @VisibleForTesting
    final MutableHistogram reqHist;
    @VisibleForTesting
    final MutableHistogram respHist;

    private CallTracker(DynamicMetricsRegistry registry, String name) {
      this(registry, name, "");
    }

    private CallTracker(DynamicMetricsRegistry registry, String name, String desc) {
      this.name = name; // units are in milliseconds.
      this.callTimer = registry.newTimeHistogram(String.format("%s_(%s)", RPC_DURATION_BASE, name), desc);
      this.reqHist = registry.newHistogram(String.format("%s_(%s)", RPC_REQUEST_BASE, name), desc);
      this.respHist = registry.newHistogram(String.format("%s_(%s)", RPC_RESPONSE_BASE, name), desc);
    }

    public void updateRpc(CallStats stats) {
      this.callTimer.add(stats.getCallTimeMs());
      this.reqHist.add(stats.getRequestSizeBytes());
      this.respHist.add(stats.getResponseSizeBytes());
    }

    @Override
    public String toString() {
      return "CallTracker:" + name;
    }
  }

  @VisibleForTesting
  public static class RegionStats {
    final String name;
    final MutableHistogram memstoreLoadHistogram;
    final MutableHistogram heapOccupancyHistogram;

    public RegionStats(DynamicMetricsRegistry registry, String name, String desc) {
      this.name = name;
      this.memstoreLoadHistogram = registry.newHistogram(String.format("%s_(%s)", MEMLOAD_BASE, name), desc);
      this.heapOccupancyHistogram = registry.newHistogram(String.format("%s_(%s)", HEAP_BASE, name), desc);
    }

    public void update(int memstoreLoad, int heapOccupancy) {
      this.memstoreLoadHistogram.add(memstoreLoad);
      this.heapOccupancyHistogram.add(heapOccupancy);
    }
  }

  /** meta cache mutators **/

  @Override
  public void incrMetaCacheHit() {
    this.metaCacheHits.incr();
  }

  @Override
  public void incrMetaCacheMiss() {
    this.metaCacheMisses.incr();
  }

  @Override
  public void incrMetaCacheNumClearServer() {
    this.metaCacheNumClearServers.incr();
  }

  @Override
  public void incrMetaCacheNumClearRegion() {
    this.metaCacheNumClearRegions.incr();
  }

  @Override
  public void incrHedgedReadOps() {
    this.hedgedReadOps.incr();
  }

  @Override
  public void incrHedgedReadWin() {
    this.hedgedReadWin.incr();
  }

  @Override
  public void incrNormalRunners() {
    this.runnerStats.incrNormalRunners();
  }

  @Override
  public void incrDelayRunners() {
    this.runnerStats.incrDelayRunners();
  }

  @Override
  public void incrCacheDroppingExceptions(Object exception) {
    String exceptionName = (exception == null ? UNKNOWN_EXCEPTION : exception.getClass().getSimpleName());
    exceptionName = String.format("%s_%s", CACHE_BASE, exceptionName);
    cacheDroppingExceptions.computeIfAbsent(exceptionName, newCacheCounter).incr();
  }

  @Override
  public void updateDelayInterval(long interval) {
    this.runnerStats.updateDelayInterval(interval);
  }

  /** rpc mutators **/

  @Override
  /** Report RPC context to metrics system. */
  public void updateRpc(MethodDescriptor method, Message param, CallStats stats) {
    long callsPerServer = stats.getConcurrentCallsPerServer();
    if (callsPerServer > 0) {
      concurrentCallsPerServerHistogram.add(callsPerServer);
    }

    // this implementation is tied directly to protobuf implementation details. would be better
    // if we could dispatch based on something static, ie, request Message type.
    if (method.getService() == ClientService.getDescriptor()) {
      switch (method.getIndex()) {
      case 0:
        assert "Get".equals(method.getName());
        getTracker.updateRpc(stats);
        return;
      case 1:
        assert "Mutate".equals(method.getName());
        final MutationType mutationType = ((MutateRequest) param).getMutation().getMutateType();
        switch (mutationType) {
        case APPEND:
          appendTracker.updateRpc(stats);
          return;
        case DELETE:
          deleteTracker.updateRpc(stats);
          return;
        case INCREMENT:
          incrementTracker.updateRpc(stats);
          return;
        case PUT:
          putTracker.updateRpc(stats);
          return;
        default:
          throw new RuntimeException("Unrecognized mutation type " + mutationType);
        }
      case 2:
        assert "Scan".equals(method.getName());
        scanTracker.updateRpc(stats);
        return;
      case 3:
        assert "BulkLoadHFile".equals(method.getName());
        // use generic implementation
        break;
      case 4:
        assert "PrepareBulkLoad".equals(method.getName());
        // use generic implementation
        break;
      case 5:
        assert "CleanupBulkLoad".equals(method.getName());
        // use generic implementation
        break;
      case 6:
        assert "ExecService".equals(method.getName());
        // use generic implementation
        break;
      case 7:
        assert "ExecRegionServerService".equals(method.getName());
        // use generic implementation
        break;
      case 8:
        assert "Multi".equals(method.getName());
        multiTracker.updateRpc(stats);
        return;
      default:
        throw new RuntimeException("Unrecognized ClientService RPC type " + method.getFullName());
      }
    }
  }

  @Override
  public void updateRegionStats(String regionPath, int memstoreLoad, int heapOccupancy) {
    regionStats.computeIfAbsent(regionPath, newRegionStat).update(memstoreLoad, heapOccupancy);
  }

  @Override
  public void addMetric(String name, MutableMetric metric) {
    registry.add(name, metric);
  }

  @Override
  public void setJMXContext(String context) {
    if (context != null) {
      DefaultMetricsSystem.instance().unregisterSource(this.metricsJmxContext);
      this.metricsJmxContext = context;
      this.registry.setContext(this.metricsJmxContext);
      DefaultMetricsSystem.instance().register(context, METRICS_DESCRIPTION, this);
    }
  }

}
