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

import java.util.concurrent.ThreadPoolExecutor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.metrics.Interns;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Message;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.MutableMetric;

/**
 * This class is for maintaining the various connection statistics.
 * and publishing them through the metrics interfaces.
 *
 * This class manages its own MetricsClientSource and delegates calls to update metrics as necessary.
 * Instantiating this class implicitly creates and "starts" instances of these classes;
 * be sure to call {@link #shutdown()} to terminate the thread pools they allocate.
 */
@InterfaceAudience.Private
public class MetricsConnection implements StatisticTrackable {

  /** Set this key to {@code true} to enable metrics collection of client requests. */
  public static final String CLIENT_SIDE_METRICS_ENABLED_KEY = "hbase.client.metrics.enable";
  public static final String CLIENT_SIDE_POOLS_ENABLED_KEY = "hbase.client.metrics.pools.enable";
  public static final String CLIENT_SIDE_JMX_ENABLED_KEY = "hbase.client.jmx.enable";

  protected final Configuration config;
  protected final MetricsClientSource source;

  private final String POOL_EXECUTOR_ACTIVE_KEY = "executorPoolActiveThreads";
  private final String POOL_EXECUTOR_ACTIVE_DESC = "number of active threads in the executor pool";
  private final String POOL_META_ACTIVE_KEY = "metaPoolActiveThreads";
  private final String POOL_META_ACTIVE_DESC = "number of active threads in the meta pool";

  MetricsConnection(final ConnectionImplementation conn) {
    this(conn, new Configuration());
  }

  MetricsConnection(final ConnectionImplementation conn, Configuration config) {
    this.source = CompatibilityFactory.getInstance(MetricsClientSource.class);
    this.source.setJMXContext(conn.toString());
    this.config = config;

    if (this.config.getBoolean(CLIENT_SIDE_POOLS_ENABLED_KEY, false)) {
      this.source.addMetric(POOL_EXECUTOR_ACTIVE_KEY, new MutableMetric() {
        @Override
        public void snapshot(MetricsRecordBuilder builder, boolean all) {
          ThreadPoolExecutor batchPool = (ThreadPoolExecutor) conn.getCurrentBatchPool();
          double ratio = batchPool == null ? // active batch executor ratio
              0.0 : ((double) (batchPool.getActiveCount()) / (double) (batchPool.getMaximumPoolSize()));
          builder.addGauge(Interns.info(POOL_EXECUTOR_ACTIVE_KEY, POOL_EXECUTOR_ACTIVE_DESC), ratio);
        }
      });
      this.source.addMetric(POOL_META_ACTIVE_KEY, new MutableMetric() {
        @Override
        public void snapshot(MetricsRecordBuilder builder, boolean all) {
          ThreadPoolExecutor metaPool = (ThreadPoolExecutor) conn.getCurrentMetaLookupPool();
          double ratio = metaPool == null ? // active meta lookup ratio
              0.0 : ((double) (metaPool.getActiveCount()) / (double) (metaPool.getMaximumPoolSize()));
          builder.addGauge(Interns.info(POOL_META_ACTIVE_KEY, POOL_META_ACTIVE_DESC), ratio);
        }
      });
    }

  }

  public void shutdown() {
    source.shutdown();
  }

  /** Increment the number of meta cache hits. */
  public void incrMetaCacheHit() {
    source.incrMetaCacheHit();
  }

  /** Increment the number of meta cache misses. */
  public void incrMetaCacheMiss() {
    source.incrMetaCacheMiss();
  }

  /** Increment the number of meta cache drops for the requested RegionServer. */
  public void incrMetaCacheNumClearServer() {
    source.incrMetaCacheNumClearServer();
  }

  /** Increment the number of meta cache drops for the requested individual region. */
  public void incrMetaCacheNumClearRegion() {
    source.incrMetaCacheNumClearRegion();
  }

  /** Increment the count of normal runners. */
  public void incrNormalRunners() {
    source.incrNormalRunners();
  }

  /** Increment the count of delay runners. */
  public void incrDelayRunners() {
    source.incrDelayRunners();
  }

  /** Log a cache-dropping error or exception. */
  public void incrCacheDroppingExceptions(Object exception) {
    source.incrCacheDroppingExceptions(exception);
  }

  /** Update delay interval of delay runners. */
  public void updateDelayInterval(long interval) {
    source.updateDelayInterval(interval);
  }

  /** Report the RPC context to the metrics system. */
  public void updateRpc(MethodDescriptor method, Message param, CallStats stats) {
    source.updateRpc(method, param, stats);
  }

  /**
   * Publish new stats received about region from RegionServer.
   * Update the memstore and heap occupancy stats for a region.
   */
  public void updateServerStats(ServerName serverName, byte[] regionName, Object r) {
    if (!(r instanceof Result)) {
      return;
    }
    Result result = (Result) r;
    RegionLoadStats stats = result.getStats();
    if (stats == null) {
      return;
    }
    updateRegionStats(serverName, regionName, stats);
  }

  @Override
  public void updateRegionStats(ServerName serverName, byte[] regionName, RegionLoadStats stats) {
    String regionPath = formatRegionPath(serverName, regionName);
    source.updateRegionStats(regionPath, stats.getMemstoreLoad(), stats.getHeapOccupancy());
  }

  /** Formats a region path string. */
  protected static String formatRegionPath(ServerName serverName, byte[] regionName) {
    return String.format("%s.%s", serverName.getServerName(), Bytes.toStringBinary(regionName));
  }
}
