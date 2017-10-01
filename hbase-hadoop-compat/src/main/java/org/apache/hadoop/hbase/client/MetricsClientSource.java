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

import org.apache.hadoop.hbase.metrics.BaseSource;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Message;
import org.apache.hadoop.metrics2.lib.MutableMetric;

/**
 * Client Metrics Source interface.
 * Manage the metrics associated with a particular hconnection.
 */
public interface MetricsClientSource extends BaseSource {

  String METRICS_NAME = "CLIENT";

  String METRICS_DESCRIPTION = "Metrics for a client's specific hconnection, tracking operation latencies, cache hits, and more.";

  /**
   * Default name of the connection managed by the Metrics Source.
   */
  String CONNECTION_NAME = "hconnection-default";

  /**
   * Regular application context of the metrics source.
   */
  String METRICS_CONTEXT = "client";

  /**
   * JMX context of the metrics source, reported to JMX server.
   * Change the metrics context on the metrics source before reporting metrics.
   */
  String METRICS_DEFAULT_JMX_CONTEXT = "Client,sub=" + CONNECTION_NAME;

  /***************** Metrics Metadata *****************/

  // Meta Cache
  String META_CACHE_HITS_KEY = "metaCacheHits";
  String META_CACHE_MISSES_KEY = "metaCacheMisses";
  String META_CACHE_CLEAR_REGIONS_KEY = "metaCacheNumClearRegion";
  String META_CACHE_CLEAR_SERVERS_KEY = "metaCacheNumClearServer";

  String META_CACHE_HITS_DESC = "";
  String META_CACHE_MISSES_DESC = "";
  String META_CACHE_CLEAR_REGIONS_DESC = "";
  String META_CACHE_CLEAR_SERVERS_DESC = "";

  // Hedged Read
  String HEDGED_READ_OPS_KEY = "hedgedReadOps";
  String HEDGED_READ_OPS_DESC = "number of hedged read ops that have occured";
  String HEDGED_READ_WIN_KEY = "hedgedReadWin";
  String HEDGED_READ_WIN_DESC = "number of hedged read ops that returned faster than original read";

  // Concurrent Server Calls
  String CONCURRENT_CALLS_KEY = "concurrentCallsPerServer";
  String CONCURRENT_CALLS_DESC = "";

  // Runner Stats
  String RUNNERS_NORMAL_COUNT_KEY = "normalRunnersCount";
  String RUNNERS_DELAY_COUNT_KEY = "delayRunnersCount";
  String RUNNERS_DELAY_HIST_KEY = "delayRunnersHist";
  String RUNNERS_NORMAL_COUNT_DESC = "";
  String RUNNERS_DELAY_COUNT_DESC = "";
  String RUNNERS_DELAY_HIST_DESC = "";

  // RPC Metrics
  final String CLIENT_SERVICE = "ClientService";
  final String RPC_DURATION_BASE = "rpcCallDurationMs";
  final String RPC_REQUEST_BASE = "rpcCallRequestSizeBytes";
  final String RPC_RESPONSE_BASE = "rpcCallResponseSizeBytes";

  // Other Bases
  final String MEMLOAD_BASE = "memstoreLoad";
  final String HEAP_BASE = "heapOccupancy";
  final String CACHE_BASE = "cacheDroppingExceptions";
  final String UNKNOWN_EXCEPTION = "UnknownException";

  /***************** Metrics Routines *****************/

  /** Increment the number of meta cache hits. */
  public void incrMetaCacheHit();

  /** Increment the number of meta cache misses. */
  public void incrMetaCacheMiss();

  /** Increment the number of meta cache drops for the requested RegionServer. */
  public void incrMetaCacheNumClearServer();

  /** Increment the number of meta cache drops for the requested individual region. */
  public void incrMetaCacheNumClearRegion();

  /** Increment the number of hedged read that have occurred. */
  public void incrHedgedReadOps();

  /** Increment the number of hedged read returned faster than the original read. */
  public void incrHedgedReadWin();

  /** Increment the count of normal runners. */
  public void incrNormalRunners();

  /** Increment the count of delay runners. */
  public void incrDelayRunners();

  /** Update delay interval of delay runners. */
  public void updateDelayInterval(long interval);

  /** Log a cache-dropping error or exception. */
  public void incrCacheDroppingExceptions(Object exception);

  /** Report the RPC context the to metrics system. */
  public void updateRpc(MethodDescriptor method, Message param, CallStats stats);

  /** Update the memstore and heap occupancy stats for a region. **/
  public void updateRegionStats(String regionPath, int memstoreLoad, int heapOccupancy);

  /***************** Tracking Routines *****************/

  /** Register a new Metric to be tracked by the MetricsSource. */
  public void addMetric(String name, MutableMetric metric);

  /** Remove a metric from the MetricsSource. */
  public void removeMetric(String name);

  /** Sets the jmxContext and updates it with the metrics system. */
  public void setJMXContext(String name);

  /** Shutdown the metrics source properly. */
  public void shutdown();
}
