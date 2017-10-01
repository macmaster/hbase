/**
 *
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

/** A container class for collecting details about the RPC call as it percolates. */
public class CallStats {
  private long requestSizeBytes = 0, responseSizeBytes = 0;
  private long startTime = 0, callTimeMs = 0;
  private long concurrentCallsPerServer = 0;

  public long getRequestSizeBytes() {
    return requestSizeBytes;
  }

  public void setRequestSizeBytes(long requestSizeBytes) {
    this.requestSizeBytes = requestSizeBytes;
  }

  public long getResponseSizeBytes() {
    return responseSizeBytes;
  }

  public void setResponseSizeBytes(long responseSizeBytes) {
    this.responseSizeBytes = responseSizeBytes;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getCallTimeMs() {
    return callTimeMs;
  }

  public void setCallTimeMs(long callTimeMs) {
    this.callTimeMs = callTimeMs;
  }

  public long getConcurrentCallsPerServer() {
    return concurrentCallsPerServer;
  }

  public void setConcurrentCallsPerServer(long callsPerServer) {
    this.concurrentCallsPerServer = callsPerServer;
  }

  /** Produce an instance of {@link CallStats} for clients to attach to RPCs. */
  public static CallStats newCallStats() {
    // instance pool to reduce GC?
    return new CallStats();
  }
}
