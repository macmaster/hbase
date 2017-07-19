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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MetricsTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import static org.junit.Assert.assertEquals;

@Category({ ClientTests.class, MetricsTests.class, SmallTests.class })
public class TestMetricsConnection {

  private static final ExecutorService BATCH_POOL = Executors.newFixedThreadPool(2);

  private MetricsConnection metricsConnection;
  private ConnectionImplementation mockedConnection;
  private Configuration config;

  @Before
  public void setUp() {
    config = new Configuration();
    config.setBoolean(MetricsConnection.CLIENT_SIDE_POOLS_ENABLED_KEY, true);
    config.setBoolean(MetricsConnection.CLIENT_SIDE_JMX_ENABLED_KEY, true);

    mockedConnection = Mockito.mock(ConnectionImplementation.class);
    Mockito.when(mockedConnection.toString()).thenReturn("mocked-connection");
    Mockito.when(mockedConnection.getCurrentBatchPool()).thenReturn(BATCH_POOL);
    metricsConnection = new MetricsConnection(mockedConnection, config);
  }

  @After
  public void tearDown() {
    metricsConnection.shutdown();
  }

  @Test
  public void testRpcMetricsCount() throws IOException {
    final byte[] foo = Bytes.toBytes("foo");
    final RegionSpecifier region = RegionSpecifier.newBuilder().setValue(ByteString.EMPTY)
        .setType(RegionSpecifierType.REGION_NAME).build();

    // populate test RPC counters.
    final int count = 5;
    for (int i = 0; i < count; i++) {
      metricsConnection.updateRpc(ClientService.getDescriptor().findMethodByName("Get"),
          GetRequest.getDefaultInstance(), CallStats.newCallStats());
      metricsConnection.updateRpc(ClientService.getDescriptor().findMethodByName("Scan"),
          ScanRequest.getDefaultInstance(), CallStats.newCallStats());
      metricsConnection.updateRpc(ClientService.getDescriptor().findMethodByName("Multi"),
          MultiRequest.getDefaultInstance(), CallStats.newCallStats());
      metricsConnection.updateRpc(ClientService.getDescriptor().findMethodByName("Mutate"),
          MutateRequest.newBuilder()
              .setMutation(ProtobufUtil.toMutation(MutationType.APPEND, new Append(foo)))
              .setRegion(region).build(),
          CallStats.newCallStats());
      metricsConnection.updateRpc(ClientService.getDescriptor().findMethodByName("Mutate"),
          MutateRequest.newBuilder()
              .setMutation(ProtobufUtil.toMutation(MutationType.DELETE, new Delete(foo)))
              .setRegion(region).build(),
          CallStats.newCallStats());
      metricsConnection.updateRpc(ClientService.getDescriptor().findMethodByName("Mutate"),
          MutateRequest.newBuilder()
              .setMutation(ProtobufUtil.toMutation(MutationType.INCREMENT, new Increment(foo)))
              .setRegion(region).build(),
          CallStats.newCallStats());
      metricsConnection.updateRpc(ClientService.getDescriptor().findMethodByName("Mutate"),
          MutateRequest.newBuilder()
              .setMutation(ProtobufUtil.toMutation(MutationType.PUT, new Put(foo)))
              .setRegion(region).build(),
          CallStats.newCallStats());
    }

    MetricsClientSourceImpl source = (MetricsClientSourceImpl) metricsConnection.source;
    for (MetricsClientSourceImpl.CallTracker t : new MetricsClientSourceImpl.CallTracker[] {
        source.getTracker, source.scanTracker, source.multiTracker, source.appendTracker,
        source.deleteTracker, source.incrementTracker, source.putTracker }) {
      assertEquals(count, t.callTimer.getSnapshot().getCount());
      assertEquals(count, t.reqHist.getSnapshot().getCount());
      assertEquals(count, t.respHist.getSnapshot().getCount());
    }

    // RatioGauge executorMetrics = (RatioGauge)
    // metricsConnection.getMetricRegistry().getMetrics()
    // .get(metricsConnection.getExecutorPoolName());
    // RatioGauge metaMetrics = (RatioGauge)
    // metricsConnection.getMetricRegistry().getMetrics()
    // .get(metricsConnection.getMetaPoolName());
    // assertEquals(Ratio.of(0, 3).getValue(), executorMetrics.getValue(), 0);
    // assertEquals(Double.NaN, metaMetrics.getValue(), 0);
  }
}
