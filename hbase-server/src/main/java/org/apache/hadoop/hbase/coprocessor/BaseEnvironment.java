/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTableWrapper;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.yetus.audience.InterfaceAudience;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Encapsulation of the environment of each coprocessor
 */
@InterfaceAudience.Private
public class BaseEnvironment<C extends Coprocessor> implements CoprocessorEnvironment<C> {
  private static final Log LOG = LogFactory.getLog(BaseEnvironment.class);

  /** The coprocessor */
  public C impl;
  /** Chaining priority */
  protected int priority = Coprocessor.PRIORITY_USER;
  /** Current coprocessor state */
  Coprocessor.State state = Coprocessor.State.UNINSTALLED;
  /** Accounting for tables opened by the coprocessor */
  protected List<Table> openTables =
    Collections.synchronizedList(new ArrayList<Table>());
  private int seq;
  private Configuration conf;
  private ClassLoader classLoader;

  /**
   * Constructor
   * @param impl the coprocessor instance
   * @param priority chaining priority
   */
  public BaseEnvironment(final C impl, final int priority,
      final int seq, final Configuration conf) {
    this.impl = impl;
    this.classLoader = impl.getClass().getClassLoader();
    this.priority = priority;
    this.state = Coprocessor.State.INSTALLED;
    this.seq = seq;
    this.conf = conf;
  }

  /** Initialize the environment */
  @Override
  public void startup() throws IOException {
    if (state == Coprocessor.State.INSTALLED ||
        state == Coprocessor.State.STOPPED) {
      state = Coprocessor.State.STARTING;
      Thread currentThread = Thread.currentThread();
      ClassLoader hostClassLoader = currentThread.getContextClassLoader();
      try {
        currentThread.setContextClassLoader(this.getClassLoader());
        impl.start(this);
        state = Coprocessor.State.ACTIVE;
      } finally {
        currentThread.setContextClassLoader(hostClassLoader);
      }
    } else {
      LOG.warn("Not starting coprocessor " + impl.getClass().getName() +
          " because not inactive (state=" + state.toString() + ")");
    }
  }

  /** Clean up the environment */
  @Override
  public void shutdown() {
    if (state == Coprocessor.State.ACTIVE) {
      state = Coprocessor.State.STOPPING;
      Thread currentThread = Thread.currentThread();
      ClassLoader hostClassLoader = currentThread.getContextClassLoader();
      try {
        currentThread.setContextClassLoader(this.getClassLoader());
        impl.stop(this);
        state = Coprocessor.State.STOPPED;
      } catch (IOException ioe) {
        LOG.error("Error stopping coprocessor "+impl.getClass().getName(), ioe);
      } finally {
        currentThread.setContextClassLoader(hostClassLoader);
      }
    } else {
      LOG.warn("Not stopping coprocessor "+impl.getClass().getName()+
          " because not active (state="+state.toString()+")");
    }
    synchronized (openTables) {
      // clean up any table references
      for (Table table: openTables) {
        try {
          ((HTableWrapper)table).internalClose();
        } catch (IOException e) {
          // nothing can be done here
          LOG.warn("Failed to close " +
              table.getName(), e);
        }
      }
    }
  }

  @Override
  public C getInstance() {
    return impl;
  }

  @Override
  public ClassLoader getClassLoader() {
    return classLoader;
  }

  @Override
  public int getPriority() {
    return priority;
  }

  @Override
  public int getLoadSequence() {
    return seq;
  }

  /** @return the coprocessor environment version */
  @Override
  public int getVersion() {
    return Coprocessor.VERSION;
  }

  /** @return the HBase release */
  @Override
  public String getHBaseVersion() {
    return VersionInfo.getVersion();
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  /**
   * Open a table from within the Coprocessor environment
   * @param tableName the table name
   * @return an interface for manipulating the table
   * @exception IOException Exception
   */
  @Override
  public Table getTable(TableName tableName) throws IOException {
    return this.getTable(tableName, null);
  }

  /**
   * Open a table from within the Coprocessor environment
   * @param tableName the table name
   * @return an interface for manipulating the table
   * @exception IOException Exception
   */
  @Override
  public Table getTable(TableName tableName, ExecutorService pool) throws IOException {
    return HTableWrapper.createWrapper(openTables, tableName, this, pool);
  }
}
