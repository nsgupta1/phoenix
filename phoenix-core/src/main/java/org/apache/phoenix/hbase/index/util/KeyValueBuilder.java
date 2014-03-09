/*
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
package org.apache.phoenix.hbase.index.util;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/**
 * Build {@link KeyValue} in an efficient way
 */
public abstract class KeyValueBuilder {
    public static final String CLIENT_KEY_VALUE_MIN_VERSION = "0.94.14";
    private static final int CUSTOM_KEY_VALUE_MIN_VERSION = VersionUtil.encodeVersion("0.94.14");

    /**
     * Helper method for a {@link KeyValueBuilder} that catches an IOException from a {@link Put}
     * when adding a {@link KeyValue} generated by the KeyValueBuilder.
     * @throws RuntimeException if there is an IOException thrown from the underlying {@link Put}
     */
    @SuppressWarnings("javadoc")
    public static void addQuietly(Put put, KeyValueBuilder builder, KeyValue kv) {
        try {
            put.add(kv);
        } catch (IOException e) {
            throw new RuntimeException("KeyValue Builder " + builder + " created an invalid kv: "
                    + kv + "!");
        }
    }

    /**
     * Helper method for a {@link KeyValueBuilder} that catches an IOException from a {@link Put}
     * when adding a {@link KeyValue} generated by the KeyValueBuilder.
     * @throws RuntimeException if there is an IOException thrown from the underlying {@link Put}
     */
    @SuppressWarnings("javadoc")
    public static void deleteQuietly(Delete delete, KeyValueBuilder builder, KeyValue kv) {
        try {
            delete.addDeleteMarker(kv);
        } catch (IOException e) {
            throw new RuntimeException("KeyValue Builder " + builder + " created an invalid kv: "
                    + kv + "!");
        }
    }

    public static KeyValueBuilder get(String hbaseVersion) {
        int version = VersionUtil.encodeVersion(hbaseVersion);
        if (version >= CUSTOM_KEY_VALUE_MIN_VERSION) {
            return ClientKeyValueBuilder.INSTANCE;
        }
        return GenericKeyValueBuilder.INSTANCE;
    }

  public KeyValue buildPut(ImmutableBytesWritable row, ImmutableBytesWritable family,
      ImmutableBytesWritable qualifier, ImmutableBytesWritable value) {
    return buildPut(row, family, qualifier, HConstants.LATEST_TIMESTAMP, value);
  }

  public abstract KeyValue buildPut(ImmutableBytesWritable row, ImmutableBytesWritable family,
      ImmutableBytesWritable qualifier, long ts, ImmutableBytesWritable value);

  public KeyValue buildDeleteFamily(ImmutableBytesWritable row, ImmutableBytesWritable family,
            ImmutableBytesWritable qualifier) {
        return buildDeleteFamily(row, family, qualifier, HConstants.LATEST_TIMESTAMP);
  }

  public abstract KeyValue buildDeleteFamily(ImmutableBytesWritable row,
            ImmutableBytesWritable family, ImmutableBytesWritable qualifier, long ts);

  public KeyValue buildDeleteColumns(ImmutableBytesWritable row, ImmutableBytesWritable family,
            ImmutableBytesWritable qualifier) {
        return buildDeleteColumns(row, family, qualifier, HConstants.LATEST_TIMESTAMP);
  }

  public abstract KeyValue buildDeleteColumns(ImmutableBytesWritable row,
            ImmutableBytesWritable family, ImmutableBytesWritable qualifier, long ts);

  public KeyValue buildDeleteColumn(ImmutableBytesWritable row, ImmutableBytesWritable family,
            ImmutableBytesWritable qualifier) {
        return buildDeleteColumn(row, family, qualifier, HConstants.LATEST_TIMESTAMP);
  }

  public abstract KeyValue buildDeleteColumn(ImmutableBytesWritable row,
            ImmutableBytesWritable family, ImmutableBytesWritable qualifier, long ts);

  /**
   * Compare the qualifier based on the type of keyvalue. Assumes that the {@link KeyValue} passed
   * in was generated by the {@link KeyValueBuilder}
   * @param kv to compare against
   * @param key to compare
   * @param offset in the passed key
   * @param length length of the key from the offset to check
   * @return the byte difference between the passed keyvalue's qualifier and the passed key
   */
  public abstract int compareQualifier(Cell kv, byte[] key, int offset, int length);

  public abstract int compareFamily(Cell kv, byte[] key, int offset, int length);
  public abstract int compareRow(Cell kv, byte[] row, int offset, int length);
  /**
   * @param kv to read
   * @param ptr set with the value from the {@link KeyValue}
   */
  public abstract void getValueAsPtr(Cell kv, ImmutableBytesWritable ptr);
  
  public abstract KVComparator getKeyValueComparator();
  
  public abstract List<Mutation> cloneIfNecessary(List<Mutation> mutations);
}
