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
package org.apache.phoenix.end2end;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.mapreduce.PhckNonSystemTableValidator;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;

@Category(NeedsOwnMiniClusterTest.class)
public class PhckNonSystemTableValidatorIT extends ParallelStatsEnabledIT {
    String CREATE_SCHEM_QUERY = "CREATE SCHEMA %s";
    String CREATE_TABLE_QUERY = "CREATE TABLE %s (A BIGINT PRIMARY KEY, B BIGINT, C BIGINT)";
    String CREATE_VIEW_QUERY = "CREATE VIEW %s AS SELECT * FROM %s";
    String CREATE_INDEX_QUERY = "CREATE INDEX %s ON %s (B) INCLUDE(A,C)";
    String CREATE_MULTI_TENANT_TABLE_QUERY = "CREATE TABLE %s (TENANT_ID VARCHAR NOT NULL, " +
            "A VARCHAR NOT NULL, B BIGINT, CONSTRAINT pk PRIMARY KEY(TENANT_ID, A)) MULTI_TENANT=true";

    @Test
    public void test() throws Exception {
        String schema1 = generateUniqueName();
        String schema2 = generateUniqueName();
        String schema3 = generateUniqueName();
        String tableName1 = generateUniqueName();
        String indexName1 = generateUniqueName();
        String indexName2 = generateUniqueName();

        String viewName1 = generateUniqueName();
        String viewName2 = generateUniqueName();
        String viewName3 = generateUniqueName();

        String viewOnIndexName1 = generateUniqueName();

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(String.format(CREATE_SCHEM_QUERY, schema1));
        conn.createStatement().execute(String.format(CREATE_SCHEM_QUERY, schema2));
        conn.createStatement().execute(String.format(CREATE_SCHEM_QUERY, schema3));

        String baseTableName1 = SchemaUtil.getTableName(schema1, tableName1);
        String baseTableName2 = SchemaUtil.getTableName(schema2, tableName1);
        String baseTableName3 = SchemaUtil.getTableName(schema3, tableName1);
        conn.createStatement().execute(String.format(CREATE_TABLE_QUERY, baseTableName1));
        conn.createStatement().execute(String.format(CREATE_TABLE_QUERY, baseTableName2));
        conn.createStatement().execute(String.format(CREATE_TABLE_QUERY, baseTableName3));

        //happy path
        String schema1ViewName1 = SchemaUtil.getTableName(schema1, viewName1);
        String schema1ViewName2 = SchemaUtil.getTableName(schema1, viewName2);
        String schema1ViewName3 = SchemaUtil.getTableName(schema1, viewName3);
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, schema1ViewName1, baseTableName1));
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, schema1ViewName2, baseTableName1));
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, schema1ViewName3, schema1ViewName2));

        String schema2ViewName1 = SchemaUtil.getTableName(schema2, viewName1);
        String schema2ViewName2 = SchemaUtil.getTableName(schema2, viewName2);
        String schema2ViewName3 = SchemaUtil.getTableName(schema2, viewName3);
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, schema2ViewName1, baseTableName2));
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, schema2ViewName2, baseTableName2));
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, schema2ViewName3, schema2ViewName2));

        String schema3ViewName1 = SchemaUtil.getTableName(schema3, viewName1);
        String schema3ViewName2 = SchemaUtil.getTableName(schema3, viewName2);
        String schema3ViewName3 = SchemaUtil.getTableName(schema3, viewName3);
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, schema3ViewName1, baseTableName3));
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, schema3ViewName2, baseTableName3));
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, schema3ViewName3, schema3ViewName2));

        String schema1IndexName1 = SchemaUtil.getTableName(schema1, indexName1);
        String schema2IndexName1 = SchemaUtil.getTableName(schema2, indexName1);
        String schema3IndexName1 = SchemaUtil.getTableName(schema3, indexName1);
        conn.createStatement().execute(String.format(CREATE_INDEX_QUERY, indexName1, baseTableName1));
        conn.createStatement().execute(String.format(CREATE_INDEX_QUERY, indexName1, baseTableName2));
        conn.createStatement().execute(String.format(CREATE_INDEX_QUERY, indexName1, baseTableName3));

        String schema1ViewOnIndexName1 = SchemaUtil.getTableName(schema1, viewOnIndexName1);
        String schema2ViewOnIndexName1 = SchemaUtil.getTableName(schema2, viewOnIndexName1);
        String schema3ViewOnIndexName1 = SchemaUtil.getTableName(schema3, viewOnIndexName1);
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, schema1ViewOnIndexName1 ,schema1IndexName1));
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, schema2ViewOnIndexName1, schema2IndexName1));
        conn.createStatement().execute(String.format(CREATE_VIEW_QUERY, schema3ViewOnIndexName1, schema3IndexName1));


        String schema1IndexOnViewName1 = SchemaUtil.getTableName(schema1, indexName2);
        String schema2IndexOnViewName1 = SchemaUtil.getTableName(schema2, indexName2);
        String schema3IndexOnViewName1 = SchemaUtil.getTableName(schema3, indexName2);
        conn.createStatement().execute(String.format(CREATE_INDEX_QUERY, indexName2 ,schema1ViewName1));
        conn.createStatement().execute(String.format(CREATE_INDEX_QUERY, indexName2, schema2ViewName1));
        conn.createStatement().execute(String.format(CREATE_INDEX_QUERY, indexName2, schema3ViewName1));

        PhckNonSystemTableValidator tool = new PhckNonSystemTableValidator();
        runPhckNonSystemTableValidator(true, false, tool);
        assertEquals(0, tool.getInvalidRowSet().size());
        assertEquals(0, tool.getInvalidTableSet().size());
//        assertEquals(0, runPhckNonSystemTableValidator(true, false, null));

        //sad path

    }

    private static String[] getArgValues(boolean invalidCount, boolean outputPath) {
        final List<String> args = Lists.newArrayList();
        if (outputPath) {
            args.add("-op");
            args.add("/tmp");
        }
        if (invalidCount) {
            args.add("-c");
        }

        return args.toArray(new String[0]);
    }

    public static int runPhckNonSystemTableValidator(boolean getCorruptedViewCount, boolean outputPath, PhckNonSystemTableValidator tool)
            throws Exception {
        if (tool == null) {
            tool = new PhckNonSystemTableValidator();
        }
        Configuration conf = new Configuration(getUtility().getConfiguration());
        tool.setConf(conf);
        final String[] cmdArgs =
                getArgValues(getCorruptedViewCount, outputPath);
        return tool.run(cmdArgs);
    }
}
