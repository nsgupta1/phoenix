package org.apache.phoenix.end2end;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.PhckSystemTableValidator;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Properties;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.*;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

@Category(NeedsOwnMiniClusterTest.class)
public class PHCKSystemTableValidatorIT extends ParallelStatsEnabledIT {

    @Test
    public void testDisabledTable() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));
        Connection connection = DriverManager.getConnection(getUrl(), props);
        PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);
        Admin admin = phoenixConnection.getQueryServices().getAdmin();
        admin.disableTable(TableName.valueOf(SYSTEM_CATALOG_SCHEMA+ "." +SYSTEM_FUNCTION_TABLE));
        runPhckSystemTableValidator(true,true,null);
    }
    @Test
    public void testDroppedTable() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));
        Connection connection = DriverManager.getConnection(getUrl(), props);
        PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);
        Admin admin = phoenixConnection.getQueryServices().getAdmin();
        admin.disableTable(TableName.valueOf(SYSTEM_CATALOG_SCHEMA+ "." +SYSTEM_FUNCTION_TABLE));
        admin.deleteTable(TableName.valueOf(SYSTEM_CATALOG_SCHEMA+ "." +SYSTEM_FUNCTION_TABLE));
        runPhckSystemTableValidator(true,true,null);
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

    public static int runPhckSystemTableValidator(boolean getCorruptedViewCount, boolean outputPath, PhckSystemTableValidator tool)
            throws Exception {
        if (tool == null) {
            tool = new PhckSystemTableValidator();
        }
        Configuration conf = new Configuration(getUtility().getConfiguration());
        tool.setConf(conf);
        final String[] cmdArgs =
                getArgValues(getCorruptedViewCount, outputPath);
        return tool.run(cmdArgs);
    }
}
