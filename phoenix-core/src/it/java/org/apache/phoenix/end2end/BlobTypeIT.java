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
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.types.FileSystemBasedLobStore;
import org.apache.phoenix.schema.types.LobStoreException;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BlobTypeIT extends BaseUniqueNamesOwnClusterIT {

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        setUpTestDriver(ReadOnlyProps.EMPTY_PROPS);
    }

    @Test
    public void testBlobUpsertFromImageFile() throws SQLException, FileNotFoundException {
        String fName = "/images/dali.jpeg";
        InputStream input = BlobTypeIT.class.getResourceAsStream(fName);
        String tableName = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl());
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE " + tableName + " (id VARCHAR(255) not null primary key, "
                + "image BLOB)");
        String upsertQuery = "UPSERT INTO " + tableName + " VALUES(?, ?)";
        PreparedStatement prepStmt = conn.prepareStatement(upsertQuery);
        prepStmt.setString(1, "ID1");
        prepStmt.setBlob(2, input);
        prepStmt.execute();
    }

    @Test
    public void testBlobDownload() throws SQLException, IOException, LobStoreException {
        String fName = "/images/dali.jpeg";
        InputStream input = BlobTypeIT.class.getResourceAsStream(fName);
        String tableName = generateUniqueName();
        Connection conn = DriverManager.getConnection(getUrl());
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE " + tableName + " (id VARCHAR(255) not null primary key, "
                + "image BLOB)");

        //put a row in so we get the default column family but no lob

        conn.createStatement().executeUpdate("UPSERT INTO " + tableName + " VALUES('a')");
        conn.commit();

        //Faking the upload with HBase client
        {
            //get the column qualifier
            Statement stmt2 = conn.createStatement();
            ResultSet rs2 = stmt2.executeQuery("SELECT COLUMN_NAME,COLUMN_QUALIFIER FROM SYSTEM.CATALOG WHERE COLUMN_NAME='IMAGE' AND TABLE_NAME='" + tableName + "'");
            rs2.next();
            byte[] qualifier = rs2.getBytes(2);

            //upload a thing
            FileSystemBasedLobStore lobStore = new FileSystemBasedLobStore();

            InputStream inputStream = BlobTypeIT.class.getResourceAsStream(fName);
            String path = lobStore.putLob(inputStream);

            PhoenixConnection phoenixConnection = (PhoenixConnection) conn;
            HTableInterface table =
                    phoenixConnection.getQueryServices().getTable(tableName.getBytes());

            String row = "abc";
            Put myPut = new Put(row.getBytes());
            //byte [] family, byte [] qualifier, byte [] value
            byte[] family = { '0' };
            String blobColumnName = "image";
            byte[] value = path.getBytes(); //Put the file path in hbase
            myPut.addColumn(family, qualifier, value);
            myPut.addColumn(family,new byte[] {}, new byte[] {});
            table.put(myPut);


/*
            {
                Scan scan = new Scan();
                ResultScanner scanner = table.getScanner(scan);


                for(Result result : scanner) {
                    for(Cell cell : result.listCells()) {
                        System.out.println("*****************");
                        System.out.println(cell);
                        System.out.println(Arrays.toString(cell.getQualifierArray()));
                        byte[] data = cell.getValueArray();
                        System.out.println(Arrays.toString(data));
                        System.out.println(new String(data));
                    }
                }

            }

            TestUtil.printResultSet(((PhoenixConnection) conn).createStatement().executeQuery("SELECT * FROM " + tableName));
*/

        }


        TestUtil.printResultSet(stmt.executeQuery("SELECT image FROM " + tableName ));

        stmt = conn.createStatement();

        ResultSet rs = stmt.executeQuery("SELECT image FROM " + tableName + " WHERE id='abc'");
        assertTrue(rs.next());
        Blob blob = rs.getBlob(1);
        InputStream blobStream = blob.getBinaryStream();
        assertTrue(IOUtils.contentEquals(blobStream,BlobTypeIT.class.getResourceAsStream(fName)));
    }
}
