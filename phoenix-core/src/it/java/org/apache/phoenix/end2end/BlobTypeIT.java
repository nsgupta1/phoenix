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

import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

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

}
