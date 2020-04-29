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

package org.apache.phoenix.schema.types;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import java.io.IOException;
import java.io.InputStream;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HDFSBasedLobStoreTest {

    @Test
    public void testLobStore() throws LobStoreException, IOException {
        HDFSBasedLobStore store = new HDFSBasedLobStore();
        String testInput = "This is a test of store.";
        String lobLocator = store.putLob(IOUtils.toInputStream(testInput));
        InputStream reader = store.getLob(lobLocator);
        String out= IOUtils.toString(reader, "UTF-8");
        reader.close();
        assertEquals(out,testInput);
    }

    @Test
    public void testFileNotFoundException() {
        HDFSBasedLobStore store = new HDFSBasedLobStore();
        String fileName = "testFile";
        try(InputStream reader = store.getLob(fileName)){
            IOUtils.toString(reader, "UTF-8");
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("File does not exist: /user/hdfs/lob/hdfs/testFile"));
        }
    }
}
