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

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Scanner;

import static org.junit.Assert.*;

public class FileSystemBasedLobStoreTest {

    @Test
    public void putLob() throws LobStoreException, FileNotFoundException {
        FileSystemBasedLobStore store = new FileSystemBasedLobStore();

        String testInput = "This is a test of store.";

        String path = store.getPathFromLocator(store.putLob(IOUtils.toInputStream(testInput)));
        FileInputStream fileInputStream = new FileInputStream(path);

        Scanner myReader = new Scanner(fileInputStream);

        assertTrue(myReader.hasNext());
        String output = myReader.nextLine();
        assertEquals(testInput,output);
        assertFalse(myReader.hasNext());

        //TODO: Clean up test files
    }

    @Test
    public void getLob() throws IOException, LobStoreException {
        FileSystemBasedLobStore store = new FileSystemBasedLobStore();

        String testInput = "This is a test of store.";
        String fileName = "testFile";
        BufferedWriter writer = new BufferedWriter(new FileWriter(store.getPathFromLocator(fileName)));
        writer.write(testInput);
        writer.close();

        InputStream output = store.getLob(fileName);

        List<String> lines = IOUtils.readLines(output);
        assertEquals(1,lines.size());
        assertEquals(testInput,lines.get(0));

    }
}