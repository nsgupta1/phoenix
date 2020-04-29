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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.io.InputStream;


public class HDFSBasedLobStore implements LobStore {

    public static final Logger LOGGER =
            LoggerFactory.getLogger(HDFSBasedLobStore.class);
    private static final Path ROOT_DIRECTORY = new Path("/user/hdfs/lob/hdfs/");
    private static HDFSBasedLobStore INSTANCE = null;
    private String hdfsuri = "hdfs://localhost:9000";

    public synchronized static HDFSBasedLobStore getInstance() {
        if(INSTANCE == null){
            INSTANCE = new HDFSBasedLobStore();
        }
        return INSTANCE;
    }

    public HDFSBasedLobStore() { }

    public HDFSBasedLobStore(String hdfsuri) {
        this.hdfsuri = hdfsuri;
    }

    private FileSystem initializeHDFS() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsuri);
        //==== Create folder if not exists
        FileSystem fs = FileSystem.get(URI.create(hdfsuri), conf);
        if(!fs.exists(ROOT_DIRECTORY)) {
            fs.mkdirs(ROOT_DIRECTORY);
            LOGGER.info("Path "+ ROOT_DIRECTORY +" created.");
        }
        return fs;
    }

    @Override
    public String putLob(InputStream lobStream) throws LobStoreException {
        String lobLocator = RandomStringUtils.randomAlphanumeric(15); //For now use lobLocator
        Path hdfswritepath = new Path(ROOT_DIRECTORY + "/" + lobLocator);
        try (FileSystem fs = initializeHDFS();
             OutputStream os = fs.create(hdfswritepath)){
            IOUtils.copy(lobStream,os);
            return lobLocator;
        } catch (IOException e) {
            throw new LobStoreException(e);
        }
    }

    @VisibleForTesting
    String getPathFromLocator(String lobLocator) {
        return (ROOT_DIRECTORY + "/" + lobLocator);
    }

    @Override
    public InputStream getLob(String lobLocator) throws LobStoreException {
        try(FileSystem fs = initializeHDFS();
            InputStream in= fs.open(new Path (getPathFromLocator(lobLocator)))) {
            return IOUtils.toBufferedInputStream(in);
        } catch (Exception e) {
            throw new LobStoreException(e);
        }
    }
}
