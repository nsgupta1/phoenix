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

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;

/**
 * Test Implementation to unblock others writes to /tmp/
 */
public class FileSystemBasedLobStore implements LobStore {

    private static final String ROOT_DIRECTORY = "/tmp/";


    @Override
    public String putLob(InputStream lobStream) throws LobStoreException {
        String lobLocator = RandomStringUtils.randomAlphanumeric(15); //For now use lobLocator
        try (FileOutputStream out = new FileOutputStream(getPathFromLocator(lobLocator)) ){
            //Write the inputstream to our file
            IOUtils.copy(lobStream,out);
            //TODO: close lobStream?
            return lobLocator;
        } catch(Exception e) {
            throw new LobStoreException(e);
        }
    }

    @VisibleForTesting
    String getPathFromLocator(String lobLocator) {
        return ROOT_DIRECTORY + lobLocator;
    }

    @Override
    public InputStream getLob(String lobLocator) throws LobStoreException {
        try {
            return new FileInputStream(getPathFromLocator(lobLocator));
        } catch (Exception e) {
            throw new LobStoreException(e);
        }
    }
}
