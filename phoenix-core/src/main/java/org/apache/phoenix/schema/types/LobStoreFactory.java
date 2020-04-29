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

public class LobStoreFactory {

    public enum SUPPORTED_FORMATS {
        S3, HDFS, LOCAL_FS
    };

    public static LobStore getLobStore(String lobStoreImpl) {
        if (SUPPORTED_FORMATS.S3.name().equalsIgnoreCase(lobStoreImpl)) {
            return new S3BasedLobStore();
        } else if (SUPPORTED_FORMATS.HDFS.name().equalsIgnoreCase(lobStoreImpl)) {
            return new HDFSBasedLobStore();
        } else { // Default
            return new FileSystemBasedLobStore();
        }
    }

    public static boolean isOnDisk(LobStore lobStore) {
        return lobStore instanceof FileSystemBasedLobStore;
    }

    public static SUPPORTED_FORMATS getFormat(LobStore lobStore) {
        if (lobStore instanceof S3BasedLobStore) {
            return SUPPORTED_FORMATS.S3;
        } else if (lobStore instanceof HDFSBasedLobStore) {
            return SUPPORTED_FORMATS.HDFS;
        } else { // Default
            return SUPPORTED_FORMATS.LOCAL_FS;
        }
    }
}
