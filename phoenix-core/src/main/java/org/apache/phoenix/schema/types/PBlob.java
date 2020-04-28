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

import org.apache.phoenix.schema.SortOrder;

import java.io.InputStream;
import java.sql.Blob;
import java.sql.Types;

public class PBlob extends PVarbinary {

    public static final PBlob INSTANCE = new PBlob(new FileSystemBasedLobStore());

    private final LobStore store;

    private PBlob(LobStore store) {
        super("BLOB", Types.BLOB, InputStream.class, null, 70);
        this.store = store;
    }

    public Object toObject(byte[] bytes, int offset, int length, PDataType actualType,
            SortOrder sortOrder, Integer maxLength, Integer scale) {

        Object obj = super.toObject(bytes,offset,length,actualType,sortOrder,maxLength,scale);
        if(obj == null) {
            return null;
        }
        if(!(obj instanceof byte[])){
            throw new RuntimeException("Why wasn't it stored in hbase as varbinary");
        }

        byte[] locatorBytes = (byte[]) obj;
        LobMetadata lobLocator = getLobMetadataFromBinary(locatorBytes);

        try {
            InputStream stream = store.getLob(lobLocator.getLobLocator());
            PhoenixBlob result = new PhoenixBlob(lobLocator,stream);
            return result;
        } catch(Exception e) {
            throw new RuntimeException("Couldn't read from store locator " + lobLocator);
        }


    }

    LobMetadata getLobMetadataFromBinary(byte[] bytes) {
        return new LobMetadata(0,new String(bytes)); //TODO real metadata
    }

}
