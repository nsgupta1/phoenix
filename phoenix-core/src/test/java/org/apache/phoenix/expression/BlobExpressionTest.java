package org.apache.phoenix.expression;

import org.apache.phoenix.end2end.BlobTypeIT;
import org.apache.phoenix.schema.SortOrder;
import org.junit.Test;

import java.io.InputStream;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class BlobExpressionTest {

    @Test
    public void getBlobExpression() {
        String fName = "/images/dali.jpeg";
        InputStream input = BlobTypeIT.class.getResourceAsStream(fName);
        BlobExpression blob = new BlobExpression(input, SortOrder.ASC, Determinism.ALWAYS);
        assertNotNull(blob);
    }

    @Test
    public void testBlobMetaDataSerDes() throws Exception {
        BlobExpression.BlobMetaData blobMetaData = new BlobExpression.BlobMetaData(true,
                "hdfs:///hostname:/tmp/file.png", 1000L,
                BlobExpression.SUPPORTED_FORMATS.HDFS, UUID.randomUUID(), 100L);

        byte[] serializedBytes = BlobExpression.BlobMetaData.serializeBlobMetaData(blobMetaData);
        BlobExpression.BlobMetaData deserObj =
                BlobExpression.BlobMetaData.deserializeBlobMetaData(serializedBytes);
        assertEquals(blobMetaData, deserObj);
    }
}
