package org.apache.phoenix.schema.types;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class S3BasedLobStoreTest {

    @Test
    public void testLobStore() throws LobStoreException, IOException {
        S3BasedLobStore store = new S3BasedLobStore();
        String testInput = "This is a test of store.";
        String lobLocator = store.putLob(IOUtils.toInputStream(testInput));
        InputStream reader = store.getLob(lobLocator);
        String out = IOUtils.toString(reader, "UTF-8");
        reader.close();
        assertEquals(out, testInput);
    }

    @Test
    public void testFileNotFoundException() {
        S3BasedLobStore store = new S3BasedLobStore();
        String fileName = "testFile";
        try (InputStream reader = store.getLob(fileName)) {
            IOUtils.toString(reader, "UTF-8");
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("The specified key does not exist."));
        }
    }
}
