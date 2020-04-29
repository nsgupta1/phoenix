package org.apache.phoenix.schema.types;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class S3BasedLobStore implements LobStore {

    private static final String ROOT_DIRECTORY = "/tmp/";
    private AmazonS3 s3client;
    private static String BUCKET_NAME = "phoenixgiants-bucket";
    private static S3BasedLobStore INSTANCE = null;

    public synchronized static S3BasedLobStore getInstance() {
        if(INSTANCE == null){
            INSTANCE = new S3BasedLobStore();
        }
        return INSTANCE;
    }

    public S3BasedLobStore() {
        BasicAWSCredentials credentials = new BasicAWSCredentials("FakeAccessKey", "FakeSecretKey");
        s3client = AmazonS3ClientBuilder.standard()
                        .withCredentials(new AWSStaticCredentialsProvider(credentials))
                        .withRegion(Regions.US_WEST_2).build();
    }

    @VisibleForTesting
    String getPathFromLocator(String lobLocator) {
        return ROOT_DIRECTORY + lobLocator;
    }

    @Override
    public String putLob(InputStream lobStream) throws LobStoreException {
        String lobLocator = RandomStringUtils.randomAlphanumeric(15); //For now use lobLocator
        File tempStore = new File(getPathFromLocator(lobLocator));
        try {
            FileUtils.copyInputStreamToFile(lobStream, tempStore);
            s3client.putObject(BUCKET_NAME,lobLocator, tempStore);
        } catch (IOException e) {
            throw new LobStoreException(e);
        }
        finally {
           tempStore.delete();
        }
        return lobLocator;
    }

    @Override
    public InputStream getLob(String lobLocator) throws LobStoreException {
        try (final S3Object s3Object = s3client.getObject(BUCKET_NAME, lobLocator);
             InputStream in = s3Object.getObjectContent()) {
            return IOUtils.toBufferedInputStream(in);
        } catch (final IOException e) {
            throw new LobStoreException(e);
        }
    }
}
