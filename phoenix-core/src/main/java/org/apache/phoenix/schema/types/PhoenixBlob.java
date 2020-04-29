package org.apache.phoenix.schema.types;

import org.apache.phoenix.expression.BlobExpression;

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

public class PhoenixBlob implements Blob {

    private final boolean isOutputBlob;
    private final InputStream inputStream;
    private final BlobExpression.BlobMetaData blobMetaData;

    public PhoenixBlob(BlobExpression.BlobMetaData blobMetaData, InputStream inputStream) {
        this.isOutputBlob = true;
        this.inputStream = inputStream;
        this.blobMetaData = blobMetaData;
    }

    public BlobExpression.BlobMetaData getBlobMetaData() {
        return this.blobMetaData;
    }

    @Override
    public long length() {
        if(isOutputBlob) {
            return blobMetaData.getSize();
        }
        return 0;
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SQLException {
        throw new SQLException("Not Supported");
    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        if(isOutputBlob) {
            return inputStream;
        }
        throw new SQLException("Not Supported");
    }

    @Override
    public long position(byte[] pattern, long start) throws SQLException {
        throw new SQLException("Not Supported");
    }

    @Override
    public long position(Blob pattern, long start) throws SQLException {
        throw new SQLException("Not Supported");
    }

    @Override
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        throw new SQLException("Not Supported");
    }

    @Override
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        throw new SQLException("Not Supported");
    }

    //TODO: is this the hook for writing?
    @Override
    public OutputStream setBinaryStream(long pos) throws SQLException {
        throw new SQLException("Not Supported");
    }

    @Override
    public void truncate(long len) throws SQLException {
        throw new SQLException("Not Supported");
    }

    @Override
    public void free() throws SQLException {
        if(isOutputBlob){
            try {
                inputStream.close();
            } catch (Exception e) {
                throw new SQLException(e);
            }
        }
    }

    @Override
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        throw new SQLException("Not Supported");
    }

    @Override
    public String toString() {
        return "PhoenixBlob Object:" + super.toString() +
                "[isOutputBlob: " + this.isOutputBlob + ", " +
                "inputStream: " + this.inputStream + ", " +
                "blobMetaData: " + this.blobMetaData + "]";
    }
}
