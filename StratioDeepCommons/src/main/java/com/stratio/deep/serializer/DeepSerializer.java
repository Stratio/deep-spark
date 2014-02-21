package com.stratio.deep.serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.*;
import scala.collection.Iterator;
import sun.nio.ch.DirectBuffer;

/**
 * Created by luca on 21/02/14.
 */
public class DeepSerializer extends JavaSerializer {
    private transient Logger logger = Logger.getRootLogger();

    public DeepSerializer(SparkConf conf) {
	super(conf);
    }

    @Override
    public SerializerInstance newInstance() {
	return new DeepSerializerInstance();
    }

    class DeepSerializerInstance implements SerializerInstance{

	@Override
	public <T> ByteBuffer serialize(T t) {
	    ByteArrayOutputStream bos = new ByteArrayOutputStream();
	    SerializationStream stream = serializeStream(bos);

	    stream.writeObject(t);
	    stream.close();
	    return ByteBuffer.wrap(bos.toByteArray());
	}

	@Override
	public <T> T deserialize(ByteBuffer bytes) {
	    ByteBufferInputStream bis = new ByteBufferInputStream(bytes, false);
	    DeserializationStream stream = deserializeStream(bis);
	    return stream.readObject();
	}

	@Override
	public <T> T deserialize(ByteBuffer bytes, ClassLoader loader) {
	    ByteBufferInputStream bis = new ByteBufferInputStream(bytes, false);
	    DeserializationStream stream = deserializeStream(bis,loader);
	    return stream.readObject();
	}

	@Override
	public SerializationStream serializeStream(OutputStream s) {
	    return new JavaSerializationStream(s);
	}

	@Override
	public DeserializationStream deserializeStream(InputStream s) {
	    return new JavaDeserializationStream(s, Thread.currentThread().getContextClassLoader());
	}

	public DeserializationStream deserializeStream(InputStream s, ClassLoader loader) {
	    return new JavaDeserializationStream(s, loader);
	}

	@Override
	public <T> ByteBuffer serializeMany(Iterator<T> iterator) {
	    return null;
	}

	@Override
	public Iterator<Object> deserializeMany(ByteBuffer buffer) {
	    return null;
	}
    }

    class ByteBufferInputStream extends InputStream{
	private ByteBuffer buffer;
	private Boolean dispose = Boolean.FALSE;

	ByteBufferInputStream(ByteBuffer buffer, Boolean dispose) {
	    this.buffer = buffer;
	    this.dispose = dispose;
	}

	@Override
	public int read() throws IOException {
	    if (buffer == null || buffer.remaining() == 0) {
		cleanUp();
		return -1;
	    } else {
		return buffer.get() & 0xFF;
	    }
	}

	@Override
	public int read(byte[] dest) throws IOException {
	    return read(dest, 0, dest.length);
	}

	@Override
	public int read(byte[] dest, int offset, int len) throws IOException {
	    if (buffer == null || buffer.remaining() == 0) {
		cleanUp();
		return -1;
	    } else {
		int amountToGet = Math.min(buffer.remaining(), len);
		buffer.get(dest, offset, amountToGet);
		return amountToGet;
	    }
	}

	@Override
	public long skip(long bytes) throws IOException {
	    if (buffer != null) {
		int amountToSkip = (int) Math.min(bytes, buffer.remaining());
		buffer.position(buffer.position() + amountToSkip);
		if (buffer.remaining() == 0) {
		    cleanUp();
		}
		return amountToSkip;
	    } else {
		return 0L;
	    }
	}

	/**
	 * Clean up the buffer, and potentially dispose of it using BlockManager.dispose().
	 */
	private void cleanUp() {
	    if (buffer != null) {
		if (dispose) {
		    if (buffer != null && buffer instanceof MappedByteBuffer) {
			logger.debug("Unmapping " + buffer);
			if (buffer instanceof  DirectBuffer && ((DirectBuffer)buffer).cleaner() != null) {
			    ((DirectBuffer)buffer).cleaner().clean();
			}
		    }
		}
		buffer = null;
	    }
	}
    }
}
