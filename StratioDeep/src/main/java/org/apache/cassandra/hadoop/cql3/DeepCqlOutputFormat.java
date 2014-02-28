package org.apache.cassandra.hadoop.cql3;

import java.io.IOException;

import org.apache.commons.lang.NotImplementedException;

import com.stratio.deep.entity.Cells;
import org.apache.cassandra.hadoop.AbstractColumnFamilyOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;

/**
 * Custom implementation of Hadoop outformat returning a DeepCqlRecordWriter
 */
public class DeepCqlOutputFormat extends AbstractColumnFamilyOutputFormat<Cells, Cells> {

    /** Fills the deprecated OutputFormat interface for streaming. */
    @Override
    @Deprecated
    public RecordWriter<Cells, Cells> getRecordWriter(FileSystem fileSystem, JobConf entries, String s,
		    Progressable progressable) throws IOException {
	throw new NotImplementedException(
			"Deprecated method \'getRecordWriter(FileSystem fileSystem, JobConf entries, String s, Progressable progressable)\' not implemented");
    }

    /**
     * Returns a DeepCqlRecordWriter.
     *
     * @param context
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public org.apache.hadoop.mapreduce.RecordWriter<Cells, Cells> getRecordWriter(final TaskAttemptContext context)
		    throws IOException, InterruptedException {
	return new DeepCqlRecordWriter(context);
    }

}
