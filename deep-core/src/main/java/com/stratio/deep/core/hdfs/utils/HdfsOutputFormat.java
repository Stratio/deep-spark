package com.stratio.deep.core.hdfs.utils;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;

public class HdfsOutputFormat<K, V>  extends TextOutputFormat<K,V> {

    private String fileName;


    @Override
    public RecordWriter getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress)
            throws IOException {
        return super.getRecordWriter(ignored, job, fileName, progress);
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

}
