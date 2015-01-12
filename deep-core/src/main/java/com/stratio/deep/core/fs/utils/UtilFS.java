/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.stratio.deep.core.fs.utils;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.api.java.JavaRDD;

import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkContext;

public class UtilFS {

    private static final String METADATA_FILE = "metaFile.csv";

    public static TextFileDataTable createTextFileFromSchemaFile(String fsFilePath,
            DeepSparkContext deepContext) {

        JavaRDD<String> rdd = deepContext.textFile(fsFilePath);

        TextFileDataTable textFileDataTable = new TextFileDataTable();
        ArrayList<SchemaMap> listSchemaMap = new ArrayList<>();
        TableName tableName = new TableName();

        for(String line :rdd.collect()){
            if(line.startsWith("CatalogName=")){
                String [] args = line.split("=");
                tableName.setCatalogName(args[1]);

            }else if(line.startsWith("TableName=")){
                String [] args = line.split("=");
                tableName.setTableName(args[1]);
            }else if(line.startsWith("Field")){
                String [] args = line.split(":");
                listSchemaMap.add(new SchemaMap(args[1], getClassFromFileField(args[2])));
            }else if(line.startsWith("Indexes=")){

            }
        }
        textFileDataTable.setTableName(tableName);
        textFileDataTable.setColumnMap(listSchemaMap);
        return textFileDataTable;
    }

    public static TextFileDataTable createTextFileMetaDataFromConfig(ExtractorConfig<Cells> extractorConfig,
            DeepSparkContext deepSparkContext) {


        Serializable separator = extractorConfig.getValues().get(ExtractorConstants.FS_FILE_SEPARATOR);
        String catalogName = (String) extractorConfig.getValues().get(ExtractorConstants.CATALOG);
        String tableName = (String) extractorConfig.getValues().get(ExtractorConstants.TABLE);
        final String splitSep = separator.toString();

        if(extractorConfig.getValues().get(ExtractorConstants.FS_FILEDATATABLE)!=null ){

            final TextFileDataTable textFileDataTable  = (TextFileDataTable)extractorConfig.getValues().get(ExtractorConstants.FS_FILEDATATABLE);
            return textFileDataTable;

        }else if(extractorConfig.getValues().get(ExtractorConstants.FS_SCHEMA)!=null){


            final ArrayList<SchemaMap<?>> columns = (ArrayList<SchemaMap<?>>) extractorConfig.getValues().get
                    (ExtractorConstants.FS_SCHEMA);

            final TextFileDataTable textFileDataTableTemp = new TextFileDataTable(new TableName(catalogName, tableName),
                    columns);
            textFileDataTableTemp.setLineSeparator(splitSep);
            return textFileDataTableTemp;

        }else{
            final TextFileDataTable textFileDataTableTmp = createTextFileFromSchemaFile(buildFilePath(extractorConfig), deepSparkContext);
            textFileDataTableTmp.setLineSeparator(splitSep);
            return textFileDataTableTmp;
        }
    }

    public static String buildFilePath(ExtractorConfig extractorConfig) throws IllegalArgumentException {
        if(ExtractorConstants.HDFS.equals(extractorConfig.getExtractorImplClassName())) {
            String host  = extractorConfig.getString(ExtractorConstants.HOST);
            String port  = extractorConfig.getString(ExtractorConstants.PORT);
            String path  = extractorConfig.getString(ExtractorConstants.FS_FILE_PATH);
            path = path.substring(0, path.lastIndexOf("/"));
            return ExtractorConstants.HDFS_PREFIX + host.toString() + ":" + port  + path.toString() + "/" + METADATA_FILE;
        } else if(ExtractorConstants.S3.equals(extractorConfig.getExtractorImplClassName())) {
            String bucket = extractorConfig.getString(ExtractorConstants.S3_BUCKET);
            String path = extractorConfig.getString(ExtractorConstants.FS_FILE_PATH);
            path = path.substring(0, path.lastIndexOf("/"));
            return extractorConfig.getString(ExtractorConstants.FS_PREFIX) + bucket + path + "/" + METADATA_FILE;
        }
        throw new IllegalArgumentException("Configured ExtractorImplClassName must be 'hdfs' or 's3'");
    }


    public static Class getClassFromFileField(String arg) {

        Class cl = null ;
        switch (arg){
            case "BIGINT":break;
            case "INT":
                cl = Integer.class;
                break;
            case "TEXT":
                cl = String.class;
                break;
            case "VARCHAR":
                cl = String.class;
                break;
            case "DOUBLE":
                cl = Double.class;
                break;
            case "FLOAT":
                cl = Float.class;
                break;
            case "BOOLEAN":
                cl = Boolean.class;
                break;
            case "SET":break;
            case "LIST":break;
            case "MAP":break;
            case "NATIVE":break;
            default:break;
        }
        return cl;
    }
}
