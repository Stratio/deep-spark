package com.stratio.deep.core.hdfs.utils;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.api.java.JavaRDD;

import com.stratio.deep.commons.config.ExtractorConfig;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.commons.extractor.utils.ExtractorConstants;
import com.stratio.deep.core.context.DeepSparkContext;

public class UtilHDFS {

    public static TextFileDataTable createTextFileFromSchemaFile(String hdfsFilePath,
            DeepSparkContext deepContext) {

        JavaRDD<String> rdd = deepContext.textFile(hdfsFilePath);

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


        Serializable separator = extractorConfig.getValues().get(ExtractorConstants.HDFS_FILE_SEPARATOR);
        String catalogName = (String) extractorConfig.getValues().get(ExtractorConstants.CATALOG);
        String tableName = (String) extractorConfig.getValues().get(ExtractorConstants.TABLE);
        final String splitSep = separator.toString();

        if(extractorConfig.getValues().get(ExtractorConstants.HDFS_FILEDATATABLE)!=null ){

            final TextFileDataTable textFileDataTable  = (TextFileDataTable)extractorConfig.getValues().get(ExtractorConstants.HDFS_FILEDATATABLE);
            return textFileDataTable;

        }else if(extractorConfig.getValues().get(ExtractorConstants.HDFS_SCHEMA)!=null){


            final ArrayList<SchemaMap<?>> columns = (ArrayList<SchemaMap<?>>) extractorConfig.getValues().get
                    (ExtractorConstants.HDFS_SCHEMA);

            final TextFileDataTable textFileDataTableTemp = new TextFileDataTable(new TableName(catalogName, tableName),
                    columns);
            textFileDataTableTemp.setLineSeparator(splitSep);
            return textFileDataTableTemp;

        }else{
            Serializable host  = extractorConfig.getValues().get(ExtractorConstants.HOST);
            Serializable port  = extractorConfig.getValues().get(ExtractorConstants.PORT);
            String path  = (String)extractorConfig.getValues().get(ExtractorConstants.HDFS_FILE_PATH);
            path = path.substring(0, path.lastIndexOf("/"));
            final TextFileDataTable textFileDataTableTmp = createTextFileFromSchemaFile(ExtractorConstants.HDFS_PREFIX
            + host.toString() + ":" + port  + path.toString()+"/metaFile.csv", deepSparkContext);
            textFileDataTableTmp.setLineSeparator(splitSep);
            return textFileDataTableTmp;

        }



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
