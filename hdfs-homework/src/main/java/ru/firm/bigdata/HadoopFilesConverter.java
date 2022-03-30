package ru.firm.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.io.IOException;

import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

/**
 * This class is used for converting a CSV file located in an HDFS into a parquet file located in a Linux file system.
 * The class should be compiled into a jar file to use it.
 */
public class HadoopFilesConverter {

    public static final String CSV_SEPARATOR = ",";

    /**
     * Runs the converting.
     *
     * @param args are JVM parameters. args[0] is a HDFS uri string of the source CSV file, args[1] is a path string of
     *             the target parquet file. For example, syntax of running:
     *             java -jar converter.jar "hdfs://sandbox-hdp.hortonworks.com:8020/user/admin/example.csv" "example.parquet",
     *             in this case file example.parquet is saved in the user's home directory.
     * @throws IOException if something goes wrong.
     */
    public static void main(String[] args) throws IOException {
        final String sourceCsvUriString = args[0];
        final String targetParquetPathString = args[1];

        convertCsvToParquet(sourceCsvUriString, CSV_SEPARATOR, targetParquetPathString);
    }

    /**
     * Performs the converting: reads the source CSV, reads the source CSVs first row, makes a parquet schema using
     * CSV column names, makes a parquet writer using the path string of the target parquet file and the parquet schema,
     * prepares a group factory for every next row of the CSV and writes the groups to the parquet file in a loop.
     *
     * @param uriString    is the HDFS uri string of the source CSV file.
     * @param csvSeparator is the separator of the source CSV file.
     * @param pathString   is the path string of the target parquet file.
     * @throws IOException if something goes wrong.
     */
    public static void convertCsvToParquet(String uriString, String csvSeparator, String pathString) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fileSystem = FileSystem.get(URI.create(uriString), conf);
        BufferedReader reader = null;
        String[] firstRow;
        String[] currentRow;
        MessageType schema;
        Group group;
        ParquetWriter writer = null;
        try {
            reader = new BufferedReader(new InputStreamReader(fileSystem.open(new Path(uriString))));
            String line = reader.readLine();
            firstRow = line.split(csvSeparator);
            schema = prepareParquetSchema(firstRow);
            writer = createParquetWriter(pathString, schema);
            while ((line = reader.readLine()) != null) {
                currentRow = line.split(csvSeparator);
                group = prepareGroupForCsvRow(schema, firstRow, currentRow);
                writer.write(group);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                reader.close();
            }
            if (writer != null) {
                writer.close();
            }
            if (fileSystem != null) {
                fileSystem.close();
            }
        }
    }

    /**
     * Prepares a group factory for current row of the CSV.
     *
     * @param schema        is a MessageType schema of the target parquet file created basing on the CSVs column names.
     * @param csvFirstRow   is the first row of the source CSV file.
     * @param csvCurrentRow is current row (second and next) of the source CSV file, that is being processed
     *                      this moment.
     * @return the group factory object created for the current CSV row.
     */
    public static Group prepareGroupForCsvRow(MessageType schema, String[] csvFirstRow, String[] csvCurrentRow) {
        GroupFactory groupFactory = new SimpleGroupFactory(schema);
        Group group = groupFactory.newGroup();
        for (int i = 0; i < csvFirstRow.length; i++) {
            group.append(csvFirstRow[i], csvCurrentRow[i]);
        }
        return group;
    }

    /**
     * Makes the parquet schema for the target parquet file using CSV column names.
     *
     * @param csvFirstRow is the first row of the source CSV file.
     * @return a MessageType object.
     */
    public static MessageType prepareParquetSchema(String[] csvFirstRow) {
        String schemaString = "message Pair {\n";
        for (String csvColumn : csvFirstRow) {
            schemaString = schemaString + " required binary " + csvColumn + " (UTF8);\n";
        }
        schemaString = schemaString + "}";
        return MessageTypeParser.parseMessageType(schemaString);
    }

    /**
     * Makes the parquet writer using the path string of the target parquet file and the parquet schema.
     *
     * @param filePath is the path string of the target parquet file.
     * @param schema   is the MessageType schema of the target parquet file.
     * @return a ParquetWriter object.
     * @throws IOException if something goes wrong.
     */
    public static ParquetWriter createParquetWriter(String filePath, MessageType schema) throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path(filePath);
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        GroupWriteSupport.setSchema(schema, conf);
        ParquetWriter<Group> writer = new ParquetWriter<Group>(path, writeSupport,
                ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                ParquetProperties.WriterVersion.PARQUET_1_0, conf);
        return writer;
    }
}
