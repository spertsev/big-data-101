package ru.firm.bigdata;

import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.example.data.Group;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HadoopFilesConverterTest {

    @AfterClass
    public static void removeFilesCreatedByTests() {
        String createParquetWriterTestParquetFilePath = "file.parquet";
        String createParquetWriterTestParquetCrcFilePath = ".file.parquet.crc";
        String convertCsvToParquetTestCsvFilePath = "temp.csv";
        String convertCsvToParquetTestParquetFilePath = "temp.parquet";
        String convertCsvToParquetTestParquetCrcFilePath = ".temp.parquet.crc";

        List<String> filesToRemove = new ArrayList<>();
        filesToRemove.add(createParquetWriterTestParquetFilePath);
        filesToRemove.add(createParquetWriterTestParquetCrcFilePath);
        filesToRemove.add(convertCsvToParquetTestParquetFilePath);
        filesToRemove.add(convertCsvToParquetTestCsvFilePath);
        filesToRemove.add(convertCsvToParquetTestParquetCrcFilePath);
        filesToRemove.forEach(pathString -> {
            try {
                File file = new File(pathString);
                file.delete();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Test
    public void convertCsvToParquetTestShouldCreateParquetFile() {
        String tempCsvFilePathString = "temp.csv";
        String targetParquetFileUriString = "temp.parquet";

        try {
            FileWriter fileWriter = new FileWriter(tempCsvFilePathString);
            PrintWriter printWriter = new PrintWriter(fileWriter);
            printWriter.print("column1,column2\n");
            printWriter.print("1,2");
            printWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            HadoopFilesConverter.convertCsvToParquet(tempCsvFilePathString, ",", targetParquetFileUriString);
        } catch (Exception e) {
            e.printStackTrace();
        }

        File targetParquetFile = new File(targetParquetFileUriString);
        Assert.assertTrue(targetParquetFile.exists());
    }

    @Test
    public void prepareGroupForCsvRowTestShouldReturnStringRepresentationOfGroupObject() {
        String[] csvFirstRow = {"column1", "column2"};
        MessageType schema = HadoopFilesConverter.prepareParquetSchema(csvFirstRow);
        String[] csvCurrentRow = {"1", "2"};
        Group group = HadoopFilesConverter.prepareGroupForCsvRow(schema, csvFirstRow, csvCurrentRow);
        Assert.assertEquals("column1: 1\n" + "column2: 2\n", group.toString());
    }

    @Test
    public void prepareParquetSchemaTestShouldReturnStringRepresentationOfMessageObjectColumns() {
        String firstColumnName = "column1";
        String secondColumnName = "column2";
        MessageType returnedObject = HadoopFilesConverter.prepareParquetSchema(new String[]{firstColumnName, secondColumnName});
        String stringRepresentationOfColumnsInTheObject = Arrays.toString(returnedObject.getColumns().toArray());
        String expectedString = "[[" + firstColumnName +
                "] required binary " + firstColumnName +
                " (STRING), [" + secondColumnName +
                "] required binary " + secondColumnName +
                " (STRING)]";
        Assert.assertEquals(expectedString, stringRepresentationOfColumnsInTheObject);
    }

    @Test
    public void createParquetWriterTestShouldReturnParquetWriterClassName() {
        String filePath = "file.parquet";
        File file = new File(filePath);
        MessageType schema = HadoopFilesConverter.prepareParquetSchema(new String[]{"column1", "column2"});
        ParquetWriter writer = null;
        try {
            writer = HadoopFilesConverter.createParquetWriter(filePath, schema);
        } catch (Exception e) {
            e.printStackTrace();
        }
        String writerClassName = "";
        try {
            if (writer != null) {
                writerClassName = writer.getClass().getName();
                writer.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        Assert.assertEquals("org.apache.parquet.hadoop.ParquetWriter", writerClassName);
    }

}