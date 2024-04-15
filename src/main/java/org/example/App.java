package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class App {
    public static void main(String[] args) {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "zookeeper");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.master", "hbase-master:16000");

        try (Connection connection = ConnectionFactory.createConnection(config)) {
            TableName tableName = TableName.valueOf("Students");

            try (Table table = connection.getTable(tableName)) {
                createStudent(connection, tableName);
                addStudents(table);
                PrintStudent(table, "student1");
                updateStudentInfo(table, "student2", "info", "age", "23");
                updateStudentInfo(table, "student2", "grades", "math", "A+");
                deleteStudent(table, "student1");
                PrintStudents(table);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void createStudent(Connection connection, TableName tableName) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            if (!admin.tableExists(tableName)) {
                TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info")).build())
                        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("grades")).build())
                        .build();

                admin.createTable(tableDescriptor);
            }
        }
    }

    private static void addStudents(Table table) throws IOException {
        Put putStudent1 = new Put(Bytes.toBytes("student1"));
        putStudent1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("John Doe"));
        putStudent1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("20"));
        putStudent1.addColumn(Bytes.toBytes("grades"), Bytes.toBytes("math"), Bytes.toBytes("B"));
        putStudent1.addColumn(Bytes.toBytes("grades"), Bytes.toBytes("science"), Bytes.toBytes("A"));
        table.put(putStudent1);

        Put putStudent2 = new Put(Bytes.toBytes("student2"));
        putStudent2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("Jane Smith"));
        putStudent2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("22"));
        putStudent2.addColumn(Bytes.toBytes("grades"), Bytes.toBytes("math"), Bytes.toBytes("A"));
        putStudent2.addColumn(Bytes.toBytes("grades"), Bytes.toBytes("science"), Bytes.toBytes("A"));
        table.put(putStudent2);
    }

    private static void PrintStudent(Table table, String studentId) throws IOException {
        Get get = new Get(Bytes.toBytes(studentId));
        Result result = table.get(get);

        System.out.println("Informations pour l'étudiant " + studentId + ":");
        for (Cell cell : result.listCells()) {
            String columnFamily = Bytes.toString(CellUtil.cloneFamily(cell));
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            System.out.println(columnFamily + ":" + qualifier + " -> " + value);
        }
    }

    private static void updateStudentInfo(Table table, String studentId, String family, String qualifier, String newValue) throws IOException {
        Put put = new Put(Bytes.toBytes(studentId));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(newValue));
        table.put(put);
    }

    private static void deleteStudent(Table table, String studentId) throws IOException {
        Delete delete = new Delete(Bytes.toBytes(studentId));
        table.delete(delete);
    }

    private static void PrintStudents(Table table) throws IOException {
        Scan scan = new Scan();
        try (ResultScanner scanner = table.getScanner(scan)) {
            for (Result result : scanner) {
                for (Cell cell : result.listCells()) {
                    String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                    String columnFamily = Bytes.toString(CellUtil.cloneFamily(cell));
                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    System.out.println(rowKey + " - " + columnFamily + ":" + qualifier + " -> " + value);
                }
            }
        }
    }
}
