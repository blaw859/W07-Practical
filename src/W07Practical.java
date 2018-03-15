import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;

public class W07Practical {
    static String dbFileName;
    static String dbName;
    static String dbURL;
    static String tableName;
    public static Connection conn;
    static File database;
    static File csvFile;
    static HashMap<String,String> attributeMap;
    static String dbPath;

    public static void main(String args[]) {
        tableName = "titanicPeople";
        dbPath = args[0];
        String action = args[1];
        if(args.length == 3) {
            csvFile = new File(args[2]);
        }
        switch(action) {
            case "create": createAndPopulateDatabase();
            break;
            case "query1": printAllRecords();
            break;
            case "query2": getNumberOfSurvivors();
            break;
            case "query3": getSurvivorCountByClass();
            break;
            case "query4": getMinimumSurvivorAge();
            break;
        }
    }
    public static void getMinimumSurvivorAge() {
        getDatabaseInformation();
        try {
            PreparedStatement preparedStatement = conn.prepareStatement("SELECT sex,survived,MIN(age)  FROM "+tableName+" GROUP BY sex,survived");
            ResultSet survivedCount = preparedStatement.executeQuery();
            System.out.println("sex, survived, minimum age");
            while (survivedCount.next()) {
                System.out.println(survivedCount.getObject(1)+", "+survivedCount.getObject(2)+", "+survivedCount.getObject(3));
            }
        } catch (SQLException e) {
            System.out.println("Unable to get number of survivors");
        }
    }

    public static void getNumberOfSurvivors() {
        getDatabaseInformation();
        try {
            PreparedStatement preparedStatement = conn.prepareStatement("SELECT count(*) FROM '"+tableName+"' WHERE survived = 1");
            ResultSet survivedCount = preparedStatement.executeQuery();
            System.out.println("Number of Survivors");
            System.out.println(survivedCount.getObject(1));
        } catch (SQLException e) {
            System.out.println("Unable to get number of survivors");
        }
    }

    public static void  getSurvivorCountByClass() {
        getDatabaseInformation();
        try {
            PreparedStatement preparedStatement = conn.prepareStatement("SELECT pClass,survived,count(survived) FROM "+tableName+" GROUP BY pClass,survived");
            ResultSet survivedCount = preparedStatement.executeQuery();
            System.out.println("pClass, survived, count");
            while (survivedCount.next()) {
                System.out.println(survivedCount.getObject(1)+", "+survivedCount.getObject(2)+", "+survivedCount.getObject(3));
            }
        } catch (SQLException e) {
            System.out.println("Unable to get number of survivors");
        }
    }

    public static void createAndPopulateDatabase() {
        try {
            createDatabase(dbPath);
            createTable(conn,csvFile);
            populateTable(conn);
        } catch (SQLException e) {
            System.out.println("Unable to create or populate database");
            System.exit(0);
        }
    }

    public static void getDatabaseInformation() {
        try {
            database = new File(dbPath);
            dbFileName = database.getName();
            dbURL = "jdbc:sqlite:" + dbFileName;
            setDBNameExtension();
            conn = DriverManager.getConnection(dbURL);

        } catch (SQLException e) {
            System.out.println("Unable to obtain database information");
        }
    }

    public static void setDBNameExtension() {
        if (dbFileName.indexOf(".") > 0) {
            dbName = dbFileName.substring(0,dbFileName.indexOf("."));
        }

    }

    public static void printAllRecords() {
        //Information used by the databaseMetaData.getColumns() method
        String   catalog           = null;
        String   schemaPattern     = null;
        String   tableNamePattern  = tableName;
        String   columnNamePattern = null;
        getDatabaseInformation();
        try {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            PreparedStatement preparedStatement = null;
            preparedStatement = conn.prepareStatement("SELECT * FROM '"+tableName+"'");
            ResultSet recordsResultSet = preparedStatement.executeQuery();
            /*preparedStatement = conn.prepareStatement("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME =  N'"+tableName+"'");*/
            /*ResultSet columnResultSet = databaseMetaData.getColumns(catalog,schemaPattern,tableNamePattern,columnNamePattern);
            for (int i = 1; i <= 11; i++) {
                System.out.print(columnResultSet.getString(i)+", ");
            }*/
            /*
            System.out.println(headersResultSet.getString(12));*/
            System.out.println("passengerId, survived, pClass, name, sex, age, sibSp, parch, ticket, fare, cabin, embarked");
            while (recordsResultSet.next()) {
                for (int i = 1; i <= 11; i++) {
                    System.out.print(recordsResultSet.getObject(i)+", ");
                }
                System.out.println(recordsResultSet.getObject(12));
            }
        } catch (java.sql.SQLException e) {
            e.printStackTrace();
        }
    }

    public static void createDatabase(String dbPath) throws SQLException {
        database = new File(dbPath);
        dbFileName = database.getName();
        setDBNameExtension();
        dbURL = "jdbc:sqlite:" + dbFileName;
        conn = DriverManager.getConnection(dbURL);
        try {
            System.out.println(database.getAbsolutePath());
            database.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createTable(Connection conn, File inputFile) throws SQLException{
        PreparedStatement preparedStatement = null;
        String[] columnHeaders = getColumnHeaders();
        HashMap<String,String> attributeMap = setAttributes(columnHeaders);
        preparedStatement = conn.prepareStatement("DROP TABLE IF EXISTS "+tableName);
        preparedStatement.executeUpdate();
        String queryString = "CREATE TABLE '"+tableName+"' (";
        for (int i = 0; i < attributeMap.size(); i++) {
            if(i == 0){
                queryString = queryString +columnHeaders[i]+" "+attributeMap.get(columnHeaders[i]);
            } else {
                queryString = queryString +","+columnHeaders[i]+" "+attributeMap.get(columnHeaders[i]);
            }
        }
        queryString = queryString + ")";
        preparedStatement = conn.prepareStatement(queryString);
        preparedStatement.executeUpdate();
        preparedStatement.close();
    }

    public static void populateTable(Connection conn) throws java.sql.SQLException{
        PreparedStatement preparedStatement = null;
        ArrayList<String[]> records = readFile();
        for (int i = 0; i < records.size(); i++) {
            String queryString = "INSERT INTO "+tableName+" VALUES ("+records.get(i)[0]+","+records.get(i)[1]+","+records.get(i)[2]+","+records.get(i)[3]+
                    ","+records.get(i)[4]+","+records.get(i)[5]+","+records.get(i)[6]+","+records.get(i)[7]+","+records.get(i)[8]+","+records.get(i)[9]+","+records.get(i)[10]+","+records.get(i)[11]+")";
            System.out.println(queryString);
            preparedStatement = conn.prepareStatement(queryString);
            preparedStatement.executeUpdate();
        }
        preparedStatement.close();
    }

    public static HashMap<String,String> setAttributes(String[] attributeArray) {
        attributeMap = new HashMap();
        attributeMap.put(attributeArray[0],"INTEGER");
        attributeMap.put(attributeArray[1],"INTEGER");
        attributeMap.put(attributeArray[2],"INTEGER");
        attributeMap.put(attributeArray[3],"VARCHAR(100)");
        attributeMap.put(attributeArray[4],"VARCHAR(20)");
        attributeMap.put(attributeArray[5],"FLOAT");
        attributeMap.put(attributeArray[6],"INTEGER");
        attributeMap.put(attributeArray[7],"INTEGER");
        attributeMap.put(attributeArray[8],"STRING");
        attributeMap.put(attributeArray[9],"DECIMAL(10,10)");
        attributeMap.put(attributeArray[10],"STRING");
        attributeMap.put(attributeArray[11],"CHAR");
        return attributeMap;
    }

    public static ArrayList<String[]> readFile() {
        ArrayList<String[]> allRecords = new ArrayList();
        try (Scanner inputStream = new Scanner(csvFile)) {
            int i = 0;
            while (inputStream.hasNext()) {
                String data = inputStream.nextLine();
                if (i != 0) {
                    String[] record = data.split(",");
                    record = sanitiseRecord(record);
                    allRecords.add(record);
                }
                i++;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return allRecords;
    }

    public static String[] sanitiseRecord(String[] record) {
        record[3] = "'"+record[3]+"'";
        record[4] = "'"+record[4]+"'";
        record[8] = "'"+record[8]+"'";
        record[10] = "'"+record[10]+"'";
        record[11] = "'"+record[11]+"'";
        for (int i = 0; i < record.length; i++) {
            if (record[i].equals("") || record[i].equals("''")) {
                record[i] = "NULL";
            }
        }
        return record;
    }

    public static String[] getColumnHeaders() {
        String[] columnHeaders;
        try (Scanner inputStream = new Scanner(csvFile)) {
            if(inputStream.hasNext()) {
                String data = inputStream.nextLine();
                columnHeaders = data.split(",");
            } else {
                columnHeaders = null;
            }
        } catch (FileNotFoundException e) {
            columnHeaders = null;
        }
        return columnHeaders;
    }

}
