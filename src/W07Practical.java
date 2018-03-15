import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class W07Practical {
    private static String dbFileName;
    private static String dbURL;
    private static String tableName;
    private static Connection conn;
    private static File database;
    private static File csvFile;
    private static HashMap<String,String> attributeMap;
    private static String dbPath;

    /**
     * Takes command line arguments as input and calls the correct method to either create the database or perform some
     * query
     * @param args Should always start with the location of the database and an action and then also the location of the
     *             .csv file if a database is being created
     */
    public static void main(String args[]) {
        tableName = "titanicpeople";
        String action = "";

        //Validation on the first two arguments
        try {
            dbPath = args[0];
            action = args[1];
            if (!dbPath.substring(dbPath.length()-3,dbPath.length()).equals(".db")) {
                System.out.println("Usage: java -cp sqlite-jdbc.jar:. W07Practical <db_file> <action> [input_file]");
                System.exit(0);
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            System.out.println("Usage: java -cp sqlite-jdbc.jar:. W07Practical <db_file> <action> [input_file]");
            System.exit(0);
        }

        //Validation on the third argument
        if (args.length == 3) {
            csvFile = new File(args[2]);
            if (!dbPath.substring(dbPath.length()-3,dbPath.length()).equals(".db")) {
                System.out.println("Usage: java -cp sqlite-jdbc.jar:. W07Practical <db_file> <action> [input_file]");
                System.exit(0);
            }
        }

        //Switch statement that will select the right query and execute it based on pre defined views
        switch (action) {
            case "create": createAndPopulateDatabase();
            break;
            case "query1": queryView("all_passengers","passengerId, survived, pClass, name, sex, age, sibSp, parch, ticket, fare, cabin, embarked",12);
            break;
            case "query2": queryView("number_of_survivors","Number of Survivors",1);
            break;
            case "query3": queryView("survivor_count","pClass, survived, count",3);
            break;
            case "query4": queryView("minimum_survivor_age","sex, survived, minimum age",3);
            break;
            default: System.out.println("Usage: java -cp sqlite-jdbc.jar:. W07Practical <db_file> <action> [input_file]");
            break;
        }
    }

    /**
     * Prints out the result set of a query using the view parameter. The header parameter is what is printed first then
     * all of the returned results are printed out
     * @param view One of the pre defined views to be used
     * @param header The line of text to be printed before the results
     * @param returnedColumns The number of columns returned such that each column can be printed in a loop
     */
    private static void queryView(String view, String header, int returnedColumns) {
        getDatabaseInformation();
        try {
            PreparedStatement preparedStatement = conn.prepareStatement("SELECT * FROM " + view);
            ResultSet viewOutput = preparedStatement.executeQuery();
            System.out.println(header);
            while (viewOutput.next()) {
                if(returnedColumns > 1) {
                    for (int i = 1; i <= returnedColumns - 1; i++) {
                        System.out.print(viewOutput.getObject(i) + ", ");
                    }
                }
                System.out.println(viewOutput.getObject(returnedColumns));
            }
        } catch (SQLException e) {
            System.out.println("Unable to perform query with this view");
        }
    }

    /**
     * Creates a view with the minimum age of people from each sex who survived or didn't
     * @throws SQLException If this view cannot be created an exception will be thrown, this will be to do with an issue
     *                      with the table name
     */
    private static void createViewMinimumSurvivorAge() throws SQLException{
            PreparedStatement preparedStatement = conn.prepareStatement("CREATE VIEW IF NOT EXISTS minimum_survivor_age AS SELECT sex,survived,MIN(age)  FROM "+tableName+" GROUP BY sex,survived");
            preparedStatement.execute();
    }

    /**
     * Creates a view with the number of survivors from the ship
     * @throws SQLException If this view cannot be created an exception will be thrown, this will be to do with an issue
     *                      with the table name
     */
    private static void createViewNumberOfSurvivors() throws SQLException{
            PreparedStatement preparedStatement = conn.prepareStatement("CREATE VIEW IF NOT EXISTS number_of_survivors AS SELECT count(survived) FROM '"+tableName+"' WHERE survived = 1");
            preparedStatement.execute();
    }

    /**
     * Creates a view with the number of people that survived or didn't from each class
     * @throws SQLException If this view cannot be created an exception will be thrown, this will be to do with an issue
     *                      with the table name
     */
    private static void createViewSurvivorCountByClass() throws SQLException{
            PreparedStatement preparedStatement = conn.prepareStatement("CREATE VIEW IF NOT EXISTS survivor_count AS SELECT pClass,survived,count(survived) FROM "+tableName+" GROUP BY pClass,survived");
            preparedStatement.execute();
    }

    /**
     * Creates a view with the entire table in it
     * @throws SQLException If this view cannot be created an exception will be thrown, this will be to do with an issue
     *                      with the table name
     */
    private static void createViewAllRecords() throws SQLException{
        PreparedStatement preparedStatement = conn.prepareStatement("CREATE VIEW IF NOT EXISTS all_passengers AS SELECT * FROM '"+tableName+"'");
        preparedStatement.execute();
    }

    /**
     * Calls methods that create the database file and then initialize and populate the table within it. It also sets the
     * views that can be selected form the command line
     */
    private static void createAndPopulateDatabase() {
        try {
            createDatabase(dbPath);
            createTable(csvFile);
            populateTable();
            setViews();
        } catch (SQLException e) {
            System.out.println("Unable to create or populate database");
            System.exit(0);
        }
        System.out.println("OK");
    }

    /**
     * Calls all of the methods to create the views and handles the SQLException that could occur
     */
    private static void setViews() {
        getDatabaseInformation();
        try {
            createViewAllRecords();
            createViewMinimumSurvivorAge();
            createViewSurvivorCountByClass();
            createViewNumberOfSurvivors();
        } catch (SQLException e) {
            System.out.println("Unable to create views");
            System.exit(0);
        }
    }

    /**
     * Sets the variables that define how to access the database
     */
    private static void getDatabaseInformation() {
        try {
            database = new File(dbPath);
            dbFileName = database.getName();
            dbURL = "jdbc:sqlite:" + dbFileName;
            conn = DriverManager.getConnection(dbURL);
        } catch (SQLException e) {
            System.out.println("Unable to obtain database information");
        }
    }

    /**
     * Initially creates the database file and sets the variables that state how to connect to it
     * @param dbPath The absolute path to the database
     * @throws SQLException If a connection cannot be made to the database then an exception will be thrown
     */
    private static void createDatabase(String dbPath) throws SQLException {
        database = new File(dbPath);
        dbFileName = database.getName();
        dbURL = "jdbc:sqlite:" + dbFileName;
        conn = DriverManager.getConnection(dbURL);
        try {
            database.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Creates the titanicpeople table which can then be populated
     * @param inputFile The database file
     * @throws SQLException SQLException will be thrown if the database cannot be connected to
     */
    private static void createTable(File inputFile) throws SQLException{
        String[] columnHeaders = getColumnHeaders();
        HashMap<String,String> attributeMap = setAttributes(columnHeaders);
        PreparedStatement preparedStatement = conn.prepareStatement("DROP TABLE IF EXISTS "+tableName);
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

    /**
     * Populates the table with all of the data records from the .csv file
     * @throws SQLException SQLException will be thrown if a connection cannot be made to the database
     */
    private static void populateTable() throws SQLException{
        PreparedStatement preparedStatement = null;
        ArrayList<String[]> records = readFile();
        for (int i = 0; i < records.size(); i++) {
            String queryString = "INSERT INTO "+tableName+" VALUES ("+records.get(i)[0]+","+records.get(i)[1]+","+records.get(i)[2]+","+records.get(i)[3]+
                    ","+records.get(i)[4]+","+records.get(i)[5]+","+records.get(i)[6]+","+records.get(i)[7]+","+records.get(i)[8]+","+records.get(i)[9]+","+records.get(i)[10]+","+records.get(i)[11]+")";
            preparedStatement = conn.prepareStatement(queryString);
            preparedStatement.executeUpdate();
        }
        preparedStatement.close();
    }

    /**
     * This method sets the types for the attributes of the database. This is not do much at the moment but in theory
     * if there was a way of working out what type the inputs were from the .csv file then they can be set here and be
     * used when creating the table
     * @param attributeArray The attributes to be put into the database so that types can be assigned to them
     * @return Attribute map mapping each attribute to its type
     */
    private static HashMap<String,String> setAttributes(String[] attributeArray) {
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
        attributeMap.put(attributeArray[9],"FLOAT");
        attributeMap.put(attributeArray[10],"STRING");
        attributeMap.put(attributeArray[11],"CHAR");
        return attributeMap;
    }


    private static ArrayList<String[]> readFile() {
        ArrayList<String[]> allRecords = new ArrayList();
        try (Scanner inputStream = new Scanner(csvFile)) {
            int i = 0;
            while (inputStream.hasNext()) {
                String data = inputStream.nextLine();
                if (data.substring(data.length()-1).equals(",")) {
                    data = data + "NULL";
                }
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

    private static String[] sanitiseRecord(String[] record) {
        for (int i = 0; i < record.length; i++) {
            record[i] = record[i].replaceAll("'","''");
        }
        record[3] = "'"+record[3]+"'";
        record[4] = "'"+record[4]+"'";
        record[8] = "'"+record[8]+"'";
        record[10] = "'"+record[10]+"'";
        record[11] = "'"+record[11]+"'";
        for (int i = 0; i < record.length; i++) {
            if (record[i].equals("") || record[i].equals("''") || record[i].equals("'NULL'")) {
                record[i] = "NULL"; 
            }
        }
        return record;
    }

    private static String[] getColumnHeaders() {
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
