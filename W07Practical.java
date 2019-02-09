import java.io.*;
import java.sql.*;
import java.util.Properties;

public class W07Practical {
    private final int stockCodeIndex = 1;   // Global variables so we don't have to use magic numbers later
    private final int descriptionIndex = 2;
    private final int quantityIndex = 3;
    private final int unitPriceIndex = 5;
    private final int countryIndex = 7;

    private final int csvColumnNo = 8;
    private final String splitChar = ",";

    public static void main(String[] args) {

        if (args.length != 3) {
            System.out.println("Usage: java -cp <mariadb-client.jar>:. W07Practical <DB_properties_file> <input_file> <action>");
            System.exit(0);
        }

        String propertiesFile = args[0];
        String input = args[1];
        String query = args[2];

        W07Practical prac = new W07Practical();

        try {
            if (query.equals("create")) {
                prac.writeToDB(propertiesFile, input);
            } else if (query.equals("query1") || query.equals("query2") || query.equals("query3") || query.equals("query4")) {
                prac.queryDB(propertiesFile, query);
            } else {
                System.out.println("Please input valid action");
                System.exit(0);
            }
        } catch (IOException e) {
            System.out.println("IO Exception: make sure input and db.properties file exist and are in correct format");
            System.exit(1);
        } catch (SQLException e) {
            System.out.println("SQL Exception: make sure database in correct format");
            System.exit(1); // Any exception makes our data incorrect or inconsistent; because of this we throw all exceptions to main method and then end program
        }
    }

    // Creates suitable tables then reads file
    private void writeToDB(String propertiesFile, String input) throws SQLException, IOException {
        Connection conn = connectToDB(propertiesFile);
        BufferedReader br = new BufferedReader(new FileReader(input));

        createTables(conn);
        readAndInsertFile(input, conn, br);
    }

    // Gets properties needed for connection then connects
    private Connection connectToDB(String propertiesFile) throws IOException, SQLException { // Code adapted from examples on studres
        Properties properties = getProperties(propertiesFile);

        String url = getDBUrl(properties);
        String username = properties.getProperty("username");
        String password = properties.getProperty("password");

        Connection conn = DriverManager.getConnection(url, username, password);
        return conn;
    }

    // See above
    private Properties getProperties(String propertiesFile) throws IOException { // Code adapted from examples on studres
        FileInputStream stream = new FileInputStream(propertiesFile);

        Properties properties = new Properties();
        properties.load(stream);

        stream.close();

        return properties;
    }

    // See above
    private String getDBUrl(Properties properties) { // Code adapted from examples on studres
        String type = properties.getProperty("type");
        String host = properties.getProperty("host");
        String port = properties.getProperty("port");
        String db = properties.getProperty("db");

        String url = "jdbc:" + type + "://" + host + ":" + port + "/" + db;

        return url;
    }

    // Defines some string literals then executes them on the DB
    private void createTables(Connection conn) throws SQLException {
        Statement statement = conn.createStatement();

        String table1 = "CREATE TABLE invoices (InvoiceNo VARCHAR(100), StockCode VARCHAR(100), Quantity int, InvoiceDate VARCHAR(100), " +
                "UnitPrice double, CustomerID VARCHAR(100), Country VARCHAR(100))";
        String table2 = "CREATE TABLE stockcodes (StockCode VARCHAR(100), Description VARCHAR(100))";

        statement.executeUpdate("DROP TABLE IF EXISTS invoices");
        statement.executeUpdate("DROP TABLE IF EXISTS stockcodes");

        statement.executeUpdate(table2);
        statement.executeUpdate(table1);

        statement.close();
    }

    // Reads file in line-by-line and inserts it into the DB using prepared statements
    private void readAndInsertFile(String input, Connection conn, BufferedReader br) throws IOException, SQLException {
        br.readLine(); // To skip document header
        String line = "";

        String[] elements = new String[csvColumnNo];
        conn.setAutoCommit(false);

        while ((line = br.readLine()) != null) {
            elements = line.split(splitChar);
            checkErrors(elements);
            insertLineUsingPreparedStatement(elements, conn);
        }
        conn.commit();
        System.out.println("OK");
    }

    // Multiple if statements because we want to add variables to DB differently depending on type
    private void insertLineUsingPreparedStatement(String[] elements, Connection conn) throws SQLException {
        String stockCode = "";
        String description = "";
        String insertString = "INSERT INTO invoices VALUES (?, ?, ?, ?, ?, ?, ?)";


        PreparedStatement statement = conn.prepareStatement(insertString);
        for (int i = 1, j = 0; j < csvColumnNo; i++, j++) {
            if (j == descriptionIndex) { // This would have been more elegant as a switch statement; unfortunately Java doesn't let us use variables as cases
                description = elements[j]; // Because we want to skip adding description to our first table; instead we add to auxiliary table in method below
                i--;
            } else if (j == quantityIndex) {
                int quantity = Integer.parseInt(elements[j]);
                statement.setInt(i, quantity);
            } else if (j == unitPriceIndex) {
                double unitPrice = Double.parseDouble(elements[j]);
                statement.setDouble(i, unitPrice);
            } else if (j == stockCodeIndex) {
                stockCode = elements[j];
                statement.setString(i, elements[j]); // Same as for description
            } else {
                statement.setString(i, elements[j]);
            }
        }
        writeToStockTable(stockCode, description, conn);

        statement.executeUpdate();
        statement.close();
    }

    // Inserts the values we need into our auxiliary stocktable
    private void writeToStockTable(String stockCode, String description, Connection conn) throws SQLException {
        String insertString = "INSERT INTO stockcodes VALUES (?, ?)";

        PreparedStatement statement = conn.prepareStatement(insertString);

        statement.setString(1, stockCode);
        statement.setString(2, description);

        statement.executeUpdate();
        statement.close();
    }

    // Takes query and calls appropriate method
    private void queryDB(String propertiesFile, String query) throws SQLException, IOException {
        Connection conn = connectToDB(propertiesFile);

        switch (query) {
            case "query1":
                executeQuery1(conn);
                break;
            case "query2":
                executeQuery2(conn);
                break;
            case "query3":
                executeQuery3(conn);
                break;
            case "query4":
                executeQuery4(conn);
        }
    }

    // Print all records in db
    private void executeQuery1(Connection conn) throws SQLException {
        String insertString = "select InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country from invoices natural join stockcodes";
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery(insertString);
        printResultSet(rs, 1); // Call print method with different int value depending on what our header needs to be

    }

    // Prints number of invoices
    private void executeQuery2(Connection conn) throws SQLException {
        String insertString = "select InvoiceNo from invoices group by `InvoiceNo`"; // Since we just need the number of rows
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery(insertString);

        int rowNo = 0;
        while(rs.next()) {
            rowNo++;
        }

        printQuery2(rowNo);
    }

    private void printQuery2(int rowNo) {
        System.out.println("Number of Invoices");
        System.out.println(rowNo);
    }

    // Prints invoices and their total prices
    // Calls method which creates a new view and returns result set; prints it
    private void executeQuery3(Connection conn) throws SQLException {
        ResultSet rs = createTotalPriceView(conn);
        printResultSet(rs, 3);
    }

    // Prints highest-valued invoice and its total price
    // Uses same view method we called above
    private void executeQuery4(Connection conn) throws SQLException {
        ResultSet rs = createTotalPriceView(conn);
        String insertString = "select * from totalprice order by Total_Price DESC LIMIT 1"; // Orders view by total price in descending order, takes first element
        Statement statement = conn.createStatement();

        rs = statement.executeQuery(insertString);
        printResultSet(rs, 4);
    }

    // Creates view we use above, then selects it and returns according ResultSet; drops it first if there's one existing by the same name
    private ResultSet createTotalPriceView(Connection conn) throws SQLException {
        String insertString = "create view totalprice as select InvoiceNo, sum(Quantity * UnitPrice) as Total_Price from invoices group by `InvoiceNo`";

        Statement statement = conn.createStatement();
        statement.executeUpdate("DROP VIEW IF EXISTS totalprice");

        statement.executeQuery(insertString);
        insertString = "select * from totalprice";
        ResultSet rs = statement.executeQuery(insertString);

        return rs;
    }

    // Gets ResultSet metadata so we print column headers appropriately
    private void printResultSetHeaders(ResultSetMetaData rsmd) throws SQLException { // Syntax for result set metadata from http://stackoverflow.com/questions/696782/retrieve-column-names-from-java-sql-resultset
        int columnCount = rsmd.getColumnCount();
        StringBuilder sb = new StringBuilder();

        for (int i = 1; i <= columnCount; i++) {
            String element = rsmd.getColumnName(i);
            sb.append(element);
            sb.append(", ");
        }
        sb.setLength(sb.length() - 2); // deletes last ", "
        String print = sb.toString();
        System.out.println(print);
    }

    private void printResultSet(ResultSet rs, int needHeader) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();

        if (needHeader == 1) {
            printResultSetHeaders(rsmd);
        } else if (needHeader == 3) {
            System.out.println("InvoiceNo, Total Price");  // We can't make mariadb columns with a space in their name (it can only be "Total_Price")
        } else if (needHeader == 4) {
            System.out.println("InvoiceNo, Maximum Total Price");
        }

        int columnCount = rsmd.getColumnCount();
        while (rs.next()) {
            StringBuilder sb = new StringBuilder();
            for (int j = 1 ; j <= columnCount; j++) {
                String element = rs.getString(j);
                sb.append(element);
                sb.append(", ");
            }
            sb.setLength(sb.length() - 2); // deletes last ", "
            String print = sb.toString();
            System.out.println(print);
        }
    }

    // This and method below check if either UnitPrice or Quantity are blank strings, sets them to "0" if so
    private void checkErrors(String[] elements) { // Adapted from extension of week 03 practical
        for (int i = 0; i < csvColumnNo; i++) {
            if (elements[i].equals("")) {
                handleError(elements, i);
            }
        }
    }

    private void handleError(String[] elements, int errorCode) { // Adapted from extension of week 03 practical
        String errorString1 = "";
        String errorString2 = "blank";
        switch (errorCode) {
            case 3:
                errorString1 = "Quantity";
                errorString2 = "0";
                elements[quantityIndex] = "0";
                break;
            case 5:
                errorString1 = "Unit Price";
                errorString2 = "0";
                elements[unitPriceIndex] = "0";
                break;
        }
    }



}
