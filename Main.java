// Main.java  (updated: enforce one salary per agent; each salary may be shared by many agents)
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Scanner;

public class Main
{
    // ----------------------------
    // Entry point
    // ----------------------------
    public static void main(String[] args)
    {
        String databaseUrl = "jdbc:sqlite:libraryDB.db";
        String scriptFilePath = "user_inputs.txt";
        String[] scriptLines = readFileLines(scriptFilePath);

        try (Connection connection = DriverManager.getConnection(databaseUrl))
        {
            Statement statement = connection.createStatement();

            // You might wanna use this.
            dropAllTables(statement);
            initializeTables(statement);

            if (scriptLines != null)
            {
                // silentMode = true so script lines do not print while running
                executeScript(scriptLines, statement, true);
            }

            menuLoop(statement, new Scanner(System.in));
        }
        catch (SQLException exception)
        {
            System.out.println("Database connection error: " + exception.toString());
        }
    }

    // ----------------------------
    // Read file lines (two-pass, no advanced APIs)
    // ----------------------------
    public static String[] readFileLines(String filePath)
    {
        try
        {
            FileReader firstReader = new FileReader(filePath);
            Scanner firstScanner = new Scanner(firstReader);

            int lineCount = 0;

            while (firstScanner.hasNextLine())
            {
                firstScanner.nextLine();
                lineCount = lineCount + 1;
            }

            firstScanner.close();
            firstReader.close();

            FileReader secondReader = new FileReader(filePath);
            Scanner secondScanner = new Scanner(secondReader);

            String[] lines = new String[lineCount];

            int lineIndex = 0;

            while (secondScanner.hasNextLine())
            {
                lines[lineIndex] = secondScanner.nextLine();
                lineIndex = lineIndex + 1;
            }

            secondScanner.close();
            secondReader.close();

            return lines;
        }
        catch (IOException exception)
        {
            System.out.println("Unable to read file: " + filePath);
            System.out.println(exception.toString());
            return null;
        }
    }

    // ----------------------------
    // Execute script lines (silent for script execution)
    // ----------------------------
    public static void executeScript(String[] scriptLines, Statement statement, boolean silentMode)
    {
        if (scriptLines == null) return;

        int lineIndex = 0;

        while (lineIndex < scriptLines.length)
        {
            String rawLine = scriptLines[lineIndex];

            if (rawLine == null)
            {
                lineIndex = lineIndex + 1;
                continue;
            }

            String trimmedLine = trimSimple(rawLine);

            if (trimmedLine.length() == 0)
            {
                lineIndex = lineIndex + 1;
                continue;
            }

            if (trimmedLine.charAt(0) == '#')
            {
                lineIndex = lineIndex + 1;
                continue;
            }

            // Allow a raw SQL passthrough line starting with "SQL|"
            if (trimmedLine.length() > 4 && trimmedLine.substring(0,4).equals("SQL|"))
            {
                String rawSql = trimmedLine.substring(4);
                try
                {
                    statement.executeUpdate(rawSql);
                }
                catch (SQLException e)
                {
                    // suppressed in silent script mode
                }
                lineIndex = lineIndex + 1;
                continue;
            }

            String[] tokens = splitByPipe(trimmedLine);

            if (tokens.length == 0)
            {
                lineIndex = lineIndex + 1;
                continue;
            }

            String command = tokens[0];

            try
            {
                if (command.equals("0"))
                {
                    // silent stop
                    break;
                }

                else if (command.equals("1")) // Register
                {
                    if (tokens.length < 2)
                    {
                        lineIndex = lineIndex + 1;
                        continue;
                    }

                    String registerSubtype = tokens[1];

                    // 1|1|Name|PhoneNumber|Experience   -> Agents
                    if (registerSubtype.equals("1"))
                    {
                        if (tokens.length != 5)
                        {
                            lineIndex = lineIndex + 1;
                            continue;
                        }

                        String name = tokens[2];
                        String phoneNumber = tokens[3];
                        String experienceString = tokens[4];

                        if (!isInteger(experienceString))
                        {
                            lineIndex = lineIndex + 1;
                            continue;
                        }

                        String[] columns = {"Name", "PhoneNumber", "Experience"};
                        String[] values = {name, phoneNumber, experienceString};
                        int newAgentId = insertIntoTableAndReturnId("Agents", columns, values, statement);

                        int experienceYears = stringToInt(experienceString);
                        if (experienceYears >= 10)
                        {
                            addBenefitsToAgent(statement, newAgentId, 0, 14);
                        }
                    }

                    // 1|2|BookName|AuthorID|Cost  -> Books + BookAuthor
                    else if (registerSubtype.equals("2"))
                    {
                        if (tokens.length != 5)
                        {
                            lineIndex = lineIndex + 1;
                            continue;
                        }

                        String bookName = tokens[2];
                        String authorIdString = tokens[3];
                        String costString = tokens[4];

                        if (!isInteger(authorIdString) || !isInteger(costString))
                        {
                            lineIndex = lineIndex + 1;
                            continue;
                        }

                        String[] columns = {"AuthorID", "Name", "Cost"};
                        String[] values = {authorIdString, bookName, costString};
                        int newBookId = insertIntoTableAndReturnId("Books", columns, values, statement);

                        if (stringToInt(authorIdString) != 0)
                        {
                            String[] linkColumns = {"BookID", "AuthorID"};
                            String[] linkValues = {integerToString(newBookId), authorIdString};
                            insertIntoTable("BookAuthor", linkColumns, linkValues, statement);
                        }
                    }

                    // 1|3|Name|PhoneNumber|HourlyCharge -> Authors
                    else if (registerSubtype.equals("3"))
                    {
                        if (tokens.length != 5)
                        {
                            lineIndex = lineIndex + 1;
                            continue;
                        }

                        String name = tokens[2];
                        String phoneNumber = tokens[3];
                        String hourlyChargeString = tokens[4];

                        if (!isInteger(hourlyChargeString))
                        {
                            lineIndex = lineIndex + 1;
                            continue;
                        }

                        String[] columns = {"Name", "PhoneNumber", "HourlyCharge"};
                        String[] values = {name, phoneNumber, hourlyChargeString};
                        insertIntoTable("Authors", columns, values, statement);
                    }

                    // 1|4|AgentID|BookID|CustomerName|Phone -> Customers + CustomerAgentBook
                    else if (registerSubtype.equals("4"))
                    {
                        if (tokens.length != 6)
                        {
                            lineIndex = lineIndex + 1;
                            continue;
                        }

                        String agentIdString = tokens[2];
                        String bookIdString = tokens[3];
                        String customerName = tokens[4];
                        String customerPhone = tokens[5];

                        if (!isInteger(agentIdString) || !isInteger(bookIdString))
                        {
                            lineIndex = lineIndex + 1;
                            continue;
                        }

                        String[] columns = {"AgentID", "BookID", "Name", "PhoneNumber"};
                        String[] values = {agentIdString, bookIdString, customerName, customerPhone};
                        int newCustomerId = insertIntoTableAndReturnId("Customers", columns, values, statement);

                        String[] linkCols = {"CustomerID", "AgentID", "BookID"};
                        String[] linkVals = {integerToString(newCustomerId), agentIdString, bookIdString};
                        insertIntoTable("CustomerAgentBook", linkCols, linkVals, statement);
                    }

                    // 1|5|Salary|Experience|AgentID(optional) -> Salaries and optional AgentSalary link
                    else if (registerSubtype.equals("5"))
                    {
                        // allowed formats:
                        // 1|5|Salary|Experience
                        // 1|5|Salary|Experience|AgentID
                        if (tokens.length != 4 && tokens.length != 5)
                        {
                            lineIndex = lineIndex + 1;
                            continue;
                        }

                        String salaryString = tokens[2];
                        String experienceString = tokens[3];
                        String agentIdString = "0";
                        if (tokens.length == 5) agentIdString = tokens[4];

                        if (!isInteger(salaryString) || !isInteger(experienceString) || !isInteger(agentIdString))
                        {
                            lineIndex = lineIndex + 1;
                            continue;
                        }

                        String[] columns = {"Salary", "Experience"};
                        String[] values = {salaryString, experienceString};
                        int newSalaryId = insertIntoTableAndReturnId("Salaries", columns, values, statement);

                        int linkAgentId = stringToInt(agentIdString);
                        if (linkAgentId != 0)
                        {
                            // addAgentSalaryLink will only link if the agent has no existing salary link
                            addAgentSalaryLink(statement, linkAgentId, newSalaryId);
                        }
                    }

                    else
                    {
                        lineIndex = lineIndex + 1;
                        continue;
                    }
                }

                // UPDATE: 2|table|id|column|newValue
                else if (command.equals("2"))
                {
                    if (tokens.length != 5)
                    {
                        lineIndex = lineIndex + 1;
                        continue;
                    }

                    String tableToken = tokens[1];
                    String idString = tokens[2];
                    String columnName = tokens[3];
                    String newValue = tokens[4];

                    if (!isInteger(idString))
                    {
                        lineIndex = lineIndex + 1;
                        continue;
                    }

                    String tableName = null;
                    String idColumnName = null;

                    if (tableToken.equals("1"))
                    {
                        tableName = "Agents";
                        idColumnName = "AgentID";
                    }
                    else if (tableToken.equals("2"))
                    {
                        tableName = "Books";
                        idColumnName = "BookID";
                    }
                    else if (tableToken.equals("3"))
                    {
                        tableName = "Authors";
                        idColumnName = "AuthorID";
                    }
                    else if (tableToken.equals("4"))
                    {
                        tableName = "Customers";
                        idColumnName = "CustomerID";
                    }
                    else if (tableToken.equals("5"))
                    {
                        tableName = "Salaries";
                        idColumnName = "SalaryID";
                    }
                    else
                    {
                        lineIndex = lineIndex + 1;
                        continue;
                    }

                    boolean numericColumn = false;
                    if (tableName.equals("Agents") && columnName.equals("Experience")) numericColumn = true;
                    if (tableName.equals("Books") && (columnName.equals("AuthorID") || columnName.equals("Cost"))) numericColumn = true;
                    if (tableName.equals("Authors") && columnName.equals("HourlyCharge")) numericColumn = true;
                    if (tableName.equals("Customers") && (columnName.equals("AgentID") || columnName.equals("BookID"))) numericColumn = true;
                    if (tableName.equals("Salaries") && (columnName.equals("Salary") || columnName.equals("Experience"))) numericColumn = true;

                    if (numericColumn && !isInteger(newValue))
                    {
                        lineIndex = lineIndex + 1;
                        continue;
                    }

                    String updateSql = "UPDATE " + tableName + " SET " + columnName + " = ";
                    if (numericColumn) updateSql = updateSql + newValue;
                    else updateSql = updateSql + "'" + escapeSingleQuotes(newValue) + "'";
                    updateSql = updateSql + " WHERE " + idColumnName + " = " + idString + ";";

                    try
                    {
                        int updatedRowCount = statement.executeUpdate(updateSql);

                        // Special handling: if we updated customer's AgentID or BookID adjust CustomerAgentBook link
                        if (tableName.equals("Customers") && (columnName.equals("AgentID") || columnName.equals("BookID")))
                        {
                            // Recreate CustomerAgentBook link for this Customer row
                            // First remove existing links for this customer
                            int customerId = stringToInt(idString);
                            statement.executeUpdate("DELETE FROM CustomerAgentBook WHERE CustomerID = " + customerId + ";");

                            // Retrieve new AgentID and BookID to reinsert link if both present
                            ResultSet rs = statement.executeQuery("SELECT AgentID, BookID FROM Customers WHERE CustomerID = " + customerId + ";");
                            if (rs.next())
                            {
                                int newAgent = rs.getInt("AgentID");
                                int newBook = rs.getInt("BookID");
                                // insert link even if agent or book is 0 (keeps consistency)
                                String[] linkCols = {"CustomerID", "AgentID", "BookID"};
                                String[] linkVals = {integerToString(customerId), integerToString(newAgent), integerToString(newBook)};
                                insertIntoTable("CustomerAgentBook", linkCols, linkVals, statement);
                            }
                            rs.close();
                        }

                        // Special handling: if we updated agents.Experience then maintain benefits
                        if (tableName.equals("Agents") && columnName.equals("Experience"))
                        {
                            int newExperienceYears = stringToInt(newValue);
                            int agentId = stringToInt(idString);
                            if (newExperienceYears >= 10)
                            {
                                if (!agentHasBenefits(statement, agentId))
                                {
                                    addBenefitsToAgent(statement, agentId, 0, 14);
                                }
                            }
                            else
                            {
                                if (agentHasBenefits(statement, agentId))
                                {
                                    removeBenefitsFromAgent(statement, agentId);
                                }
                            }
                        }
                    }
                    catch (SQLException exception)
                    {
                        // suppress script update errors
                    }
                }

                // PURGE (delete): 3|table|id
                else if (command.equals("3"))
                {
                    if (tokens.length != 3)
                    {
                        lineIndex = lineIndex + 1;
                        continue;
                    }

                    String tableToken = tokens[1];
                    String idString = tokens[2];

                    if (!isInteger(idString))
                    {
                        lineIndex = lineIndex + 1;
                        continue;
                    }

                    if (tableToken.equals("1"))
                    {
                        int agentId = stringToInt(idString);
                        cascadeDeleteAgent(statement, agentId);
                    }
                    else if (tableToken.equals("2"))
                    {
                        int bookId = stringToInt(idString);
                        cascadeDeleteBook(statement, bookId);
                    }
                    else if (tableToken.equals("3"))
                    {
                        int authorId = stringToInt(idString);
                        cascadeDeleteAuthor(statement, authorId);
                    }
                    else if (tableToken.equals("4"))
                    {
                        int customerId = stringToInt(idString);
                        cascadeDeleteCustomer(statement, customerId);
                    }
                    else if (tableToken.equals("5"))
                    {
                        int salaryId = stringToInt(idString);
                        cascadeDeleteSalary(statement, salaryId);
                    }
                }

                // DISPLAY: 4|tableToken  (note: script display tokens may differ from interactive)
                else if (command.equals("4"))
                {
                    if (tokens.length != 2)
                    {
                        lineIndex = lineIndex + 1;
                        continue;
                    }

                    String tableToken = tokens[1];
                    String tableName = null;

                    if (tableToken.equals("1")) tableName = "Agents";
                    else if (tableToken.equals("2")) tableName = "Books";
                    else if (tableToken.equals("3")) tableName = "Authors";
                    else if (tableToken.equals("4")) tableName = "Customers";
                    else if (tableToken.equals("5")) tableName = "Salaries";
                    else if (tableToken.equals("6")) tableName = "WorkBenefits";

                    else
                    {
                        lineIndex = lineIndex + 1;
                        continue;
                    }

                    if (!silentMode)
                    {
                        displayTable(statement, tableName);
                    }
                }

                // COMPUTE: 5|1 or 5|2 (kept as-is)
                else if (command.equals("5"))
                {
                    if (tokens.length != 2)
                    {
                        lineIndex = lineIndex + 1;
                        continue;
                    }

                    String sub = tokens[1];

                    if (sub.equals("1"))
                    {
                        String sql = "SELECT MIN(Experience) AS minExp, AVG(Experience) AS avgExp, MAX(Experience) AS maxExp FROM Agents;";
                        try
                        {
                            ResultSet rs = statement.executeQuery(sql);
                            if (rs.next())
                            {
                                String min = rs.getString("minExp");
                                String avg = rs.getString("avgExp");
                                String max = rs.getString("maxExp");
                                if (!silentMode)
                                {
                                    System.out.println("Agents Experience -> MIN: " + min + "  AVG: " + avg + "  MAX: " + max);
                                }
                            }
                            rs.close();
                        }
                        catch (SQLException ex) { /* suppressed */ }
                    }
                    else if (sub.equals("2"))
                    {
                        String sql = "SELECT MIN(Cost) AS minCost, AVG(Cost) AS avgCost, MAX(Cost) AS maxCost FROM Books;";
                        try
                        {
                            ResultSet rs = statement.executeQuery(sql);
                            if (rs.next())
                            {
                                String min = rs.getString("minCost");
                                String avg = rs.getString("avgCost");
                                String max = rs.getString("maxCost");
                                if (!silentMode)
                                {
                                    System.out.println("Books Cost -> MIN: " + min + "  AVG: " + avg + "  MAX: " + max);
                                }
                            }
                            rs.close();
                        }
                        catch (SQLException ex) { /* suppressed */ }
                    }
                }

                // DISPLAY COMBINATIONS: 6|1 or 6|2
                else if (command.equals("6"))
                {
                    if (tokens.length != 2)
                    {
                        lineIndex = lineIndex + 1;
                        continue;
                    }

                    String sub = tokens[1];

                    if (sub.equals("1"))
                    {
                        String sql = "SELECT Customers.CustomerID, Customers.Name AS CustomerName, Agents.AgentID, Agents.Name AS AgentName " +
                                "FROM Customers LEFT JOIN Agents ON Customers.AgentID = Agents.AgentID;";
                        try
                        {
                            ResultSet rs = statement.executeQuery(sql);
                            if (!silentMode)
                            {
                                String[] outputColumns = {"CustomerID", "CustomerName", "AgentID", "AgentName"};
                                int headerIndex = 0;
                                while (headerIndex < outputColumns.length)
                                {
                                    System.out.print(outputColumns[headerIndex]);
                                    if (headerIndex < outputColumns.length - 1) System.out.print(" | ");
                                    headerIndex = headerIndex + 1;
                                }
                                System.out.println();

                                while (rs.next())
                                {
                                    int columnIndex = 1;
                                    while (columnIndex <= outputColumns.length)
                                    {
                                        String value = rs.getString(columnIndex);
                                        if (value == null) value = "NULL";
                                        System.out.print(value);
                                        if (columnIndex < outputColumns.length) System.out.print(" | ");
                                        columnIndex = columnIndex + 1;
                                    }
                                    System.out.println();
                                }
                            }
                            rs.close();
                        }
                        catch (SQLException ex) { /* suppressed */ }
                    }
                    else if (sub.equals("2"))
                    {
                        String sql = "SELECT Agents.AgentID, Agents.Name AS AgentName, WorkBenefits.SalaryBonus, WorkBenefits.PaidLeaveDuration " +
                                "FROM Agents JOIN WorkBenefits ON Agents.AgentID = WorkBenefits.AgentID;";
                        try
                        {
                            ResultSet rs = statement.executeQuery(sql);
                            if (!silentMode)
                            {
                                String[] outputColumns = {"AgentID", "AgentName", "SalaryBonus", "PaidLeaveDuration"};
                                int headerIndex = 0;
                                while (headerIndex < outputColumns.length)
                                {
                                    System.out.print(outputColumns[headerIndex]);
                                    if (headerIndex < outputColumns.length - 1) System.out.print(" | ");
                                    headerIndex = headerIndex + 1;
                                }
                                System.out.println();

                                while (rs.next())
                                {
                                    int columnIndex = 1;
                                    while (columnIndex <= outputColumns.length)
                                    {
                                        String value = rs.getString(columnIndex);
                                        if (value == null) value = "NULL";
                                        System.out.print(value);
                                        if (columnIndex < outputColumns.length) System.out.print(" | ");
                                        columnIndex = columnIndex + 1;
                                    }
                                    System.out.println();
                                }
                            }
                            rs.close();
                        }
                        catch (SQLException ex) { /* suppressed */ }
                    }
                }
            }
            catch (Exception exception)
            {
                // suppressed for script execution
            }

            lineIndex = lineIndex + 1;
        }
    }

    // ----------------------------
    // Split and trim helpers
    // ----------------------------
    public static String[] splitByPipe(String inputString)
    {
        if (inputString == null) return new String[0];

        int pipeCount = 1;
        int charIndex = 0;
        while (charIndex < inputString.length())
        {
            if (inputString.charAt(charIndex) == '|') pipeCount = pipeCount + 1;
            charIndex = charIndex + 1;
        }

        String[] parts = new String[pipeCount];
        String currentToken = "";
        int partIndex = 0;
        charIndex = 0;
        while (charIndex < inputString.length())
        {
            char currentChar = inputString.charAt(charIndex);
            if (currentChar == '|')
            {
                parts[partIndex] = trimSimple(currentToken);
                partIndex = partIndex + 1;
                currentToken = "";
            }
            else
            {
                currentToken = currentToken + currentChar;
            }
            charIndex = charIndex + 1;
        }
        parts[partIndex] = trimSimple(currentToken);
        return parts;
    }

    public static String trimSimple(String text)
    {
        if (text == null) return "";
        int startIndex = 0;
        int endIndex = text.length() - 1;
        while (startIndex <= endIndex && text.charAt(startIndex) == ' ') startIndex = startIndex + 1;
        while (endIndex >= startIndex && text.charAt(endIndex) == ' ') endIndex = endIndex - 1;
        if (endIndex < startIndex) return "";
        String resultString = "";
        int charIndex = startIndex;
        while (charIndex <= endIndex)
        {
            resultString = resultString + text.charAt(charIndex);
            charIndex = charIndex + 1;
        }
        return resultString;
    }

    // ----------------------------
    // Table creation helpers
    // ----------------------------
    public static void createTable(String[] parameters, Statement statement)
    {
        if (parameters == null || parameters.length == 0) return;

        String creationSql = "CREATE TABLE IF NOT EXISTS " + parameters[0] + " (";
        int paramIndex = 1;
        while (paramIndex < parameters.length)
        {
            creationSql = creationSql + parameters[paramIndex];
            if (paramIndex < parameters.length - 1) creationSql = creationSql + ", ";
            paramIndex = paramIndex + 1;
        }
        creationSql = creationSql + ");";

        try
        {
            statement.execute(creationSql);
        }
        catch (SQLException exception)
        {
            System.out.println("Error creating table " + parameters[0] + ": " + exception.toString());
        }
    }

    public static void initializeTables(Statement statement)
    {
        createTable(new String[]{"Agents", "AgentID INTEGER PRIMARY KEY AUTOINCREMENT", "Name TEXT", "PhoneNumber TEXT", "Experience INTEGER"}, statement);

        createTable(new String[]{"Salaries", "SalaryID INTEGER PRIMARY KEY AUTOINCREMENT", "Salary INTEGER", "Experience INTEGER"}, statement);

        createTable(new String[]{"Books", "BookID INTEGER PRIMARY KEY AUTOINCREMENT", "AuthorID INTEGER", "Name TEXT", "Cost INTEGER"}, statement);

        createTable(new String[]{"Authors", "AuthorID INTEGER PRIMARY KEY AUTOINCREMENT", "Name TEXT", "PhoneNumber TEXT", "HourlyCharge INTEGER"}, statement);

        createTable(new String[]{"Customers", "CustomerID INTEGER PRIMARY KEY AUTOINCREMENT", "AgentID INTEGER", "BookID INTEGER", "Name TEXT", "PhoneNumber TEXT"}, statement);

        createTable(new String[]{"WorkBenefits", "BenefitID INTEGER PRIMARY KEY AUTOINCREMENT", "AgentID INTEGER", "SalaryBonus INTEGER", "PaidLeaveDuration INTEGER"}, statement);

        createTable(new String[]{"CustomerAgentBook", "Link INTEGER PRIMARY KEY AUTOINCREMENT", "CustomerID INTEGER", "AgentID INTEGER", "BookID INTEGER"}, statement);

        createTable(new String[]{"AgentSalary", "Link INTEGER PRIMARY KEY AUTOINCREMENT", "AgentID INTEGER", "SalaryID INTEGER"}, statement);

        createTable(new String[]{"AgentBenefit", "Link INTEGER PRIMARY KEY AUTOINCREMENT", "AgentID INTEGER", "BenefitID INTEGER"}, statement);

        createTable(new String[]{"BookAuthor", "Link INTEGER PRIMARY KEY AUTOINCREMENT", "BookID INTEGER", "AuthorID INTEGER"}, statement);
    }

    public static void dropTable(String tableName, Statement statement)
    {
        try
        {
            statement.execute("DROP TABLE IF EXISTS " + tableName + ";");
        }
        catch (SQLException exception)
        {
            System.out.println("Drop table error: " + exception.toString());
        }
    }

    public static void dropAllTables(Statement statement)
    {
        dropTable("CustomerAgentBook", statement);
        dropTable("AgentSalary", statement);
        dropTable("AgentBenefit", statement);
        dropTable("BookAuthor", statement);

        dropTable("Customers", statement);
        dropTable("Agents", statement);
        dropTable("Books", statement);
        dropTable("Authors", statement);
        dropTable("Salaries", statement);
        dropTable("WorkBenefits", statement);
    }

    // ----------------------------
    // Insert helpers
    // ----------------------------
    public static void insertIntoTable(String tableName, String[] columns, String[] values, Statement statement)
    {
        if (tableName == null) return;
        if (columns == null || values == null) return;
        if (columns.length != values.length) return;

        String sql = "INSERT INTO " + tableName + " (";
        int columnIndex = 0;
        while (columnIndex < columns.length)
        {
            sql = sql + columns[columnIndex];
            if (columnIndex < columns.length - 1) sql = sql + ", ";
            columnIndex = columnIndex + 1;
        }
        sql = sql + ") VALUES (";
        int valueIndex = 0;
        while (valueIndex < values.length)
        {
            String value = values[valueIndex];
            if (value == null) sql = sql + "NULL";
            else
            {
                if (isInteger(value)) sql = sql + value;
                else sql = sql + "'" + escapeSingleQuotes(value) + "'";
            }
            if (valueIndex < values.length - 1) sql = sql + ", ";
            valueIndex = valueIndex + 1;
        }
        sql = sql + ");";

        try
        {
            statement.executeUpdate(sql);
        }
        catch (SQLException exception)
        {
            System.out.println("Insert error into " + tableName + ": " + exception.toString());
        }
    }

    public static int insertIntoTableAndReturnId(String tableName, String[] columns, String[] values, Statement statement)
    {
        if (tableName == null) return -1;
        if (columns == null || values == null) return -1;
        if (columns.length != values.length) return -1;

        String insertSql = "INSERT INTO " + tableName + " (";
        int columnIndex = 0;
        while (columnIndex < columns.length)
        {
            insertSql = insertSql + columns[columnIndex];
            if (columnIndex < columns.length - 1) insertSql = insertSql + ", ";
            columnIndex = columnIndex + 1;
        }
        insertSql = insertSql + ") VALUES (";
        int valueIndex = 0;
        while (valueIndex < values.length)
        {
            String value = values[valueIndex];
            if (value == null) insertSql = insertSql + "NULL";
            else
            {
                if (isInteger(value)) insertSql = insertSql + value;
                else insertSql = insertSql + "'" + escapeSingleQuotes(value) + "'";
            }
            if (valueIndex < values.length - 1) insertSql = insertSql + ", ";
            valueIndex = valueIndex + 1;
        }
        insertSql = insertSql + ");";

        int newGeneratedId = -1;

        try
        {
            statement.executeUpdate(insertSql, Statement.RETURN_GENERATED_KEYS);
            ResultSet generatedKeysResultSet = statement.getGeneratedKeys();
            if (generatedKeysResultSet != null)
            {
                if (generatedKeysResultSet.next())
                {
                    newGeneratedId = generatedKeysResultSet.getInt(1);
                }
                generatedKeysResultSet.close();
            }
        }
        catch (SQLException exception)
        {
            System.out.println("Error inserting row and retrieving generated id: " + exception.toString());
        }

        return newGeneratedId;
    }

    // ----------------------------
    // Benefits helpers
    // ----------------------------
    public static void addBenefitsToAgent(Statement statement, int agentId, int salaryBonus, int paidLeaveDays)
    {
        String[] columns = {"AgentID", "SalaryBonus", "PaidLeaveDuration"};
        String[] values = {integerToString(agentId), integerToString(salaryBonus), integerToString(paidLeaveDays)};
        int benefitId = insertIntoTableAndReturnId("WorkBenefits", columns, values, statement);
        if (benefitId != -1)
        {
            String[] linkColumns = {"AgentID", "BenefitID"};
            String[] linkValues = {integerToString(agentId), integerToString(benefitId)};
            insertIntoTable("AgentBenefit", linkColumns, linkValues, statement);
        }
    }

    public static void removeBenefitsFromAgent(Statement statement, int agentId)
    {
        try
        {
            ResultSet benefitResultSet = statement.executeQuery("SELECT BenefitID FROM AgentBenefit WHERE AgentID = " + agentId + ";");
            while (benefitResultSet.next())
            {
                int benefitId = benefitResultSet.getInt("BenefitID");
                statement.executeUpdate("DELETE FROM WorkBenefits WHERE BenefitID = " + benefitId + ";");
            }
            benefitResultSet.close();
            statement.executeUpdate("DELETE FROM AgentBenefit WHERE AgentID = " + agentId + ";");
        }
        catch (SQLException exception)
        {
            System.out.println("Error removing benefits for agent " + agentId + ": " + exception.toString());
        }
    }

    public static boolean agentHasBenefits(Statement statement, int agentId)
    {
        try
        {
            ResultSet countResultSet = statement.executeQuery("SELECT COUNT(*) FROM AgentBenefit WHERE AgentID = " + agentId + ";");
            int linkCount = 0;
            if (countResultSet.next()) linkCount = countResultSet.getInt(1);
            countResultSet.close();
            if (linkCount > 0) return true;
            return false;
        }
        catch (SQLException exception)
        {
            return false;
        }
    }

    // ----------------------------
    // AgentSalary helpers (new behavior: each agent may have only one salary)
    // ----------------------------
    public static boolean agentHasSalary(Statement statement, int agentId)
    {
        try
        {
            ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM AgentSalary WHERE AgentID = " + agentId + ";");
            int count = 0;
            if (rs.next()) count = rs.getInt(1);
            rs.close();
            return count > 0;
        }
        catch (SQLException exception)
        {
            return false;
        }
    }

    // Adds a link Agent -> Salary only if agent has no existing salary link.
    // Returns true if link was added, false if agent already had a salary or on error.
    public static boolean addAgentSalaryLink(Statement statement, int agentId, int salaryId)
    {
        try
        {
            if (agentHasSalary(statement, agentId))
            {
                // Agent already has salary link; do not add another (Agent 1 -> Salary 1)
                return false;
            }

            String[] linkColumns = {"AgentID", "SalaryID"};
            String[] linkValues = {integerToString(agentId), integerToString(salaryId)};
            insertIntoTable("AgentSalary", linkColumns, linkValues, statement);
            return true;
        }
        catch (Exception exception)
        {
            return false;
        }
    }

    public static void removeAgentSalaryLinksForSalary(Statement statement, int salaryId)
    {
        try
        {
            statement.executeUpdate("DELETE FROM AgentSalary WHERE SalaryID = " + salaryId + ";");
        }
        catch (SQLException exception)
        {
            System.out.println("Error removing AgentSalary links for SalaryID " + salaryId + ": " + exception.toString());
        }
    }

    // ----------------------------
    // Cascade deletes
    // ----------------------------
    public static void cascadeDeleteAgent(Statement statement, int agentId)
    {
        try
        {
            ResultSet benefitResultSet = statement.executeQuery("SELECT BenefitID FROM AgentBenefit WHERE AgentID = " + agentId + ";");
            while (benefitResultSet.next())
            {
                int benefitId = benefitResultSet.getInt("BenefitID");
                statement.executeUpdate("DELETE FROM WorkBenefits WHERE BenefitID = " + benefitId + ";");
            }
            benefitResultSet.close();

            statement.executeUpdate("DELETE FROM AgentBenefit WHERE AgentID = " + agentId + ";");
            statement.executeUpdate("DELETE FROM AgentSalary WHERE AgentID = " + agentId + ";");
            statement.executeUpdate("DELETE FROM CustomerAgentBook WHERE AgentID = " + agentId + ";");
            statement.executeUpdate("UPDATE Customers SET AgentID = 0 WHERE AgentID = " + agentId + ";");
            statement.executeUpdate("DELETE FROM Agents WHERE AgentID = " + agentId + ";");
        }
        catch (SQLException exception)
        {
            System.out.println("Error deleting agent cascade: " + exception.toString());
        }
    }

    public static void cascadeDeleteBook(Statement statement, int bookId)
    {
        try
        {
            statement.executeUpdate("DELETE FROM BookAuthor WHERE BookID = " + bookId + ";");
            statement.executeUpdate("DELETE FROM CustomerAgentBook WHERE BookID = " + bookId + ";");
            statement.executeUpdate("UPDATE Customers SET BookID = 0 WHERE BookID = " + bookId + ";");
            statement.executeUpdate("DELETE FROM Books WHERE BookID = " + bookId + ";");
        }
        catch (SQLException exception)
        {
            System.out.println("Error deleting book cascade: " + exception.toString());
        }
    }

    public static void cascadeDeleteAuthor(Statement statement, int authorId)
    {
        try
        {
            statement.executeUpdate("DELETE FROM BookAuthor WHERE AuthorID = " + authorId + ";");
            statement.executeUpdate("UPDATE Books SET AuthorID = 0 WHERE AuthorID = " + authorId + ";");
            statement.executeUpdate("DELETE FROM Authors WHERE AuthorID = " + authorId + ";");
        }
        catch (SQLException exception)
        {
            System.out.println("Error deleting author cascade: " + exception.toString());
        }
    }

    public static void cascadeDeleteCustomer(Statement statement, int customerId)
    {
        try
        {
            statement.executeUpdate("DELETE FROM CustomerAgentBook WHERE CustomerID = " + customerId + ";");
            statement.executeUpdate("DELETE FROM Customers WHERE CustomerID = " + customerId + ";");
        }
        catch (SQLException exception)
        {
            System.out.println("Error deleting customer cascade: " + exception.toString());
        }
    }

    public static void cascadeDeleteSalary(Statement statement, int salaryId)
    {
        try
        {
            statement.executeUpdate("DELETE FROM AgentSalary WHERE SalaryID = " + salaryId + ";");
            statement.executeUpdate("DELETE FROM Salaries WHERE SalaryID = " + salaryId + ";");
        }
        catch (SQLException exception)
        {
            System.out.println("Error deleting salary cascade: " + exception.toString());
        }
    }

    // ----------------------------
    // Display table using hardcoded column list
    // ----------------------------
    public static String[] getColumnsForTable(String tableName)
    {
        if (tableName == null) return new String[0];

        if (tableName.equals("Agents")) return new String[]{"AgentID", "Name", "PhoneNumber", "Experience"};
        if (tableName.equals("Salaries")) return new String[]{"SalaryID", "Salary", "Experience"};
        if (tableName.equals("Books")) return new String[]{"BookID", "AuthorID", "Name", "Cost"};
        if (tableName.equals("Authors")) return new String[]{"AuthorID", "Name", "PhoneNumber", "HourlyCharge"};
        if (tableName.equals("Customers")) return new String[]{"CustomerID", "AgentID", "BookID", "Name", "PhoneNumber"};
        if (tableName.equals("WorkBenefits")) return new String[]{"BenefitID", "AgentID", "SalaryBonus", "PaidLeaveDuration"};
        if (tableName.equals("CustomerAgentBook")) return new String[]{"Link", "CustomerID", "AgentID", "BookID"};
        if (tableName.equals("AgentSalary")) return new String[]{"Link", "AgentID", "SalaryID"};
        if (tableName.equals("AgentBenefit")) return new String[]{"Link", "AgentID", "BenefitID"};
        if (tableName.equals("BookAuthor")) return new String[]{"Link", "BookID", "AuthorID"};

        return new String[0];
    }

    public static void displayTable(Statement statement, String tableName)
    {
        String[] columns = getColumnsForTable(tableName);

        if (columns == null || columns.length == 0)
        {
            System.out.println("No columns for table: " + tableName);
            return;
        }

        int headerIndex = 0;
        while (headerIndex < columns.length)
        {
            System.out.print(columns[headerIndex]);
            if (headerIndex < columns.length - 1) System.out.print(" | ");
            headerIndex = headerIndex + 1;
        }
        System.out.println();

        try
        {
            ResultSet resultSet = statement.executeQuery("SELECT * FROM " + tableName + ";");
            while (resultSet.next())
            {
                int columnIndex = 0;
                while (columnIndex < columns.length)
                {
                    String columnName = columns[columnIndex];
                    String columnValue = null;
                    try
                    {
                        columnValue = resultSet.getString(columnName);
                    }
                    catch (Exception exception)
                    {
                        try
                        {
                            columnValue = resultSet.getString(columnIndex + 1);
                        }
                        catch (Exception innerException)
                        {
                            columnValue = "NULL";
                        }
                    }
                    if (columnValue == null) columnValue = "NULL";
                    System.out.print(columnValue);
                    if (columnIndex < columns.length - 1) System.out.print(" | ");
                    columnIndex = columnIndex + 1;
                }
                System.out.println();
            }
            resultSet.close();
        }
        catch (SQLException exception)
        {
            System.out.println("Error displaying table " + tableName + ": " + exception.toString());
        }
    }

    // ----------------------------
    // Simple SQL literal escaping
    // ----------------------------
    public static String escapeSingleQuotes(String inputString)
    {
        if (inputString == null) return null;
        String resultString = "";
        int charIndex = 0;
        while (charIndex < inputString.length())
        {
            char currentChar = inputString.charAt(charIndex);
            if (currentChar == '\'') resultString = resultString + "''";
            else resultString = resultString + currentChar;
            charIndex = charIndex + 1;
        }
        return resultString;
    }

    // ----------------------------
    // Interactive menu and operations
    // ----------------------------
    public static void menuLoop(Statement statement, Scanner scanner)
    {
        String userInput = "";

        do
        {
            System.out.println();
            System.out.println("Publishing system menu:");
            System.out.println("1 - Register");
            System.out.println("2 - Update");
            System.out.println("3 - Purge");
            System.out.println("4 - Display");
            System.out.println("5 - Compute");
            System.out.println("6 - Joins");
            System.out.println("0 - Exit");

            userInput = scanner.nextLine();

            if (!validInput(userInput, new String[]{"1","2","3","4","5","6","0"}))
            {
                System.out.println("Enter 0-6:");
                userInput = scanner.nextLine();
            }

            if (userInput.equals("1")) registryFunction(statement, scanner);
            if (userInput.equals("2")) updateFunction(statement, scanner);
            if (userInput.equals("3")) purgeFunction(statement, scanner);
            if (userInput.equals("4")) displayFunction(statement, scanner);
            if (userInput.equals("5")) computationFunction(statement, scanner);
            if (userInput.equals("6")) displayCombinationFunction(statement, scanner);

        }
        while (!userInput.equals("0"));

        System.out.println("Goodbye.");
    }

    // ----------------------------
    // Registry (interactive)
    // ----------------------------
    public static void registryFunction(Statement statement, Scanner scanner)
    {
        System.out.println("1 - Register agent");
        System.out.println("2 - Register book");
        System.out.println("3 - Register author");
        System.out.println("4 - Register customer");
        System.out.println("5 - Register salary");

        String userChoice = scanner.nextLine();
        if (!validInput(userChoice, new String[]{"1","2","3","4","5"}))
        {
            System.out.println("Enter 1-5:");
            userChoice = scanner.nextLine();
        }

        if (userChoice.equals("1"))
        {
            System.out.println("Enter agent name:");
            String name = scanner.nextLine();
            System.out.println("Enter phone:");
            String phoneNumber = scanner.nextLine();
            System.out.println("Enter experience (years):");
            String experienceString = scanner.nextLine();
            while (!isInteger(experienceString))
            {
                System.out.println("Enter integer:");
                experienceString = scanner.nextLine();
            }
            String[] columns = {"Name","PhoneNumber","Experience"};
            String[] values = {name, phoneNumber, experienceString};
            int agentId = insertIntoTableAndReturnId("Agents", columns, values, statement);
            System.out.println("Agent registered.");
            int experienceYears = stringToInt(experienceString);
            if (experienceYears >= 10)
            {
                System.out.println("Assigning default work benefits.");
                addBenefitsToAgent(statement, agentId, 0, 14);
            }
        }

        if (userChoice.equals("2"))
        {
            System.out.println("Enter book name:");
            String bookName = scanner.nextLine();
            System.out.println("Enter author id (0 if unknown):");
            String authorIdString = scanner.nextLine();
            while (!isInteger(authorIdString))
            {
                System.out.println("Enter integer:");
                authorIdString = scanner.nextLine();
            }
            System.out.println("Enter cost:");
            String costString = scanner.nextLine();
            while (!isInteger(costString))
            {
                System.out.println("Enter integer:");
                costString = scanner.nextLine();
            }
            String[] columns = {"AuthorID","Name","Cost"};
            String[] values = {authorIdString, bookName, costString};
            int bookId = insertIntoTableAndReturnId("Books", columns, values, statement);
            System.out.println("Book registered.");
            if (stringToInt(authorIdString) != 0)
            {
                String[] linkColumns = {"BookID","AuthorID"};
                String[] linkValues = {integerToString(bookId), authorIdString};
                insertIntoTable("BookAuthor", linkColumns, linkValues, statement);
            }
        }

        if (userChoice.equals("3"))
        {
            System.out.println("Enter author name:");
            String name = scanner.nextLine();
            System.out.println("Enter phone:");
            String phoneNumber = scanner.nextLine();
            System.out.println("Enter hourly charge:");
            String hourlyChargeString = scanner.nextLine();
            while (!isInteger(hourlyChargeString))
            {
                System.out.println("Enter integer:");
                hourlyChargeString = scanner.nextLine();
            }
            String[] columns = {"Name","PhoneNumber","HourlyCharge"};
            String[] values = {name, phoneNumber, hourlyChargeString};
            insertIntoTable("Authors", columns, values, statement);
            System.out.println("Author registered.");
        }

        if (userChoice.equals("4"))
        {
            System.out.println("Enter agent id (0 if none):");
            String agentIdString = scanner.nextLine();
            while (!isInteger(agentIdString))
            {
                System.out.println("Enter integer:");
                agentIdString = scanner.nextLine();
            }
            System.out.println("Enter book id (0 if none):");
            String bookIdString = scanner.nextLine();
            while (!isInteger(bookIdString))
            {
                System.out.println("Enter integer:");
                bookIdString = scanner.nextLine();
            }
            System.out.println("Enter customer name:");
            String customerName = scanner.nextLine();
            System.out.println("Enter phone:");
            String customerPhone = scanner.nextLine();

            String[] columns = {"AgentID","BookID","Name","PhoneNumber"};
            String[] values = {agentIdString, bookIdString, customerName, customerPhone};
            int customerId = insertIntoTableAndReturnId("Customers", columns, values, statement);

            String[] linkCols = {"CustomerID","AgentID","BookID"};
            String[] linkVals = {integerToString(customerId), agentIdString, bookIdString};
            insertIntoTable("CustomerAgentBook", linkCols, linkVals, statement);

            System.out.println("Customer registered and linked.");
        }

        if (userChoice.equals("5"))
        {
            System.out.println("Enter salary amount (integer):");
            String salaryString = scanner.nextLine();
            while (!isInteger(salaryString))
            {
                System.out.println("Enter integer:");
                salaryString = scanner.nextLine();
            }
            System.out.println("Enter experience (years) for this salary (integer):");
            String experienceString = scanner.nextLine();
            while (!isInteger(experienceString))
            {
                System.out.println("Enter integer:");
                experienceString = scanner.nextLine();
            }
            System.out.println("Link this salary to agent id (0 if none):");
            String agentIdString = scanner.nextLine();
            while (!isInteger(agentIdString))
            {
                System.out.println("Enter integer:");
                agentIdString = scanner.nextLine();
            }

            String[] columns = {"Salary","Experience"};
            String[] values = {salaryString, experienceString};
            int salaryId = insertIntoTableAndReturnId("Salaries", columns, values, statement);
            System.out.println("Salary registered.");

            int agentId = stringToInt(agentIdString);
            if (agentId != 0)
            {
                if (agentHasSalary(statement, agentId))
                {
                    System.out.println("Agent " + agentId + " already has a salary assigned. Skipping link.");
                }
                else
                {
                    addAgentSalaryLink(statement, agentId, salaryId);
                    System.out.println("Salary linked to agent " + agentId + ".");
                }
            }
        }
    }

    // ----------------------------
    // Update (interactive)
    // ----------------------------
    public static void updateFunction(Statement statement, Scanner scanner)
    {
        System.out.println("1 - Edit agent");
        System.out.println("2 - Edit book");
        System.out.println("3 - Edit author");
        System.out.println("4 - Edit customer");
        System.out.println("5 - Edit salary");

        String userChoice = scanner.nextLine();
        if (!validInput(userChoice, new String[]{"1","2","3","4","5"}))
        {
            System.out.println("Enter 1-5:");
            userChoice = scanner.nextLine();
        }

        String tableName = null;
        String idColumnName = null;

        if (userChoice.equals("1")) { tableName = "Agents"; idColumnName = "AgentID"; }
        if (userChoice.equals("2")) { tableName = "Books"; idColumnName = "BookID"; }
        if (userChoice.equals("3")) { tableName = "Authors"; idColumnName = "AuthorID"; }
        if (userChoice.equals("4")) { tableName = "Customers"; idColumnName = "CustomerID"; }
        if (userChoice.equals("5")) { tableName = "Salaries"; idColumnName = "SalaryID"; }

        System.out.println("Enter the ID (integer):");
        String idString = scanner.nextLine();
        while (!isInteger(idString))
        {
            System.out.println("Enter integer:");
            idString = scanner.nextLine();
        }

        if (tableName.equals("Agents"))
        {
            System.out.println("Which field? 1-Name 2-PhoneNumber 3-Experience");
            String choice = scanner.nextLine();
            while (!validInput(choice, new String[]{"1","2","3"}))
            {
                System.out.println("Enter 1-3:");
                choice = scanner.nextLine();
            }
            String columnName = null;
            if (choice.equals("1")) columnName = "Name";
            if (choice.equals("2")) columnName = "PhoneNumber";
            if (choice.equals("3")) columnName = "Experience";
            System.out.println("Enter new value:");
            String newValue = scanner.nextLine();
            if (columnName.equals("Experience"))
            {
                while (!isInteger(newValue))
                {
                    System.out.println("Enter integer:");
                    newValue = scanner.nextLine();
                }
            }
            String updateSql = "UPDATE " + tableName + " SET " + columnName + " = ";
            if (columnName.equals("Experience")) updateSql = updateSql + newValue;
            else updateSql = updateSql + "'" + escapeSingleQuotes(newValue) + "'";
            updateSql = updateSql + " WHERE " + idColumnName + " = " + idString + ";";
            try
            {
                statement.executeUpdate(updateSql);
                System.out.println("Agent updated.");
                if (columnName.equals("Experience"))
                {
                    int experienceYears = stringToInt(newValue);
                    int agentId = stringToInt(idString);
                    if (experienceYears >= 10 && !agentHasBenefits(statement, agentId))
                    {
                        addBenefitsToAgent(statement, agentId, 0, 14);
                        System.out.println("Benefits assigned.");
                    }
                    else if (experienceYears < 10 && agentHasBenefits(statement, agentId))
                    {
                        removeBenefitsFromAgent(statement, agentId);
                        System.out.println("Benefits removed.");
                    }
                }
            }
            catch (SQLException exception) { System.out.println("Update error: " + exception.toString()); }
        }

        if (tableName.equals("Books"))
        {
            System.out.println("Which field? 1-AuthorID 2-Name 3-Cost");
            String choice = scanner.nextLine();
            while (!validInput(choice, new String[]{"1","2","3"}))
            {
                System.out.println("Enter 1-3:");
                choice = scanner.nextLine();
            }
            String columnName = null;
            if (choice.equals("1")) columnName = "AuthorID";
            if (choice.equals("2")) columnName = "Name";
            if (choice.equals("3")) columnName = "Cost";
            System.out.println("Enter new value:");
            String newValue = scanner.nextLine();
            if (columnName.equals("AuthorID") || columnName.equals("Cost"))
            {
                while (!isInteger(newValue))
                {
                    System.out.println("Enter integer:");
                    newValue = scanner.nextLine();
                }
            }
            String updateSql = "UPDATE " + tableName + " SET " + columnName + " = ";
            if (columnName.equals("Name")) updateSql = updateSql + "'" + escapeSingleQuotes(newValue) + "'";
            else updateSql = updateSql + newValue;
            updateSql = updateSql + " WHERE " + idColumnName + " = " + idString + ";";
            try { statement.executeUpdate(updateSql); System.out.println("Book updated."); }
            catch (SQLException exception) { System.out.println("Update error: " + exception.toString()); }
        }

        if (tableName.equals("Authors"))
        {
            System.out.println("Which field? 1-Name 2-PhoneNumber 3-HourlyCharge");
            String choice = scanner.nextLine();
            while (!validInput(choice, new String[]{"1","2","3"}))
            {
                System.out.println("Enter 1-3:");
                choice = scanner.nextLine();
            }
            String columnName = null;
            if (choice.equals("1")) columnName = "Name";
            if (choice.equals("2")) columnName = "PhoneNumber";
            if (choice.equals("3")) columnName = "HourlyCharge";
            System.out.println("Enter new value:");
            String newValue = scanner.nextLine();
            if (columnName.equals("HourlyCharge"))
            {
                while (!isInteger(newValue))
                {
                    System.out.println("Enter integer:");
                    newValue = scanner.nextLine();
                }
            }
            String updateSql = "UPDATE " + tableName + " SET " + columnName + " = ";
            if (columnName.equals("HourlyCharge")) updateSql = updateSql + newValue;
            else updateSql = updateSql + "'" + escapeSingleQuotes(newValue) + "'";
            updateSql = updateSql + " WHERE " + idColumnName + " = " + idString + ";";
            try { statement.executeUpdate(updateSql); System.out.println("Author updated."); }
            catch (SQLException exception) { System.out.println("Update error: " + exception.toString()); }
        }

        if (tableName.equals("Customers"))
        {
            System.out.println("Which field? 1-AgentID 2-BookID 3-Name 4-PhoneNumber");
            String choice = scanner.nextLine();
            while (!validInput(choice, new String[]{"1","2","3","4"}))
            {
                System.out.println("Enter 1-4:");
                choice = scanner.nextLine();
            }
            String columnName = null;
            if (choice.equals("1")) columnName = "AgentID";
            if (choice.equals("2")) columnName = "BookID";
            if (choice.equals("3")) columnName = "Name";
            if (choice.equals("4")) columnName = "PhoneNumber";
            System.out.println("Enter new value:");
            String newValue = scanner.nextLine();
            if (columnName.equals("AgentID") || columnName.equals("BookID"))
            {
                while (!isInteger(newValue))
                {
                    System.out.println("Enter integer:");
                    newValue = scanner.nextLine();
                }
            }
            String updateSql = "UPDATE " + tableName + " SET " + columnName + " = ";
            if (columnName.equals("Name") || columnName.equals("PhoneNumber")) updateSql = updateSql + "'" + escapeSingleQuotes(newValue) + "'";
            else updateSql = updateSql + newValue;
            updateSql = updateSql + " WHERE " + idColumnName + " = " + idString + ";";
            try
            {
                statement.executeUpdate(updateSql);
                System.out.println("Customer updated.");
                // keep CustomerAgentBook link in sync
                int customerId = stringToInt(idString);
                statement.executeUpdate("DELETE FROM CustomerAgentBook WHERE CustomerID = " + customerId + ";");
                ResultSet rs = statement.executeQuery("SELECT AgentID, BookID FROM Customers WHERE CustomerID = " + customerId + ";");
                if (rs.next())
                {
                    int newAgent = rs.getInt("AgentID");
                    int newBook = rs.getInt("BookID");
                    String[] linkCols = {"CustomerID","AgentID","BookID"};
                    String[] linkVals = {integerToString(customerId), integerToString(newAgent), integerToString(newBook)};
                    insertIntoTable("CustomerAgentBook", linkCols, linkVals, statement);
                }
                rs.close();
            }
            catch (SQLException exception) { System.out.println("Update error: " + exception.toString()); }
        }

        if (tableName.equals("Salaries"))
        {
            System.out.println("Which field? 1-Salary 2-Experience");
            String choice = scanner.nextLine();
            while (!validInput(choice, new String[]{"1","2"}))
            {
                System.out.println("Enter 1-2:");
                choice = scanner.nextLine();
            }
            String columnName = null;
            if (choice.equals("1")) columnName = "Salary";
            if (choice.equals("2")) columnName = "Experience";
            System.out.println("Enter new value:");
            String newValue = scanner.nextLine();
            while (!isInteger(newValue))
            {
                System.out.println("Enter integer:");
                newValue = scanner.nextLine();
            }
            String updateSql = "UPDATE " + tableName + " SET " + columnName + " = " + newValue + " WHERE " + idColumnName + " = " + idString + ";";
            try { statement.executeUpdate(updateSql); System.out.println("Salary updated."); }
            catch (SQLException exception) { System.out.println("Update error: " + exception.toString()); }
        }
    }

    // ----------------------------
    // Purge (interactive)
    // ----------------------------
    public static void purgeFunction(Statement statement, Scanner scanner)
    {
        System.out.println("1 - Purge agent");
        System.out.println("2 - Purge book");
        System.out.println("3 - Purge author");
        System.out.println("4 - Purge customer");
        System.out.println("5 - Purge salary");

        String userChoice = scanner.nextLine();
        if (!validInput(userChoice, new String[]{"1","2","3","4","5"}))
        {
            System.out.println("Enter 1-5:");
            userChoice = scanner.nextLine();
        }

        System.out.println("Enter the ID to delete:");
        String idString = scanner.nextLine();
        while (!isInteger(idString))
        {
            System.out.println("Enter integer:");
            idString = scanner.nextLine();
        }

        if (userChoice.equals("1"))
        {
            cascadeDeleteAgent(statement, stringToInt(idString));
            System.out.println("Agent removed.");
            return;
        }

        if (userChoice.equals("2"))
        {
            cascadeDeleteBook(statement, stringToInt(idString));
            System.out.println("Book removed.");
            return;
        }

        if (userChoice.equals("3"))
        {
            cascadeDeleteAuthor(statement, stringToInt(idString));
            System.out.println("Author removed.");
            return;
        }

        if (userChoice.equals("4"))
        {
            cascadeDeleteCustomer(statement, stringToInt(idString));
            System.out.println("Customer removed.");
            return;
        }

        if (userChoice.equals("5"))
        {
            cascadeDeleteSalary(statement, stringToInt(idString));
            System.out.println("Salary removed.");
            return;
        }

        try
        {
            int rowsDeleted = statement.executeUpdate("DELETE FROM " + userChoice + " WHERE " + userChoice + "ID = " + idString + ";");
            System.out.println("Rows deleted: " + rowsDeleted);
        }
        catch (SQLException exception)
        {
            System.out.println("Delete error: " + exception.toString());
        }
    }

    // ----------------------------
    // Display (interactive)
    // ----------------------------
    public static void displayFunction(Statement statement, Scanner scanner)
    {
        System.out.println("1 - Agents");
        System.out.println("2 - Books");
        System.out.println("3 - Authors");
        System.out.println("4 - Customers");
        System.out.println("5 - Salaries");
        System.out.println("6 - WorkBenefits");

        String userChoice = scanner.nextLine();
        if (!validInput(userChoice, new String[]{"1","2","3","4","5","6"}))
        {
            System.out.println("Enter 1-6:");
            userChoice = scanner.nextLine();
        }

        String tableName = null;
        if (userChoice.equals("1")) tableName = "Agents";
        if (userChoice.equals("2")) tableName = "Books";
        if (userChoice.equals("3")) tableName = "Authors";
        if (userChoice.equals("4")) tableName = "Customers";
        if (userChoice.equals("5")) tableName = "Salaries";
        if (userChoice.equals("6")) tableName = "WorkBenefits";

        displayTable(statement, tableName);
    }

    // ----------------------------
    // Computations (interactive)
    // ----------------------------
    public static void computationFunction(Statement statement, Scanner scanner)
    {
        System.out.println("1 - Agents stats");
        System.out.println("2 - Books stats");
        System.out.println("3 - Salaries stats");

        String userChoice = scanner.nextLine();
        if (!validInput(userChoice, new String[]{"1","2","3"}))
        {
            System.out.println("Enter 1-3:");
            userChoice = scanner.nextLine();
        }

        if (userChoice.equals("1"))
        {
            String sql = "SELECT MIN(Experience) AS minExp, AVG(Experience) AS avgExp, MAX(Experience) AS maxExp FROM Agents;";
            try
            {
                ResultSet resultSet = statement.executeQuery(sql);
                if (resultSet.next())
                {
                    String min = resultSet.getString("minExp");
                    String avg = resultSet.getString("avgExp");
                    String max = resultSet.getString("maxExp");
                    System.out.println("Agents Experience -> MIN: " + min + "  AVG: " + avg + "  MAX: " + max);
                }
                resultSet.close();
            }
            catch (SQLException exception) { System.out.println("Compute error: " + exception.toString()); }
        }

        if (userChoice.equals("2"))
        {
            String sql = "SELECT MIN(Cost) AS minCost, AVG(Cost) AS avgCost, MAX(Cost) AS maxCost FROM Books;";
            try
            {
                ResultSet resultSet = statement.executeQuery(sql);
                if (resultSet.next())
                {
                    String min = resultSet.getString("minCost");
                    String avg = resultSet.getString("avgCost");
                    String max = resultSet.getString("maxCost");
                    System.out.println("Books Cost -> MIN: " + min + "  AVG: " + avg + "  MAX: " + max);
                }
                resultSet.close();
            }
            catch (SQLException exception) { System.out.println("Compute error: " + exception.toString()); }
        }

        if (userChoice.equals("3"))
        {
            String sql = "SELECT MIN(Salary) AS minSal, AVG(Salary) AS avgSal, MAX(Salary) AS maxSal FROM Salaries;";
            try
            {
                ResultSet resultSet = statement.executeQuery(sql);
                if (resultSet.next())
                {
                    String min = resultSet.getString("minSal");
                    String avg = resultSet.getString("avgSal");
                    String max = resultSet.getString("maxSal");
                    System.out.println("Salaries -> MIN: " + min + "  AVG: " + avg + "  MAX: " + max);
                }
                resultSet.close();
            }
            catch (SQLException exception) { System.out.println("Compute error: " + exception.toString()); }
        }
    }

    // ----------------------------
    // Joins (interactive)
    // ----------------------------
    public static void displayCombinationFunction(Statement statement, Scanner scanner)
    {
        System.out.println("1 - Customers -> Agents");
        System.out.println("2 - Agents -> WorkBenefits");
        System.out.println("3 - Agents -> Salaries (via AgentSalary)");

        String userChoice = scanner.nextLine();
        if (!validInput(userChoice, new String[]{"1","2","3"}))
        {
            System.out.println("Enter 1-3:");
            userChoice = scanner.nextLine();
        }

        if (userChoice.equals("1"))
        {
            String sql = "SELECT Customers.CustomerID, Customers.Name AS CustomerName, Agents.AgentID, Agents.Name AS AgentName " +
                    "FROM Customers LEFT JOIN Agents ON Customers.AgentID = Agents.AgentID;";
            try
            {
                ResultSet resultSet = statement.executeQuery(sql);
                String[] outputColumns = {"CustomerID","CustomerName","AgentID","AgentName"};
                int headerIndex = 0;
                while (headerIndex < outputColumns.length)
                {
                    System.out.print(outputColumns[headerIndex]);
                    if (headerIndex < outputColumns.length - 1) System.out.print(" | ");
                    headerIndex = headerIndex + 1;
                }
                System.out.println();

                while (resultSet.next())
                {
                    int columnIndex = 1;
                    while (columnIndex <= outputColumns.length)
                    {
                        String value = resultSet.getString(columnIndex);
                        if (value == null) value = "NULL";
                        System.out.print(value);
                        if (columnIndex < outputColumns.length) System.out.print(" | ");
                        columnIndex = columnIndex + 1;
                    }
                    System.out.println();
                }
                resultSet.close();
            }
            catch (SQLException exception) { System.out.println("Join error: " + exception.toString()); }
        }

        if (userChoice.equals("2"))
        {
            String sql = "SELECT Agents.AgentID, Agents.Name AS AgentName, WorkBenefits.SalaryBonus, WorkBenefits.PaidLeaveDuration " +
                    "FROM Agents JOIN WorkBenefits ON Agents.AgentID = WorkBenefits.AgentID;";
            try
            {
                ResultSet resultSet = statement.executeQuery(sql);
                String[] outputColumns = {"AgentID","AgentName","SalaryBonus","PaidLeaveDuration"};
                int headerIndex = 0;
                while (headerIndex < outputColumns.length)
                {
                    System.out.print(outputColumns[headerIndex]);
                    if (headerIndex < outputColumns.length - 1) System.out.print(" | ");
                    headerIndex = headerIndex + 1;
                }
                System.out.println();

                while (resultSet.next())
                {
                    int columnIndex = 1;
                    while (columnIndex <= outputColumns.length)
                    {
                        String value = resultSet.getString(columnIndex);
                        if (value == null) value = "NULL";
                        System.out.print(value);
                        if (columnIndex < outputColumns.length) System.out.print(" | ");
                        columnIndex = columnIndex + 1;
                    }
                    System.out.println();
                }
                resultSet.close();
            }
            catch (SQLException exception) { System.out.println("Join error: " + exception.toString()); }
        }

        if (userChoice.equals("3"))
        {
            String sql = "SELECT Agents.AgentID, Agents.Name AS AgentName, Salaries.Salary, Salaries.Experience AS SalaryExperience " +
                    "FROM Agents JOIN AgentSalary ON Agents.AgentID = AgentSalary.AgentID JOIN Salaries ON AgentSalary.SalaryID = Salaries.SalaryID;";
            try
            {
                ResultSet resultSet = statement.executeQuery(sql);
                String[] outputColumns = {"AgentID","AgentName","Salary","SalaryExperience"};
                int headerIndex = 0;
                while (headerIndex < outputColumns.length)
                {
                    System.out.print(outputColumns[headerIndex]);
                    if (headerIndex < outputColumns.length - 1) System.out.print(" | ");
                    headerIndex = headerIndex + 1;
                }
                System.out.println();

                while (resultSet.next())
                {
                    int columnIndex = 1;
                    while (columnIndex <= outputColumns.length)
                    {
                        String value = resultSet.getString(columnIndex);
                        if (value == null) value = "NULL";
                        System.out.print(value);
                        if (columnIndex < outputColumns.length) System.out.print(" | ");
                        columnIndex = columnIndex + 1;
                    }
                    System.out.println();
                }
                resultSet.close();
            }
            catch (SQLException exception) { System.out.println("Join error: " + exception.toString()); }
        }
    }

    // ----------------------------
    // Simple validators / converters (no parseInt)
    // ----------------------------
    public static boolean validInput(String input, String[] allowedInputs)
    {
        if (input == null) return false;
        int index = 0;
        while (index < allowedInputs.length)
        {
            if (allowedInputs[index].equals(input)) return true;
            index = index + 1;
        }
        return false;
    }

    public static boolean isInteger(String inputString)
    {
        if (inputString == null) return false;
        if (inputString.length() == 0) return false;
        int index = 0;
        while (index < inputString.length())
        {
            char character = inputString.charAt(index);
            if (character < '0' || character > '9') return false;
            index = index + 1;
        }
        return true;
    }

    public static int stringToInt(String text)
    {
        if (text == null || text.length() == 0) return 0;
        int number = 0;
        int index = 0;
        while (index < text.length())
        {
            char character = text.charAt(index);
            number = number * 10 + (character - '0');
            index = index + 1;
        }
        return number;
    }

    public static String integerToString(int value)
    {
        if (value <= 0) return "0";
        String resultString = "";
        while (value > 0)
        {
            int digit = value % 10;
            char charDigit = (char) ('0' + digit);
            resultString = charDigit + resultString;
            value = value / 10;
        }
        return resultString;
    }
}
