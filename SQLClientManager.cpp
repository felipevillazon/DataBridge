//
// Created by felipevillazon on 12/19/24.
//

#include "SQLClientManager.h"
#include <sqlext.h>
#include "FileManager.h"
#include "Logger.h"

// constructor
SQLClientManager::SQLClientManager(const string& connectionString)
    : sqlEnvHandle(nullptr), sqlConnHandle(nullptr), connectionString(connectionString) {}   // initialize some variables


// destructor
SQLClientManager::~SQLClientManager() {

    /* The destructor will free the handle variables */

    // cleanup handles
    if (sqlConnHandle) {

        SQLDisconnect(sqlConnHandle);
        SQLFreeHandle(SQL_HANDLE_DBC, sqlConnHandle);
    }
    if (sqlEnvHandle) {
        SQLFreeHandle(SQL_HANDLE_ENV, sqlEnvHandle);
    }
}


// connect to SQL server
bool SQLClientManager::connect() {

    disconnect();  // disconnect from server if connected
    /* Connect method intend to achieve a communication and connection with the SQL server */

    LOG_INFO("SQLClientManager::connect(): Connecting to SQL server...");  // log info

    // step 1: allocate environment handle
    sqlReturnCode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &sqlEnvHandle);
    if (sqlReturnCode != SQL_SUCCESS && sqlReturnCode != SQL_SUCCESS_WITH_INFO) {
        LOG_ERROR("SQLClientManager::connect(): Failed to allocate memory for SQL connection.");  // log error
        // std::cerr << "Error allocating ODBC environment handle." << std::endl; // (avoid print and use logger)
        disconnect();
        return false;
    }

    // step 2: set ODBC version
    sqlReturnCode = SQLSetEnvAttr(sqlEnvHandle, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);
    if (sqlReturnCode != SQL_SUCCESS && sqlReturnCode != SQL_SUCCESS_WITH_INFO) {
        LOG_ERROR("SQLClientManager::connect(): Failed to set ODBC environment attributes."); // log error
        // std::cerr << "Error setting ODBC version." << std::endl; // (avoid print and use logger)
        disconnect();

        return false;
    }

    // step 3: allocate connection handle
    sqlReturnCode = SQLAllocHandle(SQL_HANDLE_DBC, sqlEnvHandle, &sqlConnHandle);
    if (sqlReturnCode != SQL_SUCCESS && sqlReturnCode != SQL_SUCCESS_WITH_INFO) {
        LOG_ERROR("SQLClientManager::connect(): Failed to allocate ODBC connection handle.");  // log error
        //std::cerr << "Error allocating ODBC connection handle." << std::endl; // (avoid print and use logger)
        disconnect();
        return false;
    }

    // step 4: establish connection
    sqlReturnCode = SQLDriverConnect(sqlConnHandle, NULL, (SQLCHAR*)connectionString.c_str(), SQL_NTS, NULL, 0, NULL, SQL_DRIVER_COMPLETE);
    if (sqlReturnCode != SQL_SUCCESS && sqlReturnCode != SQL_SUCCESS_WITH_INFO) {
        SQLCHAR sqlState[6], errorMsg[256];
        SQLINTEGER nativeError;
        SQLSMALLINT textLen;

        // fetch error details
        SQLGetDiagRec(SQL_HANDLE_DBC, sqlConnHandle, 1, sqlState, &nativeError, errorMsg, sizeof(errorMsg), &textLen);
        LOG_ERROR("SQLClientManager::connect(): SQLGetDiagRec failed. SQL State: " + std::string(reinterpret_cast<char*>(sqlState)) +
          ", Message: " + std::string(reinterpret_cast<char*>(errorMsg)));   // log error
        //std::cerr << "Connection failed. SQL State: " << sqlState << ", Message: " << errorMsg << std::endl; // (avoid print and use logger)
        disconnect();
        return false;
    }

    LOG_INFO("SQLClientManager::connect(): Successfully connected to database.");  // log info
    //std::cout << "Database connection established successfully!" << std::endl;  //  (avoid print and use logger)
    return true;

}


// disconnect from SQL server
void SQLClientManager::disconnect() {

    /* Disconnect manage the disconnection of the SQL client with the SQL server */

    LOG_INFO("SQLClientManager::disconnect(): Disconnecting SQL server."); // log info

    // Free prepared statements
    for (auto& [name, stmt] : preparedStatements) {
        if (stmt) {
            SQLFreeHandle(SQL_HANDLE_STMT, stmt);
            stmt = nullptr;
        }
    }
    preparedStatements.clear();


    if (sqlConnHandle) {    // check if connected to server
        sqlReturnCode = SQLDisconnect(sqlConnHandle);   // disconnect from server
        if (sqlReturnCode == SQL_SUCCESS || sqlReturnCode == SQL_SUCCESS_WITH_INFO) {   // check if disconnection is successful
            LOG_INFO("SQLClientManager::disconnect(): Successfully disconnected from SQL server."); // log info
            // std::cout << "Database disconnected successfully." << std::endl;    // return positive message  (avoid print and use logger)
        } else {
            LOG_ERROR("SQLClientManager::disconnect(): Failed to disconnect from SQL server");  // log error
            // std::cerr << "Error during database disconnection." << std::endl;   // return negative message  (avoid print and use logger)
        }
        SQLFreeHandle(SQL_HANDLE_DBC, sqlConnHandle);  // free connection handle variable
        sqlConnHandle = nullptr;     // assign to null pointer
    }

    if (sqlEnvHandle) {     // check environment handle variable
        SQLFreeHandle(SQL_HANDLE_ENV, sqlEnvHandle);    // free environment handle variable
        sqlEnvHandle = nullptr;  // assign to null pointer
    }
}


// execute any query, can be reused with different queries
bool SQLClientManager::executeQuery(const string& query) {

    LOG_INFO("SQLClientManager::executeQuery: Starting query execution...");  // log info
    SQLCHAR sqlState[6], errMsg[SQL_MAX_MESSAGE_LENGTH];  // error message handle for SQL execution
    SQLINTEGER nativeError;     // error message helper for SQL execution
    SQLSMALLINT textLength;     // error message length for SQL execution

    // allocate a statement handle
    LOG_INFO("SQLClientManager::executeQuery: Allocating statement handle.");  // log info
    sqlReturnCode = SQLAllocHandle(SQL_HANDLE_STMT, sqlConnHandle, &hStmt);
    if (sqlReturnCode != SQL_SUCCESS && sqlReturnCode != SQL_SUCCESS_WITH_INFO) {
        // cerr << "Error allocating statement handle." << endl; //  (avoid print and use logger)
        LOG_ERROR("SQLClientManager::executeQuery: Allocating statement handle failed.");  // log error
        return false;
    }

    LOG_INFO("SQLClientManager::executeQuery: Allocating statement handle successfully");  // log info
    // cout << "Allocating statement handle successfully!" << endl;  (avoid print and use logger)

    // execute SQL query
    LOG_INFO("SQLClientManager::executeQuery: Executing query...");  // log info
    sqlReturnCode = SQLExecDirect(hStmt, (SQLCHAR*)query.c_str(), SQL_NTS);
    if (sqlReturnCode != SQL_SUCCESS && sqlReturnCode != SQL_SUCCESS_WITH_INFO) {
        // Retrieve detailed error information
        if (SQLGetDiagRec(SQL_HANDLE_STMT, hStmt, 1, sqlState, &nativeError, errMsg, sizeof(errMsg), &textLength) == SQL_SUCCESS) {
            // std::cerr << "SQL Execution Error [" << sqlState << "] (" << nativeError << "): " << errMsg << std::endl;  // (avoid print and use logger)

            LOG_ERROR("SQLClientManager::executeQuery: Executing query failed. SQL State: " + std::string(reinterpret_cast<char*>(sqlState)) +
          ", Message: " + std::string(reinterpret_cast<char*>(errMsg)));  // log error

        } else {
            LOG_ERROR("SQLClientManager::executeQuery: Executing query failed.");  // log error
            // std::cerr << "SQL Execution Error: Unable to retrieve error details." << std::endl; //  (avoid print and use logger)
        }

        SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
        return false;
    }

    LOG_INFO("SQLClientManager::executeQuery: Executing query successfully.");  // log info
    // std::cout << "Query executed successfully: " << query << std::endl; // (avoid print and use logger)

    // Free statement handle
    SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
    return true;
}


// create SQL database tables from external file
// create SQL database tables from external file
void SQLClientManager::createDatabaseSchema(const std::string &schemaFile) {
    LOG_INFO("SQLClientManager::createDatabaseSchema(): Starting to build SQL database schema...");

    FileManager fileManager;
    fileManager.loadFile(schemaFile);

    LOG_INFO("SQLClientManager::createDatabaseSchema(): Loading JSON file...");
    if (fileManager.configData.empty() || !fileManager.configData.contains("tables")) {
        LOG_ERROR("SQLClientManager::createDatabaseSchema(): Loading JSON file failed or missing 'tables'.");
        return;
    }

    LOG_INFO("SQLClientManager::createDatabaseSchema(): Loading JSON file successfully.");
    LOG_INFO("SQLClientManager::createDatabaseSchema(): Starting database schema creation...");

    // Start a transaction (MariaDB syntax, same as MySQL)
    executeQuery("START TRANSACTION;");
    LOG_INFO("SQLClientManager::createDatabaseSchema(): Starting SQL transaction...");

    bool success = true;

    for (const auto& table : fileManager.configData["tables"].items()) {
        const std::string& tableName = table.key();
        const auto& tableDef = table.value();

        std::string createTableSQL = "CREATE TABLE IF NOT EXISTS " + tableName + " (\n";

        bool firstLine = true;

        // Collect PK columns to support composite PK
        std::vector<std::string> pkColumns;

        // Columns
        for (const auto& column : tableDef["columns"].items()) {
            const std::string& columnName = column.key();
            const auto& colDef = column.value();

            if (!firstLine) createTableSQL += ",\n";
            firstLine = false;

            std::string columnType = colDef["type"];

            // Type mapping for MariaDB/MySQL
            if (columnType == "TEXT")   columnType = "VARCHAR(255)";
            if (columnType == "DOUBLE") columnType = "DOUBLE";
            if (columnType == "BIGINT") columnType = "BIGINT";

            const bool isPrimaryKey =
                colDef.contains("primary_key") && colDef["primary_key"].get<bool>();
            const bool isAutoIncrement =
                colDef.contains("auto_increment") && colDef["auto_increment"].get<bool>();
            const bool isNullable =
                colDef.contains("nullable") ? colDef["nullable"].get<bool>() : true;
            const bool hasDefault = colDef.contains("default");

            createTableSQL += "    " + columnName + " " + columnType;

            if (!isNullable) createTableSQL += " NOT NULL";
            if (isAutoIncrement) createTableSQL += " AUTO_INCREMENT";

            // Default handling: do NOT quote CURRENT_TIMESTAMP
            if (hasDefault) {
                if (colDef["default"].is_string()) {
                    std::string defVal = colDef["default"].get<std::string>();
                    if (defVal == "CURRENT_TIMESTAMP") {
                        createTableSQL += " DEFAULT CURRENT_TIMESTAMP";
                    } else {
                        createTableSQL += " DEFAULT '" + defVal + "'";
                    }
                } else if (colDef["default"].is_number_integer()) {
                    createTableSQL += " DEFAULT " + std::to_string(colDef["default"].get<long long>());
                } else if (colDef["default"].is_number_float()) {
                    createTableSQL += " DEFAULT " + std::to_string(colDef["default"].get<double>());
                } else if (colDef["default"].is_boolean()) {
                    createTableSQL += std::string(" DEFAULT ") + (colDef["default"].get<bool>() ? "1" : "0");
                }
            }

            if (isPrimaryKey) {
                pkColumns.push_back(columnName);
            }
        }

        // Foreign keys (from JSON)
        if (tableDef.contains("foreign_keys")) {
            for (const auto& fk : tableDef["foreign_keys"]) {
                createTableSQL += ",\n    FOREIGN KEY (" + fk["column"].get<std::string>() + ") REFERENCES " +
                                  fk["references"]["table"].get<std::string>() + "(" +
                                  fk["references"]["column"].get<std::string>() + ")";
            }
        }

        // Primary key clause (single/composite)
        // IMPORTANT: For partitioned tables, PK must include partition column
        if (!pkColumns.empty()) {
            if (tableName == "object_readings") {
                // enforce for partitioning correctness
                createTableSQL += ",\n    PRIMARY KEY (reading_id, reading_timestamp)";
            } else if (pkColumns.size() == 1) {
                createTableSQL += ",\n    PRIMARY KEY (" + pkColumns[0] + ")";
            } else {
                createTableSQL += ",\n    PRIMARY KEY (";
                for (size_t i = 0; i < pkColumns.size(); ++i) {
                    createTableSQL += pkColumns[i];
                    if (i + 1 < pkColumns.size()) createTableSQL += ", ";
                }
                createTableSQL += ")";
            }
        }

        // Indexes (optional from JSON)
        if (tableDef.contains("indexes")) {
            int idxCounter = 0;
            for (const auto& idx : tableDef["indexes"]) {
                if (!idx.contains("columns") || !idx["columns"].is_array()) continue;

                std::string idxName = "idx_" + tableName + "_" + std::to_string(++idxCounter);
                createTableSQL += ",\n    KEY " + idxName + " (";

                for (size_t i = 0; i < idx["columns"].size(); ++i) {
                    createTableSQL += idx["columns"][i].get<std::string>();
                    if (i + 1 < idx["columns"].size()) createTableSQL += ", ";
                }
                createTableSQL += ")";
            }
        }

        // Close definition + engine
        createTableSQL += "\n) ENGINE=InnoDB";

        // Monthly partitions from 2026-01 through 2035-12, plus pMax
        // RANGE boundary is "LESS THAN first day of next month"
        if (tableName == "object_readings") {
            createTableSQL += "\nPARTITION BY RANGE COLUMNS(reading_timestamp) (\n";

            // 2026
            createTableSQL += "  PARTITION p2026_01 VALUES LESS THAN ('2026-02-01'),\n";
            createTableSQL += "  PARTITION p2026_02 VALUES LESS THAN ('2026-03-01'),\n";
            createTableSQL += "  PARTITION p2026_03 VALUES LESS THAN ('2026-04-01'),\n";
            createTableSQL += "  PARTITION p2026_04 VALUES LESS THAN ('2026-05-01'),\n";
            createTableSQL += "  PARTITION p2026_05 VALUES LESS THAN ('2026-06-01'),\n";
            createTableSQL += "  PARTITION p2026_06 VALUES LESS THAN ('2026-07-01'),\n";
            createTableSQL += "  PARTITION p2026_07 VALUES LESS THAN ('2026-08-01'),\n";
            createTableSQL += "  PARTITION p2026_08 VALUES LESS THAN ('2026-09-01'),\n";
            createTableSQL += "  PARTITION p2026_09 VALUES LESS THAN ('2026-10-01'),\n";
            createTableSQL += "  PARTITION p2026_10 VALUES LESS THAN ('2026-11-01'),\n";
            createTableSQL += "  PARTITION p2026_11 VALUES LESS THAN ('2026-12-01'),\n";
            createTableSQL += "  PARTITION p2026_12 VALUES LESS THAN ('2027-01-01'),\n";

            // 2027
            createTableSQL += "  PARTITION p2027_01 VALUES LESS THAN ('2027-02-01'),\n";
            createTableSQL += "  PARTITION p2027_02 VALUES LESS THAN ('2027-03-01'),\n";
            createTableSQL += "  PARTITION p2027_03 VALUES LESS THAN ('2027-04-01'),\n";
            createTableSQL += "  PARTITION p2027_04 VALUES LESS THAN ('2027-05-01'),\n";
            createTableSQL += "  PARTITION p2027_05 VALUES LESS THAN ('2027-06-01'),\n";
            createTableSQL += "  PARTITION p2027_06 VALUES LESS THAN ('2027-07-01'),\n";
            createTableSQL += "  PARTITION p2027_07 VALUES LESS THAN ('2027-08-01'),\n";
            createTableSQL += "  PARTITION p2027_08 VALUES LESS THAN ('2027-09-01'),\n";
            createTableSQL += "  PARTITION p2027_09 VALUES LESS THAN ('2027-10-01'),\n";
            createTableSQL += "  PARTITION p2027_10 VALUES LESS THAN ('2027-11-01'),\n";
            createTableSQL += "  PARTITION p2027_11 VALUES LESS THAN ('2027-12-01'),\n";
            createTableSQL += "  PARTITION p2027_12 VALUES LESS THAN ('2028-01-01'),\n";

            // 2028
            createTableSQL += "  PARTITION p2028_01 VALUES LESS THAN ('2028-02-01'),\n";
            createTableSQL += "  PARTITION p2028_02 VALUES LESS THAN ('2028-03-01'),\n";
            createTableSQL += "  PARTITION p2028_03 VALUES LESS THAN ('2028-04-01'),\n";
            createTableSQL += "  PARTITION p2028_04 VALUES LESS THAN ('2028-05-01'),\n";
            createTableSQL += "  PARTITION p2028_05 VALUES LESS THAN ('2028-06-01'),\n";
            createTableSQL += "  PARTITION p2028_06 VALUES LESS THAN ('2028-07-01'),\n";
            createTableSQL += "  PARTITION p2028_07 VALUES LESS THAN ('2028-08-01'),\n";
            createTableSQL += "  PARTITION p2028_08 VALUES LESS THAN ('2028-09-01'),\n";
            createTableSQL += "  PARTITION p2028_09 VALUES LESS THAN ('2028-10-01'),\n";
            createTableSQL += "  PARTITION p2028_10 VALUES LESS THAN ('2028-11-01'),\n";
            createTableSQL += "  PARTITION p2028_11 VALUES LESS THAN ('2028-12-01'),\n";
            createTableSQL += "  PARTITION p2028_12 VALUES LESS THAN ('2029-01-01'),\n";

            // 2029
            createTableSQL += "  PARTITION p2029_01 VALUES LESS THAN ('2029-02-01'),\n";
            createTableSQL += "  PARTITION p2029_02 VALUES LESS THAN ('2029-03-01'),\n";
            createTableSQL += "  PARTITION p2029_03 VALUES LESS THAN ('2029-04-01'),\n";
            createTableSQL += "  PARTITION p2029_04 VALUES LESS THAN ('2029-05-01'),\n";
            createTableSQL += "  PARTITION p2029_05 VALUES LESS THAN ('2029-06-01'),\n";
            createTableSQL += "  PARTITION p2029_06 VALUES LESS THAN ('2029-07-01'),\n";
            createTableSQL += "  PARTITION p2029_07 VALUES LESS THAN ('2029-08-01'),\n";
            createTableSQL += "  PARTITION p2029_08 VALUES LESS THAN ('2029-09-01'),\n";
            createTableSQL += "  PARTITION p2029_09 VALUES LESS THAN ('2029-10-01'),\n";
            createTableSQL += "  PARTITION p2029_10 VALUES LESS THAN ('2029-11-01'),\n";
            createTableSQL += "  PARTITION p2029_11 VALUES LESS THAN ('2029-12-01'),\n";
            createTableSQL += "  PARTITION p2029_12 VALUES LESS THAN ('2030-01-01'),\n";

            // 2030
            createTableSQL += "  PARTITION p2030_01 VALUES LESS THAN ('2030-02-01'),\n";
            createTableSQL += "  PARTITION p2030_02 VALUES LESS THAN ('2030-03-01'),\n";
            createTableSQL += "  PARTITION p2030_03 VALUES LESS THAN ('2030-04-01'),\n";
            createTableSQL += "  PARTITION p2030_04 VALUES LESS THAN ('2030-05-01'),\n";
            createTableSQL += "  PARTITION p2030_05 VALUES LESS THAN ('2030-06-01'),\n";
            createTableSQL += "  PARTITION p2030_06 VALUES LESS THAN ('2030-07-01'),\n";
            createTableSQL += "  PARTITION p2030_07 VALUES LESS THAN ('2030-08-01'),\n";
            createTableSQL += "  PARTITION p2030_08 VALUES LESS THAN ('2030-09-01'),\n";
            createTableSQL += "  PARTITION p2030_09 VALUES LESS THAN ('2030-10-01'),\n";
            createTableSQL += "  PARTITION p2030_10 VALUES LESS THAN ('2030-11-01'),\n";
            createTableSQL += "  PARTITION p2030_11 VALUES LESS THAN ('2030-12-01'),\n";
            createTableSQL += "  PARTITION p2030_12 VALUES LESS THAN ('2031-01-01'),\n";

            // 2031
            createTableSQL += "  PARTITION p2031_01 VALUES LESS THAN ('2031-02-01'),\n";
            createTableSQL += "  PARTITION p2031_02 VALUES LESS THAN ('2031-03-01'),\n";
            createTableSQL += "  PARTITION p2031_03 VALUES LESS THAN ('2031-04-01'),\n";
            createTableSQL += "  PARTITION p2031_04 VALUES LESS THAN ('2031-05-01'),\n";
            createTableSQL += "  PARTITION p2031_05 VALUES LESS THAN ('2031-06-01'),\n";
            createTableSQL += "  PARTITION p2031_06 VALUES LESS THAN ('2031-07-01'),\n";
            createTableSQL += "  PARTITION p2031_07 VALUES LESS THAN ('2031-08-01'),\n";
            createTableSQL += "  PARTITION p2031_08 VALUES LESS THAN ('2031-09-01'),\n";
            createTableSQL += "  PARTITION p2031_09 VALUES LESS THAN ('2031-10-01'),\n";
            createTableSQL += "  PARTITION p2031_10 VALUES LESS THAN ('2031-11-01'),\n";
            createTableSQL += "  PARTITION p2031_11 VALUES LESS THAN ('2031-12-01'),\n";
            createTableSQL += "  PARTITION p2031_12 VALUES LESS THAN ('2032-01-01'),\n";

            // 2032
            createTableSQL += "  PARTITION p2032_01 VALUES LESS THAN ('2032-02-01'),\n";
            createTableSQL += "  PARTITION p2032_02 VALUES LESS THAN ('2032-03-01'),\n";
            createTableSQL += "  PARTITION p2032_03 VALUES LESS THAN ('2032-04-01'),\n";
            createTableSQL += "  PARTITION p2032_04 VALUES LESS THAN ('2032-05-01'),\n";
            createTableSQL += "  PARTITION p2032_05 VALUES LESS THAN ('2032-06-01'),\n";
            createTableSQL += "  PARTITION p2032_06 VALUES LESS THAN ('2032-07-01'),\n";
            createTableSQL += "  PARTITION p2032_07 VALUES LESS THAN ('2032-08-01'),\n";
            createTableSQL += "  PARTITION p2032_08 VALUES LESS THAN ('2032-09-01'),\n";
            createTableSQL += "  PARTITION p2032_09 VALUES LESS THAN ('2032-10-01'),\n";
            createTableSQL += "  PARTITION p2032_10 VALUES LESS THAN ('2032-11-01'),\n";
            createTableSQL += "  PARTITION p2032_11 VALUES LESS THAN ('2032-12-01'),\n";
            createTableSQL += "  PARTITION p2032_12 VALUES LESS THAN ('2033-01-01'),\n";

            // 2033
            createTableSQL += "  PARTITION p2033_01 VALUES LESS THAN ('2033-02-01'),\n";
            createTableSQL += "  PARTITION p2033_02 VALUES LESS THAN ('2033-03-01'),\n";
            createTableSQL += "  PARTITION p2033_03 VALUES LESS THAN ('2033-04-01'),\n";
            createTableSQL += "  PARTITION p2033_04 VALUES LESS THAN ('2033-05-01'),\n";
            createTableSQL += "  PARTITION p2033_05 VALUES LESS THAN ('2033-06-01'),\n";
            createTableSQL += "  PARTITION p2033_06 VALUES LESS THAN ('2033-07-01'),\n";
            createTableSQL += "  PARTITION p2033_07 VALUES LESS THAN ('2033-08-01'),\n";
            createTableSQL += "  PARTITION p2033_08 VALUES LESS THAN ('2033-09-01'),\n";
            createTableSQL += "  PARTITION p2033_09 VALUES LESS THAN ('2033-10-01'),\n";
            createTableSQL += "  PARTITION p2033_10 VALUES LESS THAN ('2033-11-01'),\n";
            createTableSQL += "  PARTITION p2033_11 VALUES LESS THAN ('2033-12-01'),\n";
            createTableSQL += "  PARTITION p2033_12 VALUES LESS THAN ('2034-01-01'),\n";

            // 2034
            createTableSQL += "  PARTITION p2034_01 VALUES LESS THAN ('2034-02-01'),\n";
            createTableSQL += "  PARTITION p2034_02 VALUES LESS THAN ('2034-03-01'),\n";
            createTableSQL += "  PARTITION p2034_03 VALUES LESS THAN ('2034-04-01'),\n";
            createTableSQL += "  PARTITION p2034_04 VALUES LESS THAN ('2034-05-01'),\n";
            createTableSQL += "  PARTITION p2034_05 VALUES LESS THAN ('2034-06-01'),\n";
            createTableSQL += "  PARTITION p2034_06 VALUES LESS THAN ('2034-07-01'),\n";
            createTableSQL += "  PARTITION p2034_07 VALUES LESS THAN ('2034-08-01'),\n";
            createTableSQL += "  PARTITION p2034_08 VALUES LESS THAN ('2034-09-01'),\n";
            createTableSQL += "  PARTITION p2034_09 VALUES LESS THAN ('2034-10-01'),\n";
            createTableSQL += "  PARTITION p2034_10 VALUES LESS THAN ('2034-11-01'),\n";
            createTableSQL += "  PARTITION p2034_11 VALUES LESS THAN ('2034-12-01'),\n";
            createTableSQL += "  PARTITION p2034_12 VALUES LESS THAN ('2035-01-01'),\n";

            // 2035
            createTableSQL += "  PARTITION p2035_01 VALUES LESS THAN ('2035-02-01'),\n";
            createTableSQL += "  PARTITION p2035_02 VALUES LESS THAN ('2035-03-01'),\n";
            createTableSQL += "  PARTITION p2035_03 VALUES LESS THAN ('2035-04-01'),\n";
            createTableSQL += "  PARTITION p2035_04 VALUES LESS THAN ('2035-05-01'),\n";
            createTableSQL += "  PARTITION p2035_05 VALUES LESS THAN ('2035-06-01'),\n";
            createTableSQL += "  PARTITION p2035_06 VALUES LESS THAN ('2035-07-01'),\n";
            createTableSQL += "  PARTITION p2035_07 VALUES LESS THAN ('2035-08-01'),\n";
            createTableSQL += "  PARTITION p2035_08 VALUES LESS THAN ('2035-09-01'),\n";
            createTableSQL += "  PARTITION p2035_09 VALUES LESS THAN ('2035-10-01'),\n";
            createTableSQL += "  PARTITION p2035_10 VALUES LESS THAN ('2035-11-01'),\n";
            createTableSQL += "  PARTITION p2035_11 VALUES LESS THAN ('2035-12-01'),\n";
            createTableSQL += "  PARTITION p2035_12 VALUES LESS THAN ('2036-01-01'),\n";

            // catch-all future
            createTableSQL += "  PARTITION pMax VALUES LESS THAN (MAXVALUE)\n";
            createTableSQL += ")";
        }

        createTableSQL += ";";

        if (!executeQuery(createTableSQL)) {
            LOG_ERROR("SQLClientManager::createDatabaseSchema(): Failed to create table: " + tableName);
            success = false;
            break;
        } else {
            LOG_INFO("SQLClientManager::createDatabaseSchema(): Created/verified table: " + tableName);
        }
    }

    if (success) {
        executeQuery("COMMIT;");
        LOG_INFO("SQLClientManager::createDatabaseSchema(): Database schema created successfully.");
    } else {
        executeQuery("ROLLBACK;");
        LOG_ERROR("SQLClientManager::createDatabaseSchema(): Transaction rolled back due to errors.");
    }
}



// prepare insert statements for SQL query
void SQLClientManager::prepareInsertStatements(
    const std::unordered_map<std::string, std::unordered_map<int, float>>& tableObjects
) {
    LOG_INFO("SQLClientManager::prepareInsertStatements(): Preparing insert statements...");

    for (const auto& [tableName, records] : tableObjects) {
        // Already prepared? Skip.
        if (preparedStatements.find(tableName) != preparedStatements.end()) {
            continue;
        }

        std::string query = "INSERT INTO " + tableName + " (object_id, object_value) VALUES (?, ?)";

        SQLHSTMT stmt = nullptr;
        if (SQLAllocHandle(SQL_HANDLE_STMT, sqlConnHandle, &stmt) != SQL_SUCCESS) {
            LOG_ERROR("SQLClientManager::prepareInsertStatements(): SQLAllocHandle failed for table " + tableName);
            continue;
        }

        if (SQLPrepare(stmt, (SQLCHAR*)query.c_str(), SQL_NTS) != SQL_SUCCESS) {
            LOG_ERROR("SQLClientManager::prepareInsertStatements(): SQLPrepare failed for table " + tableName);
            SQLFreeHandle(SQL_HANDLE_STMT, stmt);
            continue;
        }

        preparedStatements[tableName] = stmt;
        LOG_INFO("SQLClientManager::prepareInsertStatements(): Prepared statement for table " + tableName);
    }
}


// insert data by input table name, object id and value from opc ua node id
bool SQLClientManager::insertBatchData(
    const std::unordered_map<std::string, std::unordered_map<int, float>>& tableObjects
) {
    LOG_INFO("SQLClientManager::insertBatchData(): Inserting batch data...");

    try {
        if (!executeQuery("START TRANSACTION;")) {
            LOG_ERROR("SQLClientManager::insertBatchData(): Failed to begin transaction.");
            return false;
        }

        for (const auto& [tableName, records] : tableObjects) {
            if (records.empty()) continue;

            // Build multi-row query: VALUES (?, ?), (?, ?), ...
            std::ostringstream qs;
            qs << "INSERT INTO " << tableName << " (object_id, object_value) VALUES ";

            size_t n = records.size();
            for (size_t i = 0; i < n; ++i) {
                qs << "(?, ?)";
                if (i + 1 < n) qs << ", ";
            }

            std::string query = qs.str();

            // Prepare a statement for THIS batch (because parameter count changes each cycle)
            SQLHSTMT stmt = nullptr;
            SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_STMT, sqlConnHandle, &stmt);
            if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
                LOG_ERROR("SQLClientManager::insertBatchData(): SQLAllocHandle failed for table " + tableName);
                continue;
            }

            ret = SQLPrepare(stmt, (SQLCHAR*)query.c_str(), SQL_NTS);
            if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
                LOG_ERROR("SQLClientManager::insertBatchData(): SQLPrepare failed for table " + tableName);
                SQLFreeHandle(SQL_HANDLE_STMT, stmt);
                continue;
            }

            // Bind all parameters: (object_id1, value1, object_id2, value2, ...)
            std::vector<SQLINTEGER> ids;
            std::vector<double> vals;
            ids.reserve(n);
            vals.reserve(n);

            for (const auto& [objectId, value] : records) {
                ids.push_back((SQLINTEGER)objectId);
                vals.push_back((double)value); // keep your float input but bind as double
            }

            // Bind parameters. Parameter index starts at 1.
            SQLUSMALLINT paramIndex = 1;
            for (size_t i = 0; i < n; ++i) {
                ret = SQLBindParameter(stmt, paramIndex++, SQL_PARAM_INPUT,
                                       SQL_C_LONG, SQL_INTEGER, 0, 0,
                                       &ids[i], 0, nullptr);
                if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
                    LOG_ERROR("SQLClientManager::insertBatchData(): Bind failed (object_id) for table " + tableName);
                    break;
                }

                ret = SQLBindParameter(stmt, paramIndex++, SQL_PARAM_INPUT,
                                       SQL_C_DOUBLE, SQL_DOUBLE, 0, 0,
                                       &vals[i], 0, nullptr);
                if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
                    LOG_ERROR("SQLClientManager::insertBatchData(): Bind failed (object_value) for table " + tableName);
                    break;
                }
            }

            ret = SQLExecute(stmt);
            if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
                LOG_ERROR("SQLClientManager::insertBatchData(): SQLExecute failed for table " + tableName);
                // optional: rollback and return false, depends how strict you want to be
            }

            SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        }

        if (!executeQuery("COMMIT;")) {
            LOG_ERROR("SQLClientManager::insertBatchData(): Failed to commit transaction.");
            executeQuery("ROLLBACK;");
            return false;
        }

        return true;

    } catch (const std::exception& e) {
        executeQuery("ROLLBACK;");
        LOG_ERROR(std::string("SQLClientManager::insertBatchData(): Exception: ") + e.what());
        return false;
    }
}


// insert alarm data into database
void SQLClientManager::insertAlarm(
    const std::string &table,
    const std::unordered_map<opcua::NodeId, std::tuple<int, int, int, int, int, float, int>>& values,
    const std::string &type)
{
    std::ostringstream queryStream;

    // Base insert statement
    queryStream << "INSERT INTO " << table
                << " (severity, event_id, state_id, subsystem_id, object_id, object_value, error_code";

    // Conditionally add timestamp columns based on `type`
    if (type == "ACKNOWLEDGED") {
        queryStream << ", ack_timestamp";
    } else if (type == "FIXED") {
        queryStream << ", fixed_timestamp";
    }

    queryStream << ") VALUES (?, ?, ?, ?, ?, ?, ?";

    // Add corresponding values for timestamps
    if (type == "ACKNOWLEDGED") {
        queryStream << ", NOW()";  // Fill ack_timestamp
    } else if (type == "FIXED") {
        queryStream << ", NOW()";  // Fill fixed_timestamp
    }

    queryStream << ")";

    const std::string query = queryStream.str();

    // Allocate statement handle
    SQLHSTMT stmt;
    SQLAllocHandle(SQL_HANDLE_STMT, sqlConnHandle, &stmt);

    // Begin transaction (disable auto-commit)
    if (SQLSetConnectAttr(sqlConnHandle, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0) != SQL_SUCCESS) {
        LOG_ERROR("SQLClientManager::insertAlarm(): Failed to disable auto-commit.");
        return;
    }

    // Prepare the SQL statement
    if (SQLPrepare(stmt, (SQLCHAR*)query.c_str(), SQL_NTS) != SQL_SUCCESS) {
        LOG_ERROR("SQLClientManager::insertAlarm(): Failed to prepare SQL statement.");
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        return;
    }

    // Log the query for debugging
    std::cout << "Executing Query: " << query << std::endl;

    // Iterate through the provided values
    for (const auto& [nodeId, data] : values) {
        int severity, event_id, state_id, subsystem_id, object_id, error_code;
        float object_value;

        std::tie(severity, event_id, state_id, subsystem_id, object_id, object_value, error_code) = data;

        // Bind parameters
        SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0, 0, &severity, 0, NULL);
        SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0, 0, &event_id, 0, NULL);
        SQLBindParameter(stmt, 3, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0, 0, &state_id, 0, NULL);
        SQLBindParameter(stmt, 4, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0, 0, &subsystem_id, 0, NULL);
        SQLBindParameter(stmt, 5, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0, 0, &object_id, 0, NULL);
        SQLBindParameter(stmt, 6, SQL_PARAM_INPUT, SQL_C_FLOAT, SQL_REAL, 0, 0, &object_value, 0, NULL);
        SQLBindParameter(stmt, 7, SQL_PARAM_INPUT, SQL_C_LONG, SQL_INTEGER, 0, 0, &error_code, 0, NULL);

        // Execute the statement
        SQLRETURN ret = SQLExecute(stmt);
        if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
            LOG_ERROR("SQLClientManager::insertAlarm(): Failed to execute statement. Rolling back transaction.");

            // Rollback transaction on failure
            SQLEndTran(SQL_HANDLE_DBC, sqlConnHandle, SQL_ROLLBACK);
            SQLFreeHandle(SQL_HANDLE_STMT, stmt);

            // Re-enable auto-commit
            SQLSetConnectAttr(sqlConnHandle, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_ON, 0);
            return;
        }
    }

    // Commit transaction if all inserts succeed
    if (SQLEndTran(SQL_HANDLE_DBC, sqlConnHandle, SQL_COMMIT) != SQL_SUCCESS) {
        LOG_ERROR("SQLClientManager::insertAlarm(): Failed to commit transaction.");
        SQLEndTran(SQL_HANDLE_DBC, sqlConnHandle, SQL_ROLLBACK);
    }

    // Cleanup
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

    // Re-enable auto-commit
    SQLSetConnectAttr(sqlConnHandle, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_ON, 0);
}


