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

    /* Connect method intend to achieve a communication and connection with the SQL server */

    LOG_INFO("SQLClientManager::connect(): Connecting to SQL server...");  // log info

    // step 1: allocate environment handle
    sqlReturnCode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &sqlEnvHandle);
    if (sqlReturnCode != SQL_SUCCESS && sqlReturnCode != SQL_SUCCESS_WITH_INFO) {
        LOG_ERROR("SQLClientManager::connect(): Failed to allocate memory for SQL connection.");  // log error
        std::cerr << "Error allocating ODBC environment handle." << std::endl;
        return false;
    }

    // step 2: set ODBC version
    sqlReturnCode = SQLSetEnvAttr(sqlEnvHandle, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);
    if (sqlReturnCode != SQL_SUCCESS && sqlReturnCode != SQL_SUCCESS_WITH_INFO) {
        LOG_ERROR("SQLClientManager::connect(): Failed to set ODBC environment attributes."); // log error
        std::cerr << "Error setting ODBC version." << std::endl;
        return false;
    }

    // step 3: allocate connection handle
    sqlReturnCode = SQLAllocHandle(SQL_HANDLE_DBC, sqlEnvHandle, &sqlConnHandle);
    if (sqlReturnCode != SQL_SUCCESS && sqlReturnCode != SQL_SUCCESS_WITH_INFO) {
        LOG_ERROR("SQLClientManager::connect(): Failed to allocate ODBC connection handle.");  // log error
        std::cerr << "Error allocating ODBC connection handle." << std::endl;
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
        LOG_ERROR("SQLClientManager::connect(): SQLGetDiagRec failed.");  // log error
        std::cerr << "Connection failed. SQL State: " << sqlState
                  << ", Message: " << errorMsg << std::endl;
        return false;
    }

    LOG_INFO("SQLClientManager::connect(): Successfully connected to database.");  // log info
    std::cout << "Database connection established successfully!" << std::endl;
    return true;

}

// disconnect from SQL server
void SQLClientManager::disconnect() {

    /* Disconnect manage the disconnection of the SQL client with the SQL server */

    LOG_INFO("SQLClientManager::disconnect(): Disconnecting SQL server."); // log info

    if (sqlConnHandle) {    // check if connected to server
        sqlReturnCode = SQLDisconnect(sqlConnHandle);   // disconnect from server
        if (sqlReturnCode == SQL_SUCCESS || sqlReturnCode == SQL_SUCCESS_WITH_INFO) {   // check if disconnection is successful
            LOG_INFO("SQLClientManager::disconnect(): Successfully disconnected from SQL server."); // log info
            std::cout << "Database disconnected successfully." << std::endl;    // return positive message
        } else {
            LOG_ERROR("SQLClientManager::disconnect(): Failed to disconnect from SQL server");  // log error
            std::cerr << "Error during database disconnection." << std::endl;   // return negative message
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
        cerr << "Error allocating statement handle." << endl;
        LOG_ERROR("SQLClientManager::executeQuery: Allocating statement handle failed.");  // log error
        return false;
    } else {
        LOG_INFO("SQLClientManager::executeQuery: Allocating statement handle successfully");  // log info
        cout << "Allocating statement handle successfully!" << endl;
    }

    // execute SQL query
    LOG_INFO("SQLClientManager::executeQuery: Executing query...");  // log info
    sqlReturnCode = SQLExecDirect(hStmt, (SQLCHAR*)query.c_str(), SQL_NTS);
    if (sqlReturnCode != SQL_SUCCESS && sqlReturnCode != SQL_SUCCESS_WITH_INFO) {
        // Retrieve detailed error information
        if (SQLGetDiagRec(SQL_HANDLE_STMT, hStmt, 1, sqlState, &nativeError, errMsg, sizeof(errMsg), &textLength) == SQL_SUCCESS) {
            std::cerr << "SQL Execution Error [" << sqlState << "] (" << nativeError << "): " << errMsg << std::endl;
            LOG_ERROR("SQLClientManager::executeQuery: Executing query failed.");  // log error
        } else {
            LOG_ERROR("SQLClientManager::executeQuery: Executing query failed.");  // log error
            std::cerr << "SQL Execution Error: Unable to retrieve error details." << std::endl;
        }

        SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
        return false;
    }

    LOG_INFO("SQLClientManager::executeQuery: Executing query successfully.");  // log info
    std::cout << "Query executed successfully: " << query << std::endl;

    // Free statement handle
    SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
    return true;
}

// create SQL database tables from external file
void SQLClientManager::createDatabaseSchema(const string &schemaFile) {

    LOG_INFO("SQLClientManager::createDatabaseSchema(): Starting to build SQL database schema...");  // log info
    FileManager fileManager;
    fileManager.loadFile(schemaFile);  // assume loadFile() is implemented

    LOG_INFO("SQLClientManager::createDatabaseSchema(): Loading JSON file...");  // log info
    if (fileManager.configData.empty()) {

        LOG_ERROR("SQLClientManager::createDatabaseSchema(): Loading JSON file failed.");  // log error
        cerr << "Failed to load schema file!" << endl;
        return;
    }

    LOG_INFO("SQLClientManager::createDatabaseSchema(): Loading JSON file successfully.");  // log info
    cout << "Starting database schema creation..." << endl;
    LOG_INFO("SQLClientManager::createDatabaseSchema(): Starting database schema creation...");  // log info

    // start a transaction
    executeQuery("BEGIN TRANSACTION;");
    LOG_INFO("SQLClientManager::createDatabaseSchema(): Starting SQL transaction...");  // log info

    bool success = true;  // flag to track execution success

    for (const auto& table : fileManager.configData["tables"].items()) {
        const string& tableName = table.key();
        string createTableSQL = "IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '" + tableName + "') BEGIN\n";
        createTableSQL += "    CREATE TABLE " + tableName + " (\n";

        bool firstColumn = true;
        for (const auto& column : table.value()["columns"].items()) {
            if (!firstColumn) {
                createTableSQL += ",\n    ";
            }
            firstColumn = false;

            const string& columnName = column.key();
            string columnType = column.value()["type"];

            // fix invalid types
            if (columnType == "TEXT") columnType = "NVARCHAR(255)";  // TEXT is deprecated, use NVARCHAR(255)
            if (columnType == "DOUBLE") columnType = "FLOAT";         // DOUBLE does not exist, use FLOAT

            const bool isPrimaryKey = column.value().contains("primary_key") && column.value()["primary_key"];
            const bool isAutoIncrement = column.value().contains("auto_increment") && column.value()["auto_increment"];
            const bool isNullable = column.value().contains("nullable") ? column.value()["nullable"].get<bool>() : true;
            string defaultValue = column.value().contains("default") ? column.value()["default"].get<string>() : "";

            // build column definition
            createTableSQL += "    " + columnName + " " + columnType;
            if (isPrimaryKey) {
                createTableSQL += " PRIMARY KEY";
            }
            if (isAutoIncrement) {
                createTableSQL += " IDENTITY(1,1)";  // auto-increment for SQL Server
            }
            if (!isNullable) {
                createTableSQL += " NOT NULL";
            }
            if (!defaultValue.empty()) {
                createTableSQL += " DEFAULT " + defaultValue;
            }
        }

        // handle foreign keys
        if (table.value().contains("foreign_keys")) {
            for (const auto& fk : table.value()["foreign_keys"]) {
                createTableSQL += ",\n    FOREIGN KEY (" + fk["column"].get<string>() + ") REFERENCES " +
                                  fk["references"]["table"].get<string>() + "(" +
                                  fk["references"]["column"].get<string>() + ")";
            }
        }

        createTableSQL += "\n);\nEND;";

        cout << "Executing: " << createTableSQL << endl;

        // execute SQL to create table
        if (!executeQuery(createTableSQL)) {
            LOG_ERROR("SQLClientManager::createDatabaseSchema(): Failed to create table.");  // log error
            cerr << "Failed to create table: " << tableName << ". Rolling back transaction." << endl;
            LOG_INFO("SQLClientManager::createDatabaseSchema(): Rolling back transaction.");  // log info
            success = false;
            break;  // Stop execution if one query fails
        }
    }

    // Commit or rollback based on success status
    if (success) {
        LOG_INFO("SQLClientManager::createDatabaseSchema(): Commit transaction.");  // log info
        executeQuery("COMMIT TRANSACTION;");
        cout << "Database schema created successfully!" << endl;
        LOG_INFO("SQLClientManager::createDatabaseSchema(): Database schema created successfully.");  // log info
    } else {
        executeQuery("ROLLBACK TRANSACTION;");
        LOG_INFO("SQLClientManager::createDatabaseSchema(): Rolling back transaction.");  // log info
        cerr << "Transaction rolled back due to errors!" << endl;
        LOG_ERROR("SQLClientManager::createDatabaseSchema(): Transaction rolled back due to errors.");  // log error
    }
}

// insert data by input table name, object id and value from opc ua node id
void SQLClientManager::insertBatchData(const string &table, const pmr::unordered_map<int, float> &data) {

}

// insert alarm data into Alarm table
void SQLClientManager::insertAlarm(const string &table, const string &alarm) {

}
