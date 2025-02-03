//
// Created by felipevillazon on 12/19/24.
//

#include "SQLClientManager.h"
#include <sqlext.h>

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

    Logger::getInstance().logInfo("SQLClientManager::connect(): Connecting to SQL server...");  // log info

    // step 1: allocate environment handle
    sqlReturnCode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &sqlEnvHandle);
    if (sqlReturnCode != SQL_SUCCESS && sqlReturnCode != SQL_SUCCESS_WITH_INFO) {
        Logger::getInstance().logError("SQLClientManager::connect(): Failed to allocate memory for SQL connection.");  // log error
        std::cerr << "Error allocating ODBC environment handle." << std::endl;
        return false;
    }

    // step 2: set ODBC version
    sqlReturnCode = SQLSetEnvAttr(sqlEnvHandle, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);
    if (sqlReturnCode != SQL_SUCCESS && sqlReturnCode != SQL_SUCCESS_WITH_INFO) {
        Logger::getInstance().logError("SQLClientManager::connect(): Failed to set ODBC environment attributes."); // log error
        std::cerr << "Error setting ODBC version." << std::endl;
        return false;
    }

    // step 3: allocate connection handle
    sqlReturnCode = SQLAllocHandle(SQL_HANDLE_DBC, sqlEnvHandle, &sqlConnHandle);
    if (sqlReturnCode != SQL_SUCCESS && sqlReturnCode != SQL_SUCCESS_WITH_INFO) {
        Logger::getInstance().logError("SQLClientManager::connect(): Failed to allocate ODBC connection handle.");  // log error
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
        Logger::getInstance().logError("SQLClientManager::connect(): SQLGetDiagRec failed.");  // log error
        std::cerr << "Connection failed. SQL State: " << sqlState
                  << ", Message: " << errorMsg << std::endl;
        return false;
    }

    Logger::getInstance().logInfo("SQLClientManager::connect(): Successfully connected to database.");  // log info
    std::cout << "Database connection established successfully!" << std::endl;
    return true;

}

// disconnect from SQL server
void SQLClientManager::disconnect() {

    /* Disconnect manage the disconnection of the SQL client with the SQL server */

    Logger::getInstance().logInfo("SQLClientManager::disconnect(): Disconnecting SQL server."); // log info

    if (sqlConnHandle) {    // check if connected to server
        sqlReturnCode = SQLDisconnect(sqlConnHandle);   // disconnect from server
        if (sqlReturnCode == SQL_SUCCESS || sqlReturnCode == SQL_SUCCESS_WITH_INFO) {   // check if disconnection is successful
            Logger::getInstance().logInfo("SQLClientManager::disconnect(): Successfully disconnected from SQL server."); // log info
            std::cout << "Database disconnected successfully." << std::endl;    // return positive message
        } else {
            Logger::getInstance().logError("SQLClientManager::disconnect(): Failed to disconnect from SQL server");  // log error
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

// insert data
bool SQLClientManager::insert() {

    return true;
}
