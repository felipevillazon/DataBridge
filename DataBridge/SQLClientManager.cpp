//
// Created by felipe villazon on 12/11/24.
//

#include "SQLClientManager.h"
#include <sqlext.h>

// constructor
SQLClientManager::SQLClientManager(const string& connectionString)
    : sqlEnvHandle(nullptr), sqlConnHandle(nullptr), connectionString(connectionString) {}   // initialize some variables

// destructor
SQLClientManager::~SQLClientManager() {

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

    // step 1: allocate environment handle
    sqlReturnCode = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &sqlEnvHandle);
    if (sqlReturnCode != SQL_SUCCESS && sqlReturnCode != SQL_SUCCESS_WITH_INFO) {
        std::cerr << "Error allocating ODBC environment handle." << std::endl;
        return false;
    }

    // step 2: set ODBC version
    sqlReturnCode = SQLSetEnvAttr(sqlEnvHandle, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);
    if (sqlReturnCode != SQL_SUCCESS && sqlReturnCode != SQL_SUCCESS_WITH_INFO) {
        std::cerr << "Error setting ODBC version." << std::endl;
        return false;
    }

    // step 3: allocate connection handle
    sqlReturnCode = SQLAllocHandle(SQL_HANDLE_DBC, sqlEnvHandle, &sqlConnHandle);
    if (sqlReturnCode != SQL_SUCCESS && sqlReturnCode != SQL_SUCCESS_WITH_INFO) {
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
        std::cerr << "Connection failed. SQL State: " << sqlState
                  << ", Message: " << errorMsg << std::endl;
        return false;
    }

    std::cout << "Database connection established successfully!" << std::endl;
    return true;

}

// disconnect from SQL server
void SQLClientManager::disconnect() {

    if (sqlConnHandle) {    // check if connected to server
        sqlReturnCode = SQLDisconnect(sqlConnHandle);   // disconnect from server
        if (sqlReturnCode == SQL_SUCCESS || sqlReturnCode == SQL_SUCCESS_WITH_INFO) {   // check if disconnection is successful
            std::cout << "Database disconnected successfully." << std::endl;    // return positive message
        } else {
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



