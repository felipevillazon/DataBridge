//
// Created by felipevillazon on 12/19/24.
//

#ifndef SQLCLIENTMANAGER_H
#define SQLCLIENTMANAGER_H



#include <iostream>
#include <string>
#include <sql.h>
#include <sqltypes.h>
#include <unordered_map>
#include <vector>


using namespace std;

class SQLClientManager {

public:
    explicit SQLClientManager(const std::string& connectionString);  // constructor
    ~SQLClientManager(); // destructor

    bool connect();   // connect to database
    void disconnect(); // disconnect from database
    void createDatabaseSchema(const string& schemaFile); // create database tables from file with JSON format
    void insertBatchData(const string& table, const pmr::unordered_map<int, float>& data);  // insert data using table, object id and value
    void insertAlarm(const string& table, const string& alarm);  // insert alarm into table Alarms
    bool executeQuery(const string& query); // execute query from by using prestatement

private:
    SQLHANDLE sqlEnvHandle; // environment handle
    SQLHANDLE sqlConnHandle; // connection handle
    SQLHANDLE sqlStmtHandle{};  // statement handle
    SQLRETURN sqlReturnCode{};  // return code from ODBC functions
    SQLHSTMT hStmt{}; // statement handle
    string connectionString; // connection string
    SQLCHAR errMsg[1000]{};  // message string
};




#endif //SQLCLIENTMANAGER_H
