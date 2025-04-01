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
#include <open62541pp/client.hpp>
#include <open62541pp/open62541pp.h>

using namespace std;

class SQLClientManager {

public:
    explicit SQLClientManager(const std::string& connectionString);  // constructor
    ~SQLClientManager(); // destructor

    bool connect();   // connect to database
    void disconnect(); // disconnect from database
    void createDatabaseSchema(const string& schemaFile); // create database tables from file with JSON format
    bool executeQuery(const string& query); // execute query using string queries
    void prepareInsertStatements(const std::unordered_map<std::string,  std::unordered_map<int, float>>& tableObjects); // prepare insert statements
    bool insertBatchData(const std::unordered_map<std::string,  std::unordered_map<int, float>>& tableObjects); // insert batch data into tables
    void insertAlarm(const string& table, const unordered_map<opcua::NodeId, std::tuple<int, int, int, int, int, float, int>>& values, const string &type);  // insert alarm into table Alarms

private:
    SQLHANDLE sqlEnvHandle; // environment handle
    SQLHANDLE sqlConnHandle; // connection handle
    SQLHANDLE sqlStmtHandle{};  // statement handle
    SQLRETURN sqlReturnCode{};  // return code from ODBC functions
    SQLHSTMT hStmt{}; // statement handle
    string connectionString; // connection string
    SQLCHAR errMsg[1000]{};  // message string
    std::unordered_map<std::string, SQLHSTMT> preparedStatements; // store prepared statements
};




#endif //SQLCLIENTMANAGER_H
