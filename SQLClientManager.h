//
// Created by felipevillazon on 12/19/24.
//

#ifndef SQLCLIENTMANAGER_H
#define SQLCLIENTMANAGER_H



#include <iostream>
#include <string>
#include <sql.h>
#include <sqltypes.h>
#include <vector>


using namespace std;

class SQLClientManager {

public:
    explicit SQLClientManager(const std::string& connectionString);  // constructor
    ~SQLClientManager(); // destructor

    bool connect();   // connect to database
    void disconnect(); // disconnect from database
    bool insert();  // insert data

private:
    SQLHANDLE sqlEnvHandle; // environment handle
    SQLHANDLE sqlConnHandle; // connection handle
    SQLRETURN sqlReturnCode{};  // return code from ODBC functions
    string connectionString; // connection string
};




#endif //SQLCLIENTMANAGER_H
