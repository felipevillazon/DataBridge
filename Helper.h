//
// Created by felipevillazon on 12/19/24.
//

#ifndef DATAPROCESSOR_H
#define DATAPROCESSOR_H



#include <string>
#include <vector>
#include <iostream>
#include <fstream>

using namespace std;

class Helper {

public:
    Helper(); // constructor
    ~Helper(); // destructor

    string setSQLString(const vector<string>& credentialsSQL); // set sql string from input credentials placed in a vector
    static std::array<int, 2> getNodeIdInfo(const string& nodeId);  // from nodeId string get nameSpaceIndex and identifier
    static int generateEventId(const std::string& filename);  // generated unique event id for alarm events

private:

    string sqlString;   // sql string
    string username;    // username string
    string password;    // password string
    string servername;    // servername string
    string databasename;    // databasename string
};



#endif //DATAPROCESSOR_H
