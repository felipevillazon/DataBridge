//
// Created by felipevillazon on 12/19/24.
//

#include "Helper.h"

#include <array>
#include <open62541/plugin/log.h>

#include "Logger.h"

// constructor
Helper::Helper() {;}

// destructor
Helper::~Helper() {;}

// build communication string for sql connection
string Helper::setSQLString(const vector<string>& credentialsSQL) {

    /* Method to set or to build the SQL string connection in the right format for the SQL server */

    LOG_INFO("DataProcessor::setSQLString(): Starting building SQL connection string..."); // log info

    if (credentialsSQL.size() != 6) { // check vector size
        LOG_ERROR("DataProcessor::setSQLString(): Missing credentials.");  // log error
        throw std::runtime_error("Invalid SQL credentials. Current size: " +
                                  std::to_string(credentialsSQL.size()) +
                                  ". Expected size: 6.");
    }
    username = credentialsSQL[2];  // retrieve username
    password = credentialsSQL[3];   // retrieve password
    servername = credentialsSQL[4];  // retrieve servername
    databasename = credentialsSQL[5];   // retrieve databasename

    // Constructing the connection string
    sqlString = "Driver={ODBC Driver 18 for SQL Server};Server=" + servername + ";Database=" + databasename +
                              ";Uid=" + username + ";Pwd=" + password + ";TrustServerCertificate=yes;";

    LOG_INFO("DataProcessor::setSQLString(): SQL connection string built successfully."); // log info
    return sqlString;  // return sql string

}

// from nodeId string get nameSpaceIndex and identifier
std::array<int, 2> Helper::getNodeIdInfo(const string& nodeId) {

    int ns = 0, id = 0;  // initialize values of ns and id
    char ignore;  // to ignore '=' and ';' characters
    std::istringstream ss(nodeId);

    // parsing the format "ns=INT;i=INT"
    if (ss >> ignore >> ignore >> ns >> ignore >> ignore >> id) {
        return {ns, id};
    }
    throw std::invalid_argument("Invalid NodeId format");
}


