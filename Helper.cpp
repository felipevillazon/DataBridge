//
// Created by felipevillazon on 12/19/24.
//

#include "Helper.h"

#include <array>
#include <open62541/plugin/log.h>
#include <string>
#include <regex>
#include "Logger.h"
#include <fstream>
#include <mutex>
#include <stdexcept>

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
    sqlString = "Driver={MariaDB ODBC 3.2 Driver};Server=" + servername + ";Database=" + databasename +
               ";User=" + username + ";Password=" + password + ";PORT=3306;";


    LOG_INFO("DataProcessor::setSQLString(): SQL connection string built successfully."); // log info
    return sqlString;  // return sql string

}

// from nodeId string get nameSpaceIndex and identifier
std::array<int, 2> Helper::getNodeIdInfo(const std::string& nodeId) {


    int ns = 0, id = 0;

    // Regex to match the "ns=x;i=y" format where x and y are integers
    std::regex pattern(R"(^ns=(\d+);i=(\d+))");
    std::smatch matches;

    if (std::regex_match(nodeId, matches, pattern)) {
        ns = std::stoi(matches[1].str());  // Convert matched string to int (namespace)
        id = std::stoi(matches[2].str());  // Convert matched string to int (identifier)
    } else {

        throw std::invalid_argument("Invalid node ID. Wrong format or not existing in OPC UA server: " + nodeId);
    }

    return {ns, id};
}

// generated unique event id for alarm events
int Helper::generateEventId(const std::string& filename) {
    static std::mutex mtx;                 // thread-safe inside one process
    std::lock_guard<std::mutex> lock(mtx);

    int lastID = 0;

    // Read last ID (if file doesn't exist, start at 0)
    {
        std::ifstream infile(filename);
        if (infile.is_open()) {
            infile >> lastID;
        }
    }

    const int newID = lastID + 1;

    // Write using a simple overwrite (you can also do temp+rename for extra safety)
    {
        std::ofstream outfile(filename, std::ios::trunc);
        if (!outfile.is_open()) {
            throw std::runtime_error("Helper::generateEventId(): cannot open event id file for writing: " + filename);
        }
        outfile << newID;
        outfile.flush();
        if (!outfile.good()) {
            throw std::runtime_error("Helper::generateEventId(): failed writing event id to: " + filename);
        }
    }

    return newID;
}


