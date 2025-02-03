//
// Created by felipevillazon on 12/19/24.
//

#include "DataProcessor.h"

#include "Logger.h"

// constructor
DataProcessor::DataProcessor() {;}

// destructor
DataProcessor::~DataProcessor() {;}

// convert OPC UA Server data into format for SQL database
void DataProcessor::processData() {;}



string DataProcessor::setSQLString(const vector<string>& credentialsSQL) {

    /* Method to set or to build the SQL string connection in the right format for the SQL server */

    Logger::getInstance().logInfo("DataProcessor::setSQLString(): Starting building SQL connection string..."); // log info

    if (credentialsSQL.size() != 6) { // check vector size
        Logger::getInstance().logError("DataProcessor::setSQLString(): Missing credentials.");  // log error
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

    Logger::getInstance().logInfo("DataProcessor::setSQLString(): SQL connection string built successfully."); // log info
    return sqlString;  // return sql string

}