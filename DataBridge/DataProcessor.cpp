//
// Created by felipe on 12/11/24.
//

#include "DataProcessor.h"

// constructor
DataProcessor::DataProcessor() {;}

// destructor
DataProcessor::~DataProcessor() {;}

// convert OPC UA Server data into format for SQL database
void DataProcessor::processData() {;}



string DataProcessor::setSQLString(vector<string>& credentialsSQL) {

  if (credentialsSQL.size() != 4) {
    throw runtime_error("Invalid SQL credentials.");
  }

  string host = credentialsSQL[0];
  string port = credentialsSQL[1];  // This could be an empty string if no custom port is used
  string username = credentialsSQL[2];
  string password = credentialsSQL[3];

  string server = host;
  if (!port.empty()) {
    server += "," + port;  // Add port if provided
  }

  // Constructing the connection string
  string connectionString = "Driver={SQL Server};Server=" + server + ";Database=" + databaseName +
                            ";Uid=" + username + ";Pwd=" + password + ";";

  return connectionString;



  return sqlString;  // return sql string

}