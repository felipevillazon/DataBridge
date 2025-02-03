//
// Created by felipevillazon on 12/19/24.
//

#include "ConfigManager.h"

#include "Logger.h"


// constructor
ConfigManager::ConfigManager(const string& filename) : filename(filename) {;}

// destructor
ConfigManager::~ConfigManager() {;}

// load config file with server settings
void ConfigManager::loadConfig() {

  /* loadConfig try to open the configuration file which is located somewhere in the machine */

  Logger::getInstance().logInfo("ConfigManager::loadConfig(): Starting configuration file loading...");  // log info

  ifstream file(filename); // open the file for reading.
  if (!file.is_open()) {   // if file is not open
    Logger::getInstance().logError(string("ConfigManager::loadConfig(): file not found: " + filename)); // log error
    throw std::runtime_error("Unable to open configuration file: " + filename); // message if failed to open file
  }

  try {
    configData = json::parse(file); // parse the JSON file into configData
  } catch (const std::exception& e) {
    Logger::getInstance().logError(std::string("ConfigManager::loadConfig(): ") + e.what()); // log error
    throw std::runtime_error("Error parsing configuration file: " + std::string(e.what()));  // message if failed parsing file
  }

  Logger::getInstance().logInfo("ConfigManager::loadConfig(): Loaded configuration file.");
  std::cout << "Configuration successfully loaded from " << filename << std::endl;   // message if parsing was done successfully
  }

// retrieve OPC Server login data
vector<string> ConfigManager::getOPCUAServerDetails() {

  /* This method retrieve the OPC UA server credentials from the configuration server and return a vector with the information */

  Logger::getInstance().logInfo("ConfigManager::getOPCUAServerDetails(): Start retrieving OPC UA connection credentials...");  // log info

      if (configData.contains("opcua")) {  // check if file contains pcua details

        auto opcuaData = configData["opcua"];   // opcua data holder
        bool hasError = false;     // error flag
        string errorMsg;     // error message

        if (!opcuaData["endpoint"].empty()) {    // check if endpoint information exists
          credentialsOPCUA.push_back(opcuaData["endpoint"]);   // if inside file, put it in vector
        } else {    // if not found
          hasError = true;   // set error flag to true
          errorMsg += "[missing: endpoint] ";    // add message to error message
        }

        if (!opcuaData["username"].empty()) {   // check if usernname inside file
          credentialsOPCUA.push_back(opcuaData["username"]);   // if inside, push back to vector
        } else {    // if not found username
          hasError = true;   // set error flag to true
          errorMsg += "[missing: username] ";   // add information about missing username to error message
        }

        if (!opcuaData["password"].empty()) {    // check if pasword inside file
          credentialsOPCUA.push_back(opcuaData["password"]);   // if password inside file, push back to vector
        } else {    // if password not found
          hasError = true;   // set error flag to true
          errorMsg += "[missing: password] ";   // add information about missing password to error message
        }

        if (hasError) {  // ff any of the credentials are missing, throw an error with a detailed message
          Logger::getInstance().logError(errorMsg); // log error
          throw std::runtime_error("OPC UA server details not found in configuration " + errorMsg);
        }

      } else {    // if opcua credential are not inside file
        Logger::getInstance().logError(string("ConfigManager::getOPCUAServerDetails(): OPC UA server details not found in configuration file " + filename));
        throw std::runtime_error("OPC UA server details not found in configuration.");
      }

  Logger::getInstance().logInfo("ConfigManager::getOPCUAServerDetails(): OPC UA connection credentials retrieved successfully."); // log info
  return credentialsOPCUA;  // return vector

}



// retrieve SQL database login data
vector<string> ConfigManager::getSQLConnectionDetails() {

  /* This method retrieve the SQL server credentials from the configuration server and return a vector with the information */

  Logger::getInstance().logInfo("ConfigManager::getSQLConnectionDetails(): Start retrieving SQL connection credentials...");  // log info

    if (configData.contains("sql")) {  // check if file contains SQL credentials

      auto sqlData = configData["sql"];    // SQL credentials holder
      bool hasError = false;     // error flag
      string errorMsg;    // error message

      if (!sqlData["host"].empty()) { // check if host (ip address) in file
        credentialsSQL.push_back(sqlData["host"]);   // if inside, push back to vector
      } else {    // if host not found
        hasError = true;   // set error flag to true
        errorMsg += "[missing: host] ";    // add information about missing host to error message
      }

      if (!sqlData["port"].empty()) {    // check if port number in file
        credentialsSQL.push_back(to_string(sqlData["port"]));  // if found, push back to vector (assuming integer)
      } else {    // if not found hot information
        hasError = true;    // set error flag to true
        errorMsg += "[missing: port] ";   // add information about missing port credentials to error message
      }

      if (!sqlData["username"].empty()) {   // check if username is contained in file
        credentialsSQL.push_back(sqlData["username"]);   // if found, push back to vecotr
      } else {    // if username not found in file
        hasError = true;    // set error file to true
        errorMsg += "[missing: username] ";    // add information about missing username to error message
      }

      if (!sqlData["password"].empty()) {   // check if password inside file
        credentialsSQL.push_back(sqlData["password"]);   // if found, push back to vector
      } else {  // if not found password
        hasError = true;    // set error flag to true
        errorMsg += "[missing: password] ";    // add information about missing password to error message
      }

      if (!sqlData["servername"].empty()) {   // check if servername inside file
        credentialsSQL.push_back(sqlData["servername"]);   // if found, push back to vector
      } else {  // if not found servername
        hasError = true;    // set error flag to true
        errorMsg += "[missing: servername] ";    // add information about missing servername to error message
      }

      if (!sqlData["databasename"].empty()) {   // check if databasename inside file
        credentialsSQL.push_back(sqlData["databasename"]);   // if found, push back to vector
      } else {  // if not found databasename
        hasError = true;    // set error flag to true
        errorMsg += "[missing: databasename] ";    // add information about missing databasename to error message
      }

      if (hasError) {   // ff any of the credentials are missing, throw an error with a detailed message
        Logger::getInstance().logError(errorMsg);  // log error
        throw std::runtime_error("ConfigManager::getSQLConnectionDetails(): SQL server details not found in configuration " + errorMsg);
      }

    } else {   // if not credential for SQL connection found
      Logger::getInstance().logError(string("ConfigManager::getSQLConnectionDetails(): OPC UA server details not found in configuration file " + filename)); // log error
      throw std::runtime_error("SQL server details not found in configuration.");
    }

    Logger::getInstance().logInfo("ConfigManager::getSQLConnectionDetails(): SQL connection credentials retrieved successfully."); // log info
    return credentialsSQL; // return vector
}
