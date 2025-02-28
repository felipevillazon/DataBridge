//
// Created by felipevillazon on 12/19/24.
//

#include "FileManager.h"

#include "Logger.h"


// constructor
FileManager::FileManager()  {;}

// destructor
FileManager::~FileManager() {;}

// load config file with server settings
void FileManager::loadFile(const string& filename) {

  /* loadConfig try to open the configuration file which is located somewhere in the machine */

  LOG_INFO("ConfigManager::loadConfig(): Starting configuration file loading...");  // log info

  ifstream file(filename); // open the file for reading.
  if (!file.is_open()) {   // if file is not open
    LOG_ERROR(string("ConfigManager::loadConfig(): file not found: " + filename)); // log error
    throw std::runtime_error("Unable to open configuration file: " + filename); // message if failed to open file
  }

  try {
    configData = ordered_json::parse(file); // parse the JSON file into configData
  } catch (const std::exception& e) {
    LOG_ERROR(std::string("ConfigManager::loadConfig(): ") + e.what()); // log error
    throw std::runtime_error("Error parsing configuration file: " + std::string(e.what()));  // message if failed parsing file
  }

  LOG_INFO("ConfigManager::loadConfig(): Loaded configuration file: " + filename);
  // std::cout << "Configuration successfully loaded from " << filename << std::endl;   // message if parsing was done successfully // (avoid printing and use logger)
  }

// retrieve OPC Server login data
vector<string> FileManager::getOPCUAServerDetails() {

  /* This method retrieve the OPC UA server credentials from the configuration server and return a vector with the information */

  LOG_INFO("ConfigManager::getOPCUAServerDetails(): Start retrieving OPC UA connection credentials...");  // log info

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
          LOG_ERROR(errorMsg); // log error
          throw std::runtime_error("OPC UA server details not found in configuration " + errorMsg);
        }

      } else {    // if opcua credential are not inside file
        LOG_ERROR(string("ConfigManager::getOPCUAServerDetails(): OPC UA server details not found in configuration file " + filename));
        throw std::runtime_error("OPC UA server details not found in configuration.");
      }

  LOG_INFO("ConfigManager::getOPCUAServerDetails(): OPC UA connection credentials retrieved successfully."); // log info
  return credentialsOPCUA;  // return vector

}

// retrieve SQL database login data
vector<string> FileManager::getSQLConnectionDetails() {

  /* This method retrieve the SQL server credentials from the configuration server and return a vector with the information */

  LOG_INFO("ConfigManager::getSQLConnectionDetails(): Start retrieving SQL connection credentials...");  // log info

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
        LOG_ERROR(errorMsg);  // log error
        throw std::runtime_error("ConfigManager::getSQLConnectionDetails(): SQL server details not found in configuration " + errorMsg);
      }

    } else {   // if not credential for SQL connection found
      LOG_ERROR(string("ConfigManager::getSQLConnectionDetails(): OPC UA server details not found in configuration file " + filename)); // log error
      throw std::runtime_error("SQL server details not found in configuration.");
    }

    LOG_INFO("ConfigManager::getSQLConnectionDetails(): SQL connection credentials retrieved successfully."); // log info
    return credentialsSQL; // return vector
}

// map node_id to object_id and table_name
std::unordered_map<std::string, std::tuple<int, std::string>> FileManager::mapNodeIdToObjectId() {

  LOG_INFO("FileManager::mapNodeIdToObjectId(): Starting mapping between nodeId, objectId, and tableName...");  // log info

  std::unordered_map<std::string, std::tuple<int, std::string>> nodeIdMap;  // Map to store results
  std::vector<std::pair<std::string, std::tuple<int, std::string>>> batchData;  // Temporary batch container

  // Iterate dynamically through the JSON structure
  for (const auto& [_, category] : configData.items()) {
    if (!category.is_object()) continue;

    for (const auto& [_, entry] : category.items()) {
      if (!entry.is_object() || !entry.contains("columns") || !entry["columns"].is_object()) continue;

      const auto& columns = entry["columns"];
      int primaryId = -1;
      std::string nodeId;
      std::string tableName;

      // Extract primaryId (first integer found), nodeId, and tableName
      for (auto it = columns.begin(); it != columns.end(); ++it) {
        if (it.value().is_number_integer() && primaryId == -1) {
          primaryId = it.value().get<int>();  // First integer field is used as primaryId
        }
        if (it.key() == "node_id" && it.value().is_string()) {
          nodeId = it.value().get<std::string>();
        }
        if (it.key() == "table_name" && it.value().is_string()) {
          tableName = it.value().get<std::string>();
        }
      }

      // Only store valid entries
      if (!nodeId.empty() && primaryId != -1 && !tableName.empty()) {
        batchData.emplace_back(nodeId, std::make_tuple(primaryId, tableName));
      }
    }
  }

  // Bulk insert into unordered_map
  nodeIdMap.insert(batchData.begin(), batchData.end());

  return nodeIdMap;
}

