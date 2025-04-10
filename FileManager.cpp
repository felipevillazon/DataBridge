//
// Created by felipevillazon on 12/19/24.
//

#include "FileManager.h"

#include "Logger.h"
#include "Helper.h"


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
vector<string> FileManager::getOPCUAServerDetails(const std::string& plc) {

    /* This method retrieves the OPC UA server credentials for the specified PLC from the configuration file
       and returns a vector with the information for the associated PLC (either PLC_1 or PLC_2). */

    LOG_INFO("ConfigManager::getOPCUAServerDetails(): Start retrieving OPC UA connection credentials for " + plc);  // log info

    if (configData.contains("opcua")) {  // Check if file contains OPC UA details
        auto opcuaData = configData["opcua"];   // OPC UA data holder
        bool hasError = false;     // Error flag
        string errorMsg;          // Error message

        if (opcuaData.contains(plc)) {  // Check if PLC (PLC_1 or PLC_2) exists in the config
            auto plcData = opcuaData[plc];  // Get data for the specific PLC

            // Check and add the endpoint
            if (!plcData["endpoint"].empty()) {
                credentialsOPCUA.push_back(plcData["endpoint"]);
            } else {
                hasError = true;
                errorMsg += "[missing: endpoint] ";
            }

            // Check and add the username
            if (!plcData["username"].empty()) {
                credentialsOPCUA.push_back(plcData["username"]);
            } else {
                hasError = true;
                errorMsg += "[missing: username] ";
            }

            // Check and add the password
            if (!plcData["password"].empty()) {
                credentialsOPCUA.push_back(plcData["password"]);
            } else {
                hasError = true;
                errorMsg += "[missing: password] ";
            }

            if (hasError) {  // If any of the credentials are missing, throw an error
                LOG_ERROR(errorMsg);  // Log error
                throw std::runtime_error("OPC UA server details not found for " + plc + " " + errorMsg);
            }

        } else {  // If the specified PLC is not found
            LOG_ERROR("[missing: " + plc + " configuration]");
            throw std::runtime_error("OPC UA server details for " + plc + " not found in configuration.");
        }

    } else {  // If OPC UA credentials are not found in the config file
        LOG_ERROR("ConfigManager::getOPCUAServerDetails(): OPC UA server details not found in configuration file.");
        throw std::runtime_error("OPC UA server details not found in configuration.");
    }

    LOG_INFO("ConfigManager::getOPCUAServerDetails(): OPC UA connection credentials for " + plc + " retrieved successfully.");  // Log info
    return credentialsOPCUA;  // Return vector with credentials for the specified PLC
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

// map object_node_id to object_id and table_name
std::unordered_map<std::string, std::tuple<int, std::string>> FileManager::mapNodeIdToObjectId() {

  LOG_INFO("FileManager::mapNodeIdToObjectId(): Starting mapping between nodeId, objectId, and tableName...");  // log info

  // <object_node_id , object_id, table_name>
  std::unordered_map<std::string, std::tuple<int, std::string>> nodeIdMap;  // Map to store results

  // <object_node_id , object_id, table_name>
  std::vector<std::pair<std::string, std::tuple<int, std::string>>> batchData;  // Temporary batch container

  // iterate dynamically through the JSON structure
  for (const auto& [_, category] : configData.items()) {
    if (!category.is_object()) continue;

    for (const auto& [_, entry] : category.items()) {
      if (!entry.is_object() || !entry.contains("columns") || !entry["columns"].is_object()) continue;

      const auto& columns = entry["columns"];
      int objectId;
      std::string nodeId;
      std::string tableName;

      // Extract primaryId (first integer found), nodeId, and tableName
      for (auto it = columns.begin(); it != columns.end(); ++it) {
        if (it.key() == "object_id" && it.value().is_string()) {    // getting object_id to identify object-nodeid relation
          objectId = it.value().get<int>();               // getting integer
        }
        if (it.key() == "object_node_id" && it.value().is_string()) {   // getting object_node_id to get object value from opcua server
          nodeId = it.value().get<std::string>();   // node id string
        }
        if (it.key() == "table_name" && it.value().is_string()) {      // getting table_name to realize reading table to later insert
          tableName = it.value().get<std::string>();    // table name string
        }
      }

      // only store valid entries
      if (!nodeId.empty() && !std::to_string(objectId).empty() && !tableName.empty()) {     // check if entries are not empty
        batchData.emplace_back(nodeId, std::make_tuple(objectId, tableName));  // place into batchData
      }
    }
  }

  // bulk insert into unordered_map
  nodeIdMap.insert(batchData.begin(), batchData.end());

  return nodeIdMap;
}


// map severity node_id, acknowledged_node_id and fixed_node_id from file
// Implementation of getNodeIdListAlarm
std::vector<std::tuple<opcua::NodeId, opcua::NodeId, opcua::NodeId>> FileManager::getNodeIdListAlarm() {
  std::vector<std::tuple<opcua::NodeId, opcua::NodeId, opcua::NodeId>> alarmRelatedNodeId;

  // Check if the "sensors" key exists in the configData
  if (configData.contains("objects")) {
    // Iterate through all sensors
    for (const auto& objectItem : configData["objects"].items()) {
      const auto& object = objectItem.value();  // Get the sensor data

      // Check if "alarm" exists and has the required columns
      if (object.contains("alarm") && object["alarm"].contains("columns")) {
        const auto& alarmColumns = object["alarm"]["columns"];

        // Ensure all three required node_ids are available
        if (alarmColumns.contains("severity_node_id") &&
            alarmColumns.contains("acknowledged_node_id") &&
            alarmColumns.contains("fixed_node_id")) {

          // Create NodeId instances and store them in the tuple
          std::array<int, 2> severityInfo = Helper::getNodeIdInfo(alarmColumns["severity_node_id"].get<std::string>());
          std::array<int, 2> acknowledgedInfo = Helper::getNodeIdInfo(alarmColumns["acknowledged_node_id"].get<std::string>());
          std::array<int, 2> fixedInfo = Helper::getNodeIdInfo(alarmColumns["fixed_node_id"].get<std::string>());

          // Now, create the NodeId instances using the extracted namespace and identifier
          opcua::NodeId severityNodeId(severityInfo[0], severityInfo[1]);  // severityInfo[0] is namespaceIndex, severityInfo[1] is identifier
          opcua::NodeId acknowledgedNodeId(acknowledgedInfo[0], acknowledgedInfo[1]); // Same for acknowledged node
          opcua::NodeId fixedNodeId(fixedInfo[0], fixedInfo[1]); // Same for fixed node

          // Add the tuple to the result vector
          alarmRelatedNodeId.emplace_back(severityNodeId, acknowledgedNodeId, fixedNodeId);
            }
      }
    }
  }

  return alarmRelatedNodeId;
}

