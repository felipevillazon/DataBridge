//
// Created by felipevillazon on 12/19/24.
//

#include "FileManager.h"

#include "Logger.h"
#include "Helper.h"
#include <filesystem>
#include <system_error>

// constructor - IN USED
FileManager::FileManager()  {;}

// destructor - IN USED
FileManager::~FileManager() {;}

// load config file with server settings - IN USED
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

// retrieve OPC Server login data - IN USED
vector<string> FileManager::getOPCUAServerDetails(const std::string& plc) {

    /* This method retrieves the OPC UA server credentials for the specified PLC from the configuration file
       and returns a vector with the information for the associated PLC (either PLC_1 or PLC_2). */
  credentialsOPCUA.clear();


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

// retrieve SQL database login data - IN USED
vector<string> FileManager::getSQLConnectionDetails() {

  /* This method retrieve the SQL server credentials from the configuration server and return a vector with the information */

  credentialsSQL.clear();

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
      LOG_ERROR(string("ConfigManager::getSQLConnectionDetails(): SQL server details not found in configuration file.")); // log error
      throw std::runtime_error("SQL server details not found in configuration.");
    }

    LOG_INFO("ConfigManager::getSQLConnectionDetails(): SQL connection credentials retrieved successfully."); // log info
    return credentialsSQL; // return vector
}

// map object_node_id to object_id and table_name - IN USED
std::unordered_map<std::string, std::tuple<int, std::string>> FileManager::mapNodeIdToObjectId() {
    ::LOG_INFO("FileManager::mapNodeIdToObjectId(): Building nodeId -> (object_id, table_name) map...");

    // Keep your interface (minimal changes in rest of code)
    std::unordered_map<std::string, std::tuple<int, std::string>> nodeIdMap;

    // Force a constant readings table (you can change it in one place later)
    static constexpr auto READINGS_TABLE = "object_readings";

    // Validate expected structure
    if (!configData.contains("objects") || !configData["objects"].is_object()) {
        ::LOG_ERROR("FileManager::mapNodeIdToObjectId(): Missing or invalid 'objects' section in JSON.");
        return nodeIdMap;
    }

    // Iterate ONLY objects
    for (const auto& [objectKey, entry] : configData["objects"].items()) {
        if (!entry.is_object()) {
            ::LOG_ERROR("FileManager::mapNodeIdToObjectId(): object '" + objectKey + "' is not a JSON object. Skipping.");
            continue;
        }

        if (!entry.contains("columns") || !entry["columns"].is_object()) {
            ::LOG_ERROR("FileManager::mapNodeIdToObjectId(): object '" + objectKey + "' missing 'columns'. Skipping.");
            continue;
        }

        const auto& columns = entry["columns"];

        // Required fields
        int objectId = -1;
        std::string nodeId;

        if (columns.contains("object_id") && columns["object_id"].is_number_integer()) {
            objectId = columns["object_id"].get<int>();
        } else {
            ::LOG_ERROR("FileManager::mapNodeIdToObjectId(): object '" + objectKey + "' missing/invalid 'object_id'. Skipping.");
            continue;
        }

        if (columns.contains("object_node_id") && columns["object_node_id"].is_string()) {
            nodeId = columns["object_node_id"].get<std::string>();
        } else {
            ::LOG_ERROR("FileManager::mapNodeIdToObjectId(): object '" + objectKey + "' missing/invalid 'object_node_id'. Skipping.");
            continue;
        }

        if (nodeId.empty() || objectId < 0) {
            ::LOG_ERROR("FileManager::mapNodeIdToObjectId(): object '" + objectKey + "' has empty nodeId or invalid objectId. Skipping.");
            continue;
        }

        // Insert mapping (if duplicates exist, last wins — we also log it)
        auto it = nodeIdMap.find(nodeId);
        if (it != nodeIdMap.end()) {
            ::LOG_ERROR("FileManager::mapNodeIdToObjectId(): Duplicate object_node_id '" + nodeId +
                        "' found. Overwriting previous mapping.");
        }

        nodeIdMap[nodeId] = std::make_tuple(objectId, std::string(READINGS_TABLE));
    }

    ::LOG_INFO("FileManager::mapNodeIdToObjectId(): Done. Loaded " + std::to_string(nodeIdMap.size()) + " mappings.");
    return nodeIdMap;
}

// check if file has been modified - IN USED
bool FileManager::hasFileBeenModified(const std::string& filename) {
  std::lock_guard<std::mutex> lock(fileWatchMutex_);

  std::error_code ec;
  if (!std::filesystem::exists(filename, ec) || ec) {
    LOG_ERROR("FileManager::hasFileBeenModified(): file does not exist or cannot be accessed: " + filename);
    return false; // or throw, but for a watcher it's usually better to not crash
  }

  const auto currentWriteTime = std::filesystem::last_write_time(filename, ec);
  if (ec) {
    LOG_ERROR("FileManager::hasFileBeenModified(): could not read last_write_time for: " + filename);
    return false;
  }

  const auto it = lastWriteTime_.find(filename);

  // First time we see this file: store timestamp, do NOT treat as "modified"
  if (it == lastWriteTime_.end()) {
    lastWriteTime_[filename] = currentWriteTime;
    return false;
  }

  // If timestamp differs, mark as modified and update stored value
  if (currentWriteTime != it->second) {
    it->second = currentWriteTime;
    return true;
  }

  return false;
}

// Check if file has been modified and reload if necessary - IN USED
bool FileManager::reloadFileIfModified(const std::string& filename) {
  if (!hasFileBeenModified(filename)) return false;

  try {
    LOG_INFO("... reloading ...");
    loadFile(filename);
    return true;
  } catch (const std::exception& e) {
    LOG_ERROR(std::string("reloadFileIfModified(): reload failed, keeping old config. Reason: ") + e.what());
    return false;
  }
}

// map severity node_id, acknowledged_node_id and fixed_node_id from file - IN USED
std::vector<FileManager::AlarmNodeMapping> FileManager::getAlarmNodeMappings() {
    std::vector<AlarmNodeMapping> out;

    // Support both keys: "objects" or "sensors"
    const char* rootKey = nullptr;
    if (configData.contains("objects")) rootKey = "objects";
    else if (configData.contains("sensors")) rootKey = "sensors";

    if (!rootKey) {
        LOG_ERROR("FileManager::getAlarmNodeMappings(): No 'objects' or 'sensors' section found.");
        return out;
    }

    for (const auto& item : configData[rootKey].items()) {
        const auto& obj = item.value();

        if (!obj.contains("columns") || !obj["columns"].is_object()) continue;

        const auto& cols = obj["columns"];
        const int objectId = cols.contains("object_id") && cols["object_id"].is_number_integer()
                                 ? cols["object_id"].get<int>()
                                 : -1;
        const int systemId = cols.contains("system_id") && cols["system_id"].is_number_integer()
                                 ? cols["system_id"].get<int>()
                                 : -1;

        if (!obj.contains("alarm") || !obj["alarm"].contains("columns")) continue;
        const auto& alarmCols = obj["alarm"]["columns"];

        // required
        if (!alarmCols.contains("severity_node_id")) continue;

        // ack naming: support both
        const bool hasAck =
            alarmCols.contains("ack_node_id") || alarmCols.contains("acknowledged_node_id");
        if (!hasAck) continue;

        const std::string severityStr = alarmCols["severity_node_id"].get<std::string>();
        const std::string ackStr = alarmCols.contains("ack_node_id")
                                       ? alarmCols["ack_node_id"].get<std::string>()
                                       : alarmCols["acknowledged_node_id"].get<std::string>();

        const auto sevInfo = Helper::getNodeIdInfo(severityStr);
        const auto ackInfo = Helper::getNodeIdInfo(ackStr);

        AlarmNodeMapping m;
        m.object_id = objectId;
        m.system_id = systemId;
        m.severity = opcua::NodeId(sevInfo[0], sevInfo[1]);
        m.ack = opcua::NodeId(ackInfo[0], ackInfo[1]);

        // optional error_code
        if (alarmCols.contains("error_code_node_id")) {
            const std::string errStr = alarmCols["error_code_node_id"].get<std::string>();
            const auto errInfo = Helper::getNodeIdInfo(errStr);
            m.has_error_code = true;
            m.error_code = opcua::NodeId(errInfo[0], errInfo[1]);
        }

        // optional value
        // (you can name this in JSON "value_node_id" or reuse "object_node_id" - choose one)
        if (alarmCols.contains("value_node_id")) {
            const std::string valStr = alarmCols["value_node_id"].get<std::string>();
            const auto valInfo = Helper::getNodeIdInfo(valStr);
            m.has_value = true;
            m.value = opcua::NodeId(valInfo[0], valInfo[1]);
        }

        // optional system_state (for system alarms)
        if (alarmCols.contains("system_state_node_id")) {
            const std::string stStr = alarmCols["system_state_node_id"].get<std::string>();
            const auto stInfo = Helper::getNodeIdInfo(stStr);
            m.has_system_state = true;
            m.system_state = opcua::NodeId(stInfo[0], stInfo[1]);
        }

        out.push_back(std::move(m));
    }

    LOG_INFO("FileManager::getAlarmNodeMappings(): Loaded " + std::to_string(out.size()) + " alarm mappings.");
    return out;
}

// Convenience: detect which root table exists (systems/plcs/equipment/objects)
std::optional<std::string> FileManager::detectSingleRootTable() const {
    // If file contains exactly one root key, return it.
    // Helps when you load "systems.json" and want to auto-detect "systems".
    if (!configData.is_object()) return std::nullopt;

    std::string found;
    int count = 0;

    for (auto it = configData.begin(); it != configData.end(); ++it) {
        if (!it.value().is_object()) continue;
        found = it.key();
        ++count;
    }

    if (count == 1) return found;
    return std::nullopt;
}

// Convenience: extract rows from a static table JSON file
std::vector<FileManager::Row> FileManager::extractTableRows(const std::string& tableName) const {
    std::vector<Row> rows;

    if (!configData.contains(tableName) || !configData[tableName].is_object()) {
        LOG_ERROR("FileManager::extractTableRows(): Missing or invalid root key: " + tableName);
        return rows;
    }

    const auto& root = configData[tableName];

    for (const auto& item : root.items()) {
        const auto& entry = item.value();
        if (!entry.contains("columns") || !entry["columns"].is_object()) continue;

        const auto& cols = entry["columns"];
        Row row;

        for (auto it = cols.begin(); it != cols.end(); ++it) {
            const std::string colName = it.key();
            const auto& v = it.value();

            if (v.is_null()) {
                row[colName] = nullptr;
            } else if (v.is_boolean()) {
                row[colName] = v.get<bool>();
            } else if (v.is_number_integer()) {
                row[colName] = static_cast<long long>(v.get<long long>());
            } else if (v.is_number_float()) {
                row[colName] = static_cast<double>(v.get<double>());
            } else if (v.is_string()) {
                row[colName] = v.get<std::string>();
            } else {
                // For objects/arrays, store as JSON text (safe fallback)
                row[colName] = v.dump();
            }
        }

        rows.push_back(std::move(row));
    }

    LOG_INFO("FileManager::extractTableRows(): Extracted " + std::to_string(rows.size()) +
             " rows from table '" + tableName + "'");
    return rows;
}

// Convenience: extract rows from a static table JSON file
std::vector<ordered_json> FileManager::getStaticRows(const std::string& tableKey) const {
    std::vector<ordered_json> rows;

    if (!configData.contains(tableKey) || !configData[tableKey].is_object()) {
        LOG_ERROR("FileManager::getStaticRows(): missing or invalid key: " + tableKey);
        return rows;
    }

    for (const auto& item : configData[tableKey].items()) {
        const auto& entry = item.value();
        if (!entry.contains("columns") || !entry["columns"].is_object()) continue;
        rows.push_back(entry["columns"]);
    }

    LOG_INFO("FileManager::getStaticRows(): Loaded " + std::to_string(rows.size()) +
             " rows from section '" + tableKey + "'");
    return rows;
}




