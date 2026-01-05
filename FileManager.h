//
// Created by felipevillazon on 12/19/24.
//

#ifndef CONFIGMANAGER_H
#define CONFIGMANAGER_H


#include <string>
#include <nlohmann/json.hpp>
#include <iostream>
#include <fstream>
#include <vector>
#include <open62541pp/node.hpp>
#include <unordered_map>
#include <mutex>
#include <filesystem>

using namespace std;
using ordered_json = nlohmann::ordered_json;


class FileManager {

public:
    explicit FileManager(); // constructor
    ~FileManager(); // destructor

    void loadFile(const string& filename); //  load JSON file
    vector<string> getOPCUAServerDetails(const std::string& plc); // retrieve OPC Server login data
    vector<string> getSQLConnectionDetails(); // retrieve SQL database login data

    // <NODE_ID, OBJECT_ID, TABLE_NAME>
    unordered_map<std::string, std::tuple<int, std::string>> mapNodeIdToObjectId();  // map node id to sensor id and table name

    string filename;  // path to the location of the credential file
    ordered_json configData;  // store json file content

    // If this is the first time it sees the file, it initializes state and returns false.
    bool hasFileBeenModified(const std::string& filename);

    // Convenience: checks if modified and reloads configData if yes.
    bool reloadFileIfModified(const std::string& filename);

    struct AlarmNodeMapping {
        int object_id = -1;
        int system_id = -1;

        opcua::NodeId severity;
        opcua::NodeId ack;

        bool has_error_code = false;
        opcua::NodeId error_code;

        bool has_value = false;
        opcua::NodeId value;

        bool has_system_state = false;
        opcua::NodeId system_state;
    };

    std::vector<AlarmNodeMapping> getAlarmNodeMappings();

    using SqlValue = std::variant<std::nullptr_t, long long, double, bool, std::string>;
    using Row = std::unordered_map<std::string, SqlValue>;

    // Extract rows from a static table JSON file that follows:
    // { "<tableName>": { "<entryKey>": { "columns": { ... } }, ... } }
    std::vector<Row> extractTableRows(const std::string& tableName) const;

    // Convenience: detect which root table exists (systems/plcs/equipment/objects)
    std::optional<std::string> detectSingleRootTable() const;

    // Return a vector of row objects for a static table file.
    // Example: tableKey="systems" returns each entry's "columns" object.
    std::vector<ordered_json> getStaticRows(const std::string& tableKey) const;



private:

    vector<string> credentialsOPCUA; // opc ua server details
    vector<string> credentialsSQL;  // sql server details

    // Track last seen write time per file
    std::unordered_map<std::string, std::filesystem::file_time_type> lastWriteTime_;
    std::mutex fileWatchMutex_;
};




#endif //CONFIGMANAGER_H

