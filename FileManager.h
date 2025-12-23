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


    std::vector<std::tuple<opcua::NodeId, opcua::NodeId, opcua::NodeId>> getNodeIdListAlarm();

    // new methods 23/12/2025

    // If this is the first time it sees the file, it initializes state and returns false.
    bool hasFileBeenModified(const std::string& filename);

    // Convenience: checks if modified and reloads configData if yes.
    bool reloadFileIfModified(const std::string& filename);

private:

    vector<string> credentialsOPCUA; // opc ua server details
    vector<string> credentialsSQL;  // sql server details

    // Track last seen write time per file
    std::unordered_map<std::string, std::filesystem::file_time_type> lastWriteTime_;
    std::mutex fileWatchMutex_;
};




#endif //CONFIGMANAGER_H

