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

using namespace std;
using ordered_json = nlohmann::ordered_json;


class FileManager {

public:
    explicit FileManager(); // constructor
    ~FileManager(); // destructor

    void loadFile(const string& filename); //  load JSON file
    vector<string> getOPCUAServerDetails(); // retrieve OPC Server login data
    vector<string> getSQLConnectionDetails(); // retrieve SQL database login data
    unordered_map<std::string, std::tuple<int, std::string>> mapNodeIdToObjectId();  // map node id to sensor id and table name

    string filename;  // path to the location of the credential file
    ordered_json configData;  // store json file content

private:

    vector<string> credentialsOPCUA; // opc ua server details
    vector<string> credentialsSQL;  // sql server details
};




#endif //CONFIGMANAGER_H

