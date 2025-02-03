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
using json = nlohmann::json;


class ConfigManager {

public:
    explicit ConfigManager(const string& filename); // constructor
    ~ConfigManager(); // destructor

    void loadConfig(); //  load config file with server settings
    vector<string> getOPCUAServerDetails(); // retrieve OPC Server login data
    vector<string> getSQLConnectionDetails(); // retrieve SQL database login data

private:
    string filename;  // path to the location of the credential file
    json configData;  // store json file content
    vector<string> credentialsOPCUA; // opc ua server details
    vector<string> credentialsSQL;  // sql server details
};




#endif //CONFIGMANAGER_H

