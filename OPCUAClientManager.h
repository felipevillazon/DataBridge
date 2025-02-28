//
// Created by felipevillazon on 12/19/24.
//

#ifndef OPCUACLIENTMANAGER_H
#define OPCUACLIENTMANAGER_H


#include <iostream>
#include <string>
#include <open62541pp/client.hpp>
#include <open62541pp/open62541pp.h>
#include <chrono>
#include <queue>


using namespace std;
using namespace opcua;


class OPCUAClientManager {

public:

    // constructor
    explicit OPCUAClientManager(const string& endpointUrl, const string& username = "", const string& password = "");

    // destructor
    ~OPCUAClientManager();

    // connect to OPC UA Server
    bool connect();

    // disconnect from OPC UA Server
    void disconnect();

    // poll values from node ids asynchronously
    void pollNodeValues(const std::unordered_map<std::string, std::tuple<int, std::string>>& nodeMap);

    // group values and objects ids by table names, also convert DataValue to float to match SQL datatype
    void groupByTableName(const std::unordered_map<std::string, std::tuple<int, std::string, opcua::DataValue>>& monitoredNodes);

    // instance UA_Client to create client
    Client client;

    // stores monitored node values: <nodeId, <objectId, tableName, DataValue>>
    std::unordered_map<std::string, std::tuple<int, std::string, opcua::DataValue>> monitoredNodes;

    // stores monitored data and group by table name <tableName, <objectId, value [[float type only]]>>
    std::unordered_map<std::string,  std::unordered_map<int, float>> tableObjects;


private:

    // the endpoint for the client to connect to. Such as "opc.tcp://host:port".
    string endpointUrl;

    // username credentials
    string username;

    // password credentials
    string password;

    // One mutex per node
    std::unordered_map<std::string, std::mutex> nodeLocks;

};

#endif //OPCUACLIENTMANAGER_H
