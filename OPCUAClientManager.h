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
    explicit OPCUAClientManager(const string& endpointUrl, const string& username = "", const string& password = "");  // constructor
    ~OPCUAClientManager(); // destructor

    bool connect();  // connect to OPC UA Server
    void disconnect();  // disconnect from OPC UA Server
    void getValueFromNodeId(const std::unordered_map<std::string, std::tuple<int, std::string>>& nodeIdMap); // get values from OPC UA Sever using nodeIds

    Client client;       // instance UA_Client to create client

    // Stores monitored node values: <nodeId, <objectId, tableName, DataValue>>
    std::unordered_map<std::string, std::tuple<int, std::string, opcua::DataValue>> monitoredNodes;

    const std::unordered_map<std::string, std::tuple<int, std::string, opcua::DataValue>>& getLatestValues() const {
        return monitoredNodes; }



private:
    string endpointUrl; // the endpoint for the client to connect to. Such as "opc.tcp://host:port".
    string username;    // username credentials
    string password;    // password credentials

    struct UpdateData {
        string nodeId{};
        opcua::DataValue value;
    };

    std::queue<UpdateData> updateQueue;
    std::mutex queueMutex;
    std::condition_variable queueCV;
    std::thread workerThread;
    bool stopWorker;

    void processUpdates();


};

#endif //OPCUACLIENTMANAGER_H
