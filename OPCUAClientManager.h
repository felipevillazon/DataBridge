//
// Created by felipevillazon on 12/19/24.
//

#ifndef OPCUACLIENTMANAGER_H
#define OPCUACLIENTMANAGER_H


#include <iostream>
#include <string>
#include <open62541pp/client.hpp>
#include <open62541pp/open62541pp.h>


using namespace std;
using namespace opcua;

class OPCUAClientManager {

public:
    explicit OPCUAClientManager(const string& endpointUrl, const string& username = "", const string& password = "");  // constructor
    ~OPCUAClientManager(); // destructor

    bool connect();  // connect to OPC UA Server
    void disconnect();  // disconnect from OPC UA Server
    bool getData();   // get data from OPC UA Sever


private:
    string endpointUrl; // the endpoint for the client to connect to. Such as "opc.tcp://host:port".
    string username;    // username credentials
    string password;    // password credentials
    Client client;       // instance UA_Client to create client

    //UA_ClientConfig* config;  // pointer UA_ClientConfig to configure client

};



#endif //OPCUACLIENTMANAGER_H
