//
// Created by felipe on 12/11/24.
//

#ifndef OPCUACLIENTMANAGER_H
#define OPCUACLIENTMANAGER_H

#include <open62541/client.h>
#include <open62541/client_config_default.h>
#include <iostream>
#include <string>

using namespace std;

class OPCUAClientManager {

    public:
           OPCUAClientManager(const string& endpointUrl, const string& username = "", const string& password = "");  // constructor
           ~OPCUAClientManager(); // destructor

           bool connect();  // connect to OPC UA Server
           void disconnect();  // disconnect from OPC UA Server
           bool getData();   // get data from OPC UA Sever

    private:
            string endpointUrl; // the endpoint for the client to connect to. Such as "opc.tcp://host:port".
            string username;    // username credentials
            string password;    // password credentials
            UA_Client* client;       // pointer UA_Client to create client
            UA_ClientConfig* config;  // pointer UA_ClientConfig to configure client

};



#endif //OPCUACLIENTMANAGER_H