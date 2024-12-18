//
// Created by felipe on 12/11/24.
//

#include "OPCUAClientManager.h"
#include <open62541/client_config_default.h>

// constructor
OPCUAClientManager::OPCUAClientManager(const string& endpointUrl, const string& username, const string& password)
    : endpointUrl(endpointUrl), username(username), password(password), client(nullptr), config(nullptr) {}

// destructor
OPCUAClientManager::~OPCUAClientManager() {

    if (client) {  // if client exist
        UA_Client_disconnect(client);    // disconnect from OPC UA server
        UA_Client_delete(client);   // delete client
    }
    delete config;
}

// connect to OPC UA Server
bool OPCUAClientManager::connect() {

    if (client) {    // check if client already exist
        std::cerr << "Client is already connected!" << std::endl;   // return message if client exist
        return false;   // exit method
    }

    client = UA_Client_new();   // create new client
    if (!client) {              // check it client was successfully created
        std::cerr << "Failed to create UA_Client." << std::endl;   // return message if fail when creating client
        return false;    // exit method
    }

    config = UA_Client_getConfig(client);  // start client configuration
    UA_ClientConfig_setDefault(config);    // start with default configuration of the client

    if (!username.empty() && !password.empty()) {  // if password and username passed to contructor (must be passed!)
        UA_StatusCode ret = UA_ClientConfig_setAuthenticationUsername(config , username.c_str(), password.c_str());  // set username and password for client
        if (ret != UA_STATUSCODE_GOOD) {  // check if username and password set successfully
            std::cerr << "Failed to set username and password: " << UA_StatusCode_name(ret) << std::endl;  // message case failed to configure username and password
            return false;  // exit method
        }
    }

    // space for more configurations if needed, deepends on security levels required //

    UA_StatusCode statusCode = UA_Client_connect(client, endpointUrl.c_str());  // try to connect to server, username and password already inclused inside client
    if (statusCode != UA_STATUSCODE_GOOD) {   // check if connection is stablished
        std::cerr << "Failed to connect to " << endpointUrl << ": " << UA_StatusCode_name(statusCode) << std::endl; // message if failed connection to server
        UA_Client_delete(client);  // delete client
        client = nullptr;  // null pointer to client
        return false;  // exit method
    }

    std::cout << "Connected to OPC UA server at " << endpointUrl << std::endl; // message if connection was stablished with server
    return true;  // method return true
}



// disconnect from OPC UA Server
void OPCUAClientManager::disconnect() {

    if (client) {   // if client exist
        UA_Client_disconnect(client);  // disconnect from server
        UA_Client_delete(client);  // delete client
        client = nullptr;   // null pointer for client
        std::cout << "Disconnected from OPC UA server." << std::endl;  // message to notify disconection from server
    } else {
        std::cerr << "Client is not connected!" << std::endl;  // message if disconnection attempted but client was not connected
    }
}

// get data from OPC UA Server
bool OPCUAClientManager::getData() {;}
