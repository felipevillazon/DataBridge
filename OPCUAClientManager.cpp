//z
// Created by felipevillazon on 12/19/24.
//

#include "OPCUAClientManager.h"
#include "Logger.h"

// constructor
OPCUAClientManager::OPCUAClientManager(const string& endpointUrl, const string& username, const string& password)
    : endpointUrl(endpointUrl), username(username), password(password) {}

// destructor
OPCUAClientManager::~OPCUAClientManager() {

    // The destructor of the class will disconnect the client from the OPC UA server if called

    ::Logger::getInstance().logInfo("OPCUAClientManager: Disconnecting from OPC UA server by using destructor of the class."); // log info

    if (client.isConnected()) {  // if client exist
        client.disconnect();    // disconnect from OPC UA server
    }
}

// connect to OPC UA Server
bool OPCUAClientManager::connect() {
    ::Logger::getInstance().logInfo("OPCUAClientManager::connect(): Connecting to OPC UA server...");

    if (client.isConnected()) {  // check if already connected
        std::cerr << "Client is already connected!" << std::endl;
        ::Logger::getInstance().logInfo("OPCUAClientManager::connect(): Client already connected.");  // log info
        return true;  // already connected, return success
    }

    // configure authentication **before** connecting
    /*if (!username.empty() && !password.empty()) {  // check if username and password were given
        ::Logger::getInstance().logInfo("OPCUAClientManager::connect(): Setting authentication credentials...");  // log info
        client.config().setUserIdentityToken(opcua::UserNameIdentityToken(username, password));  // configuring client by adding username and password credentials
    } else {  // if username and password were not given
        ::Logger::getInstance().logInfo("OPCUAClientManager::connect(): No authentication credentials provided, connecting anonymously..."); // log info
    }*/


    try {  // try to connect to OPC UA server

        client.connect(endpointUrl);  // connect to OPC UA server

    } catch (const opcua::BadStatus&) {  // catch error if connection failed

        cerr << "Failed to connect to OPC UA server: " << endl;
        ::Logger::getInstance().logError("OPCUAClientManager::connect(): Failed to connect to OPC UA server."); // log error
        return false;

    }

    std::cout << "Connected to OPC UA server at " << endpointUrl << std::endl;
    ::Logger::getInstance().logInfo("OPCUAClientManager::connect(): Successfully connected to OPC UA server."); // log info

    return true;
}


// disconnect from OPC UA Server
void OPCUAClientManager::disconnect() {

    // Disconnect deals with the disconnection from the OPC UA server

    ::Logger::getInstance().logInfo("OPCUAClientManager::disconnect(): Disconnecting from OPC UA server..."); // log info

    client.disconnect();  // disconnect from opc ua server

    // check if client is disconnected
    if (!client.isConnected()) {  // if disconnection sucessfully

        std::cout << "OPC UA client successfully disconnected." << std::endl; // return positive message
        ::Logger::getInstance().logInfo("OPCUAClientManager::disconnect(): Disconnection from OPC UA server successfully."); // log info

    } else {  // if disconnection failed

        ::Logger::getInstance().logError("OPCUAClientManager::disconnect(): Disconnection from OPC UA server failed."); // log error
        std::cerr << "OPC UA client disconnection failed!" << std::endl;  // return negative message
    }

}

// get data from OPC UA Server
bool OPCUAClientManager::getData() {return true;;}
