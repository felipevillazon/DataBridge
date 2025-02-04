//z
// Created by felipevillazon on 12/19/24.
//

#include "OPCUAClientManager.h"
#include "Logger.h"
#include <open62541pp/node.hpp>

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


constexpr std::string_view toString(opcua::NodeClass nodeClass) {
    switch (nodeClass) {
        case opcua::NodeClass::Object:
            return "Object";
        case opcua::NodeClass::Variable:
            return "Variable";
        case opcua::NodeClass::Method:
            return "Method";
        case opcua::NodeClass::ObjectType:
            return "ObjectType";
        case opcua::NodeClass::VariableType:
            return "VariableType";
        case opcua::NodeClass::ReferenceType:
            return "ReferenceType";
        case opcua::NodeClass::DataType:
            return "DataType";
        case opcua::NodeClass::View:
            return "View";
        default:
            return "Unknown";
    }
}

void printNodeTree(opcua::Node<opcua::Client>& node, int indent) {  // NOLINT
    for (auto&& child : node.browseChildren()) {
        std::cout << std::setw(indent) << "- "
                  << child.readBrowseName().name()  // Node Name
                  << " (" << toString(child.readNodeClass()) << ") "  // Node Type
                  << "[NodeId: " << child.id().toString() << "]\n";  // NodeId
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

    /*opcua::Node nodeRoot(client, opcua::ObjectId::RootFolder);

    printNodeTree(nodeRoot, 0);

    auto nodeServer = nodeRoot.browseChild({{0, "Objects"}, {0, "Server"}});
    // Browse the parent node
    auto nodeServerParent = nodeServer.browseParent();

    std::cout << nodeServer.readDisplayName().text() << "'s parent node is "
              << nodeServerParent.readDisplayName().text() << "\n";*/

    try {
        Node nodeObjects(client, ObjectId::ObjectsFolder);
        Node nodeServerInterfaces = nodeObjects.browseChild({{3, "ServerInterfaces"}});
        Node nodeInterface = nodeServerInterfaces.browseChild({{4, "interface"}});

        std::cout << "Data inside 'interface':\n";
        for (auto&& dataNode : nodeInterface.browseChildren()) {
            std::cout << "- " << dataNode.readBrowseName().name()
                      << " (" << toString(dataNode.readNodeClass()) << ") "
                      << "[NodeId: " << dataNode.id().toString() << "]\n";

            // If it's a variable, try reading the value
            if (dataNode.readNodeClass() == opcua::NodeClass::Variable) {
                try {
                    auto value = dataNode.readValue();
                    std::cout << "  Data type: " << value.type()->typeName << "\n";
                    std::cout << "  Value: " << (value.to<bool>() ? "true" : "false") << "\n";
                    if (value.type()->typeName == "Boolean"){ cout << "Hola" << endl; }




                } catch (const std::exception& e) {
                    std::cout << "  (Failed to read value: " << e.what() << ")\n";
                }
            }
        }


    } catch (const opcua::BadStatus&) {
        cerr << "Failed to connect to OPC UA server: " << endl;
    }




    client.run();


    client.onSessionClosed([] {cout << "Session closed!" << endl;});
    client.onDisconnected([] {cout << "Disconnected from OPC UA server!" << endl;});



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
bool OPCUAClientManager::getData() {

    return true;;
}
