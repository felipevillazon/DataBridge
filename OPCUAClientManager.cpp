//z
// Created by felipevillazon on 12/19/24.
//

#include "OPCUAClientManager.h"
#include "Logger.h"
#include <open62541pp/node.hpp>
#include "Helper.h"

// constructor
OPCUAClientManager::OPCUAClientManager(const string& endpointUrl, const string& username, const string& password)
    : endpointUrl(endpointUrl), username(username), password(password), stopWorker(false) {

        workerThread = std::thread(&OPCUAClientManager::processUpdates, this); // start worker thread for processing updates
}


// destructor
OPCUAClientManager::~OPCUAClientManager() {

    // The destructor of the class will disconnect the client from the OPC UA server if called

    stopWorker = true;
    queueCV.notify_all();
    if (workerThread.joinable()) {
        workerThread.join();
    }

    ::LOG_INFO("OPCUAClientManager: Disconnecting from OPC UA server by using destructor of the class."); // log info

    if (client.isConnected()) {  // if client exist
        client.disconnect();    // disconnect from OPC UA server
    }

}


constexpr std::string_view toString(const opcua::NodeClass nodeClass) {
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
    ::LOG_INFO("OPCUAClientManager::connect(): Connecting to OPC UA server...");

    if (client.isConnected()) {  // check if already connected
        std::cerr << "Client is already connected!" << std::endl;
        ::LOG_INFO("OPCUAClientManager::connect(): Client already connected.");  // log info
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
        ::LOG_ERROR("OPCUAClientManager::connect(): Failed to connect to OPC UA server."); // log error
        return false;

    }

    std::cout << "Connected to OPC UA server at " << endpointUrl << std::endl;
    ::LOG_INFO("OPCUAClientManager::connect(): Successfully connected to OPC UA server."); // log info

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

    ::LOG_INFO("OPCUAClientManager::disconnect(): Disconnecting from OPC UA server..."); // log info

    client.disconnect();  // disconnect from opc ua server

    // check if client is disconnected
    if (!client.isConnected()) {  // if disconnection sucessfully

        std::cout << "OPC UA client successfully disconnected." << std::endl; // return positive message
        ::LOG_INFO("OPCUAClientManager::disconnect(): Disconnection from OPC UA server successfully."); // log info

    } else {  // if disconnection failed

        ::LOG_ERROR("OPCUAClientManager::disconnect(): Disconnection from OPC UA server failed."); // log error
        std::cerr << "OPC UA client disconnection failed!" << std::endl;  // return negative message
    }

}

// get values from OPC UA Server using node Ids
void OPCUAClientManager::getValueFromNodeId(const std::unordered_map<std::string, std::tuple<int, std::string>>& nodeIdMap) {


    // callback to update monitoredNodes when data changes
    //auto callback = [&](uint32_t subId, uint32_t monId, const opcua::DataValue& value) {
    //     for (const auto& [nodeId, info] : nodeIdMap) {
    //        if (monitoredNodes.find(nodeId) != monitoredNodes.end()) {
    //            std::get<2>(monitoredNodes[nodeId]) = value; // update DataValue
    //        }
    //    }
    //};

    // callback to update monitoredNodes when data changes
    auto callback = [&](uint32_t subId, uint32_t monId, const DataValue& value) {
        std::lock_guard<std::mutex> lock(queueMutex);
        updateQueue.push({monId, value});
        queueCV.notify_one(); // notify the worker thread
    };

    client.onSessionActivated([&] {

        cout << "Session Activated\n";


        // create subscription
        cout << "Creating subscription...\n";
        const SubscriptionParameters parameters{};
        const auto createSubscriptionResponse = createSubscription(
            client, parameters, true, {}, {}
        );

        // create subscription ID
        createSubscriptionResponse.responseHeader().serviceResult().throwIfBad();
        const auto subId = createSubscriptionResponse.subscriptionId();

        // prepare the list of nodes to monitor
        vector<MonitoredItemCreateRequest> monitoredItems;
        for (const auto& [nodeId, info] : nodeIdMap) {

            std::array<int, 2> nodeInfo = Helper::getNodeIdInfo(nodeId);  // get node ID information

            NamespaceIndex nameSpaceIndex = nodeInfo[0];   // get node nameSpaceIndex
            uint32_t identifier = nodeInfo[1];    // get node identifier

            MonitoredItemCreateRequest item({{nameSpaceIndex, identifier}, opcua::AttributeId::Value});
            monitoredItems.push_back(item);
        }

        // send subscription request
        const CreateMonitoredItemsRequest request({}, subId, TimestampsToReturn::Both, monitoredItems);
        const auto createMonitoredItemResponse = services::createMonitoredItemsDataChange(client, request, callback, {});

        createMonitoredItemResponse.responseHeader().serviceResult().throwIfBad();
    });
}

// **worker thread processes updates in batches**
void OPCUAClientManager::processUpdates() {
    while (!stopWorker) {
        std::vector<UpdateData> batchUpdates;

        {
            std::unique_lock<std::mutex> lock(queueMutex);
            queueCV.wait_for(lock, std::chrono::milliseconds(500), [&] { return !updateQueue.empty(); });

            while (!updateQueue.empty()) {
                batchUpdates.push_back(updateQueue.front());
                updateQueue.pop();
            }
        }

        if (!batchUpdates.empty()) {
            for (const auto& update : batchUpdates) {
                if (monitoredNodes.find(update.nodeId) != monitoredNodes.end()) {
                    std::get<2>(monitoredNodes[update.nodeId]) = update.value;
                    std::cout << "Updated Node ID " << update.nodeId << " with new value.\n";
                }
            }
        }
    }
}