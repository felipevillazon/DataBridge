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

    ::LOG_INFO("OPCUAClientManager::getValueFromNodeId(): Starting getting values from nodeIds..."); // log info

    // callback to update monitoredNodes when data changes
    auto callback = [&](uint32_t subId, uint32_t monId, const opcua::DataValue& value) {
        std::lock_guard<std::mutex> lock(queueMutex);

        // iterate over the nodeIdMap to find the nodeId and push to the queue
        for (const auto& [nodeId, info] : nodeIdMap) {
            // push the nodeId and value to the queue
            updateQueue.push({nodeId, value});
        }

        // notify the worker thread to process the updates
        queueCV.notify_one();
    };

    ::LOG_INFO("OPCUAClientManager::getValueFromNodeId(): Checking if OPC UA session activated..."); // log info

    client.onSessionActivated([&] {

        ::LOG_INFO("OPCUAClientManager::getValueFromNodeId(): OPC UA session activated."); // log info
        std::cout << "Session Activated\n";

        // create subscription
        ::LOG_INFO("OPCUAClientManager::getValueFromNodeId(): Creating OPC UA subscriptions to node IDs"); // log info
        std::cout << "Creating subscription...\n";
        const SubscriptionParameters parameters{};
        const auto createSubscriptionResponse = createSubscription(client, parameters, true, {}, {});
        createSubscriptionResponse.responseHeader().serviceResult().throwIfBad();
        const auto subId = createSubscriptionResponse.subscriptionId();

        // Prepare nodes for monitoring
        std::vector<MonitoredItemCreateRequest> monitoredItems;
        for (const auto& [nodeId, info] : nodeIdMap) {
            std::array<int, 2> nodeInfo = Helper::getNodeIdInfo(nodeId);
            NamespaceIndex nameSpaceIndex = nodeInfo[0];
            uint32_t identifier = nodeInfo[1];

            MonitoredItemCreateRequest item({{nameSpaceIndex, identifier}, opcua::AttributeId::Value});
            monitoredItems.push_back(item);

            // store the mapping (efficient lookup)
            monitoredNodes[nodeId] = {get<0>(info), get<1>(info), opcua::DataValue()};
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

        // Wait for updates in the queue
        {
            std::unique_lock<std::mutex> lock(queueMutex);
            queueCV.wait_for(lock, std::chrono::milliseconds(500), [&] { return !updateQueue.empty(); });

            // Move all updates from the queue to the batch
            while (!updateQueue.empty()) {
                batchUpdates.push_back(updateQueue.front());
                updateQueue.pop();
            }
        }

        // Process the updates in the batch
        if (!batchUpdates.empty()) {
            for (const auto& update : batchUpdates) {
                // Find the nodeId in the monitoredNodes map and update the value
                if (monitoredNodes.find(update.nodeId) != monitoredNodes.end()) {
                    std::get<2>(monitoredNodes[update.nodeId]) = update.value;  // Update the value
                    std::cout << "Updated Node ID " << update.nodeId << " with new value.\n";
                }
            }
        }
    }
}

// convert DataValue to float
float extractFloatValue(const opcua::DataValue& dataValue) {

    ::LOG_INFO("OPCUAClientManager::extractFloatValue(): Converting to float data type..."); // log info

    if (!dataValue.hasValue()) {

        ::LOG_INFO("OPCUAClientManager::extractFloatValue(): Value not found."); // log info
        return 0.0f;  // Default to 0.0 if the value is not present
    }

    if (const opcua::Variant& variant = dataValue.value(); variant.isType<int>()) {
        return static_cast<float>(variant.to<int>());
    } else if (variant.isType<double>()) {
        return static_cast<float>(variant.to<double>());
    } else if (variant.isType<float>()) {
        return variant.to<float>();  // Already a float
    } else if (variant.isType<bool>()) {
        return variant.to<bool>() ? 1.0f : 0.0f;  // Convert bool to float (true → 1.0, false → 0.0)
    }

    return 0.0f;  // Default return if type is unsupported
}

// group data by table name to insert into DataBase
void OPCUAClientManager::groupByTableName(const std::unordered_map<std::string, std::tuple<int, std::string, opcua::DataValue>>& monitoredNodes)
{
    ::LOG_INFO("OPCUAClientManager::groupByTableName(): Grouping data by table name..."); // log info

    for (const auto& [nodeId, data] : monitoredNodes) {

        int objectId = std::get<0>(data);    // extract objectId

        std::string tableName = std::get<1>(data);  // extract tableName

        opcua::DataValue dataValue = std::get<2>(data);  // extract DataValue

        float value = extractFloatValue(dataValue); // convert DataValue to float

        tableObjects[tableName].emplace_back(objectId, value); // store (objectId, float value)
    }
}