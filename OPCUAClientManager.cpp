//
// Created by felipevillazon on 12/19/24.
//

#include "OPCUAClientManager.h"
#include "Logger.h"
#include <open62541pp/node.hpp>
#include "Helper.h"
#include <mutex>

// constructor
OPCUAClientManager::OPCUAClientManager(const string& endpointUrl, const string& username, const string& password)
    : endpointUrl(endpointUrl), username(username), password(password) {}


// destructor
OPCUAClientManager::~OPCUAClientManager() {

    // The destructor of the class will disconnect the client from the OPC UA server if called

    ::LOG_INFO("OPCUAClientManager: Disconnecting from OPC UA server by using destructor of the class."); // log info

    if (client.isConnected()) {  // if client exist
        client.disconnect();    // disconnect from OPC UA server
    }

}


// connect to OPC UA Server
bool OPCUAClientManager::connect() {

    ::LOG_INFO("OPCUAClientManager::connect(): Connecting to OPC UA server...");  // log info

    if (client.isConnected()) {  // check if already connected
        std::cerr << "Client is already connected!" << std::endl; // (avoid printing and use logger)
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

        std::cout << "Connected to OPC UA server at " << endpointUrl << std::endl; // (avoid printing and use logger)
        //client.run();  // run server-client connection

        ::LOG_INFO("OPCUAClientManager::connect(): OPCUA client connected and running.");  // log info

    } catch (const opcua::BadStatus& e) {  // catch error if connection failed

         cerr << "Failed to connect to OPC UA server: " << endl;  // (avoid printing and use logger)
        ::LOG_ERROR("OPCUAClientManager::connect(): Failed to connect to OPC UA server. - Exception: " + std::string(e.what())); // log error
        return false;

    }

     // std::cout << "Connected to OPC UA server at " << endpointUrl << std::endl; // (avoid printing and use logger)
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

        //std::cout << "OPC UA client successfully disconnected." << std::endl; // return positive message
        ::LOG_INFO("OPCUAClientManager::disconnect(): Disconnection from OPC UA server successfully."); // log info

    } else {  // if disconnection failed

        ::LOG_ERROR("OPCUAClientManager::disconnect(): Disconnection from OPC UA server failed."); // log error
        //std::cerr << "OPC UA client disconnection failed!" << std::endl;  // return negative message
    }

}


// poll values from node ids asynchronously
void OPCUAClientManager::pollNodeValues(const std::unordered_map<std::string, std::tuple<int, std::string>>& nodeMap) {


    ::LOG_INFO("OPCUAClientManager::pollNodeValues(): Starting polling values from nodes..."); // log info

    for (const auto& [nodeIdStr, nodeInfo] : nodeMap) {   // loop over all mapped nodeIds

        ::LOG_INFO("OPCUAClientManager::pollNodeValues(): Retrieving nameSpaceIndex and Identifier from nodeID..."); // log info
        array<int,2> nodeInformation = Helper::getNodeIdInfo(nodeIdStr);  // extract numeric nameSpaceIndex and identifier from string "ns=x;i=y"

        ::LOG_DEBUG("OPCUAClientManager::pollNodeValues(): Creating opcua::NodeId object.");  // log debug
        opcua::NodeId nodeId(nodeInformation[0], nodeInformation[1]);   // create NodeId object nodeId(nameSpaceIndex, identifier)

        ::LOG_DEBUG("OPCUAClientManager::pollNodeValues(): Start opcua::services::readValueAsync.");  // log debug
        opcua::services::readValueAsync(     // read values from nodeIds asynchronously  (not network blocking)
            client,      // Client* client
            nodeId,         // NodeId nodeId(nameSpaceIndex, identifier)
            [nodeIdStr, this, nodeInfo](opcua::Result<opcua::Variant>& result) {      // callback method

                ::LOG_INFO("OPCUAClientManager::pollNodeValues(): Reading nodeId: " + nodeIdStr);       // log info

                if (result.hasValue()) {  // check if nodeId has value
                    try {  // try and catch for better handling

                        {
                            const opcua::DataValue dataValue(result.value());  // get DataValue
                            int objectID = std::get<0>(nodeInfo);      // get objectId from nodeId string
                            std::string tableName = std::get<1>(nodeInfo);   // get identifier from nodeId string
                            std::lock_guard<std::mutex> lock(nodeLocks[nodeIdStr]);    // mutex lock for nodeId
                            monitoredNodes[nodeIdStr] = std::make_tuple(objectID, tableName, dataValue);  // update monitoredNdes
                     }

                        ::LOG_INFO("OPCUAClientManager::pollNodeValues(): Store value from " + nodeIdStr + "successfully.");       // log info

                        //std::cout << "Stored value for " << nodeIdStr << ": " << dataValue.value().to<float>() << "\n";  // print not needed, slow down process

                    } catch (const std::exception& e) {   // catch any error

                        ::LOG_ERROR("OPCUAClientManager::pollNodeValues(): Error processing value for nodeId: " + nodeIdStr + " - Exception: " + std::string(e.what())); // log error

                        // cerr << "Error processing value for " << nodeIdStr << ": " << e.what() << endl; // print not needed, slow down process
                    }
                } else {

                    ::LOG_ERROR("OPCUAClientManager::pollNodeValues(): Failed to read: " + nodeIdStr);  // log error

                    // std::cerr << "Failed to read " << nodeIdStr << "\n";  // print not needed, slow down process (just for debugging)
                }
            }
        );
    }
}


// convert DataValue to float
float extractFloatValue(const opcua::DataValue& dataValue) {

    ::LOG_INFO("OPCUAClientManager::extractFloatValue(): Converting opcua::DataValue to float..."); // log info

    if (!dataValue.hasValue()) {
        ::LOG_ERROR("OPCUAClientManager::extractFloatValue(): Value not found, returning NaN."); // log error
        return std::numeric_limits<float>::quiet_NaN(); // return NaN on error;
        }

    if (const opcua::Variant& variant = dataValue.value(); variant.isType<int16_t>()) {  // check if value is INT16
        return static_cast<float>(variant.to<int>());    // convert INT16 to FLOAT
    } else if (variant.isType<double>()) {     // check if value is DOUBLE
        return static_cast<float>(variant.to<double>());  // convert DOUBLE to FLOAT
    } else if (variant.isType<float>()) {  // check if value is FLOAT
        return variant.to<float>();  // already a float
    } else if (variant.isType<bool>()) {  // check if value is BOOL
        return variant.to<bool>() ? 1.0f : 0.0f;  // convert BOOL to FLOAT (true → 1.0, false → 0.0)
    }

    ::LOG_ERROR("OPCUAClientManager::extractFloatValue(): Data type not found, returning NaN."); // log error
    return std::numeric_limits<float>::quiet_NaN(); // return NaN;
}


// group data by table name to insert into DataBase
void OPCUAClientManager::groupByTableName(const std::unordered_map<std::string, std::tuple<int, std::string, opcua::DataValue>>& monitoredNodes)
{
    ::LOG_INFO("OPCUAClientManager::groupByTableName(): Grouping data by table name..."); // log info

    for (const auto& [nodeId, data] : monitoredNodes) {   // loop over monitoredNodes entries <string(nodeId),<int(objectID, string(tableName), DataValue)>>

        int objectId = std::get<0>(data);    // extract objectId
        std::string tableName = std::get<1>(data);  // extract tableName
        opcua::DataValue dataValue = std::get<2>(data);  // extract DataValue

        const float value = extractFloatValue(dataValue); // convert DataValue to float

        tableObjects[tableName][objectId] = value; // overwrite value for objectId in the unordered_map
    }
}


void OPCUAClientManager::setSubscription(
    const double& subscriptionInterval,
    const double& samplingInterval,
    const std::vector<std::tuple<opcua::NodeId, opcua::NodeId, opcua::NodeId>>& alarmNodes
) {
    if (alarmNodes.empty()) {
        std::cerr << "[ERROR] alarmNodes is empty. No nodes to subscribe!" << std::endl;
        return;
    }

    // Create the subscription asynchronously
    opcua::services::createSubscriptionAsync(
        client,  // client reference
        opcua::SubscriptionParameters{subscriptionInterval},  // subscription parameters
        true,  // enable publishing
        {},
        [](opcua::IntegerId subId) {
            std::cout << "[INFO] Subscription deleted: " << subId << std::endl;
        },  // delete callback

        // Capture alarmNodes by value to ensure correct access inside the async callback
        [this, alarmNodes, subscriptionInterval, samplingInterval](opcua::CreateSubscriptionResponse& response) {
            if (!response.responseHeader().serviceResult().isGood()) {
                std::cerr << "[ERROR] Failed to create subscription. Status: " << response.responseHeader().serviceResult() << std::endl;
                return;
            }

            std::cout << "[INFO] Subscription created (ID: " << response.subscriptionId() << ")\n";

            // Loop over the alarmNodes captured by value
            for (const auto& alarm : alarmNodes) {
                const opcua::NodeId& severityNode = std::get<0>(alarm);
                const opcua::NodeId& ackNode = std::get<1>(alarm);
                const opcua::NodeId& fixedNode = std::get<2>(alarm);

                std::cout << "Severity Node: " << severityNode.toString() << std::endl;
                std::cout << "Acknowledged Node: " << ackNode.toString() << std::endl;
                std::cout << "Fixed Node: " << fixedNode.toString() << std::endl;

                std::vector<opcua::NodeId> nodesToMonitor = {severityNode, ackNode, fixedNode};

                // Create monitored items for each node in the nodesToMonitor vector
                for (const auto& node : nodesToMonitor) {
                    opcua::services::createMonitoredItemDataChangeAsync(
                        client,
                        response.subscriptionId(),
                        opcua::ReadValueId(node, opcua::AttributeId::Value),
                        opcua::MonitoringMode::Reporting,
                        opcua::MonitoringParametersEx{.samplingInterval = samplingInterval, .queueSize = 10},
                        [this, node](opcua::IntegerId, opcua::IntegerId, const opcua::DataValue& dv) {
                            std::cout << "Node " << node.toString() << " has changed its value." << std::endl;
                            // Optionally, store the new value or set a flag for processing
                        },
                        {},
                        [](opcua::MonitoredItemCreateResult& result) {
                            if (!result.statusCode().isGood()) {
                                std::cerr << "[ERROR] Failed to create monitored item. Status: "
                                          << result.statusCode() << std::endl;
                            } else {
                                std::cout << "[INFO] MonitoredItem created (ID: "
                                          << result.monitoredItemId() << ")\n";
                            }
                        }
                    );
                }
            }
        }
    );
}



