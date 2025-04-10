//
// Created by felipevillazon on 12/19/24.
//

#include "OPCUAClientManager.h"
#include "Logger.h"
#include <open62541pp/node.hpp>
#include "Helper.h"
#include <mutex>

// constructor
OPCUAClientManager::OPCUAClientManager(const string& endpointUrl, const string& username, const string& password, SQLClientManager& dbManager)
    : endpointUrl(endpointUrl), username(username), password(password), sqlClientManager(dbManager) {
}


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


// poll values from node ids asynchronously  input: <object_node_id, object_id, table_name>
void OPCUAClientManager::pollNodeValues(const std::unordered_map<std::string, std::tuple<int, std::string>>& nodeMap) {


    ::LOG_INFO("OPCUAClientManager::pollNodeValues(): Starting polling values from nodes..."); // log info

    // loop over object_node_id while nodeInfo holds object_id and table_name
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

                        ::LOG_ERROR("OPCUAClientManager::pollNodeValues(): Error processing value for nodeId: " + nodeIdStr + ". object_id is: " + "objectID" + " - Exception: " + std::string(e.what())); // log error

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


// convert DataValue to float for object_values column in database
float extractFloatValue(const opcua::DataValue& dataValue) {

    // independently of the data type of the incoming object, we convert it to float to not have split data
    // by data type in different tables in the database. It should not cause any problems since INT are
    // represented as 1 -> 1.0, doubles/floats remains the same and bools true -> 1.0 and false 0.0
    // string data type is no send to the database so we do not have conflicts there.

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


// group data by table name to insert into DataBase: input object -> <object_node_id, <object_id, table_name, value>>
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


// create and set subscription of alarm events
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
        [](const opcua::IntegerId subId) {
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
                severityNode = std::get<0>(alarm);
                ackNode = std::get<1>(alarm);
                fixedNode = std::get<2>(alarm);

                //std::cout << "Severity Node: " << severityNode.toString() << std::endl;
                //std::cout << "Acknowledged Node: " << ackNode.toString() << std::endl;
                //std::cout << "Fixed Node: " << fixedNode.toString() << std::endl;

                // Create monitored items for each node in the nodesToMonitor vector
                for (std::vector<opcua::NodeId> nodesToMonitor = {severityNode, ackNode, fixedNode}; const auto& node : nodesToMonitor) {
                    opcua::services::createMonitoredItemDataChangeAsync(
                        client,
                        response.subscriptionId(),
                        opcua::ReadValueId(node, opcua::AttributeId::Value),
                        opcua::MonitoringMode::Reporting,
                        opcua::MonitoringParametersEx{.samplingInterval = samplingInterval, .queueSize = 10},
                        [this, node](opcua::IntegerId, opcua::IntegerId, const opcua::DataValue& dv) {

                            //std::cout << "Node " << node.toString() << " has changed its value." << std::endl;

                            //cout << "Identifier is:" << node.identifier<uint32_t>() +100 << endl;
                            // Optionally, store the new value or set a flag for processing
                            dataChangeCallback(node, dv);


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


// handle severity change
void OPCUAClientManager::handleSeverityChange(const opcua::NodeId& node, const int16_t newSeverity) {

    const auto it = activeAlarms.find(node); // try to find if alarm is activate by checking if severity node id is there

    if (newSeverity == 0) {      // check if value is equal to 0

        //cout << "SEVERITY value returned to 0, FIXED flag will take care of it." << endl;
    }

    if (it == activeAlarms.end()) {  // check if node is not in activeAlarms
        // new Alarm Detected

        //cout << "New alarm detected" << endl;
        const int eventId = Helper::generateEventId("/home/felipevillazon/Xelips/alarmEventID.txt"); // generate unique event ID
        std::cout << "[INFO] New alarm detected. Event ID: " << eventId << std::endl;
        prepareAlarmDataBaseData(node);
        // Poll additional values from the server
        //auto additionalData = pollAdditionalNodes(node);  // get alarm-related values from opcua server

        pollAlarmNodes(node);

        // Insert new alarm record in the database
        //insertIntoDatabase(eventId, "New Alarm", newSeverity, additionalData);   // insert into database in its corresponding table
        sqlClientManager.insertAlarm("alarms", alarmDataBaseValues, "SEVERITY");

        // store alarm details
        activeAlarms[node] = {eventId, newSeverity, false, false};  // record new alarm into activeAlarms
    } else {
        // existing alarm - update severity
        //std::cout << "[INFO] Updating alarm severity for event ID: " << it->second.eventId << std::endl;
        pollAlarmNodes(node);
        prepareAlarmDataBaseData(node);
        sqlClientManager.insertAlarm("alarms", alarmDataBaseValues, "SEVERITY");

        it->second.severity = newSeverity; // update severity from activeAlarm
    }
}


// handle acknowledged flag
void OPCUAClientManager::handleAckChange(const opcua::NodeId& node, const bool isAcknowledged) {


    // extract namespace index and identifier
    const int namespaceIndex = node.namespaceIndex();  // get nameSpaceIndex from acknowledge node id
    const uint32_t baseIdentifier = node.identifier<uint32_t>();  // get identifier from acknowledge node id

    constexpr int m = 6;  // identifier position of acknowledge node id
    const opcua::NodeId relatedSeverityNode(namespaceIndex, baseIdentifier - m);  // build severity node id from acknowledge node id

    const auto it = activeAlarms.find(relatedSeverityNode);  // check if node is inside activeAlarms (tt must be inside otherwise there is logic problem)
    if (it != activeAlarms.end() && isAcknowledged) {  // if node is indeed a key of activeAlarms and acknowledge value is true
        std::cout << "[INFO] Alarm acknowledged for event ID: " << it->second.eventId << std::endl;
        pollAlarmNodes(relatedSeverityNode);
        prepareAlarmDataBaseData(relatedSeverityNode);
        sqlClientManager.insertAlarm("alarms", alarmDataBaseValues, "ACKNOWLEDGED");
        //insertIntoDatabase(it->second.eventId, "Acknowledged", it->second.severity);  // set acknowledged timestamp
        it->second.acknowledged = true;   // turn to true flag from activeAlarm struct
    }
}


// handle fixed flag
void OPCUAClientManager::handleFixedChange(const opcua::NodeId& node, const bool isFixed) {

    // extract namespace index and identifier
    const int namespaceIndex = node.namespaceIndex();  // get nameSpaceIndex from acknowledge node id
    const uint32_t baseIdentifier = node.identifier<uint32_t>();  // get identifier from acknowledge node id

    constexpr int m = 6;  // identifier position of acknowledge node id
    const opcua::NodeId relatedSeverityNode(namespaceIndex, baseIdentifier - m);  // build severity node id from acknowledge node id

    const auto it = activeAlarms.find(relatedSeverityNode);  // check if node is inside activeAlarms
    if (it != activeAlarms.end() && isFixed) { // if node is indeed a key of activeAlarms
        std::cout << "[INFO] Alarm fixed for event ID: " << it->second.eventId << std::endl;
        pollAlarmNodes(relatedSeverityNode);
        prepareAlarmDataBaseData(relatedSeverityNode);
        //insertIntoDatabase(it->second.eventId, "Fixed", it->second.severity);  // set fixed timestamp
        sqlClientManager.insertAlarm("alarms", alarmDataBaseValues, "FIXED");
        it->second.fixed = true;  // turn to true flag from activeAlarm struct

        activeAlarms.erase(relatedSeverityNode);  // delete current alarm event from activeAlarms
        alarmValues.erase(relatedSeverityNode);   // delete values from current alarm event from alarmValues
        alarmDataBaseValues.erase(relatedSeverityNode);  // delete database values from current alarm event from alarmDataBaseValues
    }
}


// poll alarm-related values from OPC UA server
void OPCUAClientManager::pollAlarmNodes(const NodeId& node) {
    constexpr int numRelatedVariables = 5;  // Adjust based on OPC UA structure

    // Extract namespace index and identifier
    const int namespaceIndex = node.namespaceIndex();
    const uint32_t baseIdentifier = node.identifier<uint32_t>();

    // Map to store alarm values

    for (int i = 0; i <= numRelatedVariables; ++i) {
        opcua::NodeId relatedNode(namespaceIndex, baseIdentifier + i);
        std::string nodeIdStr = relatedNode.toString();

        opcua::services::readValueAsync(
            client, relatedNode,
            [this, relatedNode,i](opcua::Result<opcua::Variant>& result) { // Capture `alarmValues` by reference

                if (result.hasValue()) {
                    try {

                        const opcua::DataValue dataValue(result.value());
                        const opcua::Variant& variantValue = dataValue.value();

                        int16_t severity = 0, stateId = 0, subsystemId = 0, objectId = 0, errorCode = 0;
                        float value = 0.0f;

                        // assuming the variables are read sequentially
                        switch (i) { // Distribute values based on ID
                            case 0: severity = variantValue.to<int16_t>();
                            cout << "severity: " << severity << endl;
                            break;
                            case 1: stateId = variantValue.to<int16_t>();
                            cout << "stateId: " << stateId << endl;
                            break;
                            case 2: subsystemId = variantValue.to<int16_t>();
                            cout << "subsystemId: " << subsystemId << endl;
                            break;
                            case 3: objectId = variantValue.to<int16_t>();
                            cout << "objectId: " << objectId << endl;
                            break;
                            case 4:
                            value = extractFloatValue(dataValue);
                            cout << "value: " << value << endl;
                            break;
                            case 5: errorCode = variantValue.to<int16_t>();
                            cout << "errorCode: " << errorCode << endl;
                            break;
                            default: ;
                        }

                        // Store in the map (modify existing or insert)
                        if (alarmValues.contains(relatedNode)) {
                            auto& entry = alarmValues[relatedNode];
                            entry = std::make_tuple(severity, stateId, subsystemId, objectId, value, errorCode);
                        } else {
                            alarmValues[relatedNode] = std::make_tuple(severity, stateId, subsystemId, objectId, value, errorCode);
                        }

                    } catch (const std::exception& e) {
                        std::cerr << "[ERROR] OPCUAClientManager::pollNodeValues() - Exception: " << e.what() << std::endl;
                    }
                } else {
                    std::cerr << "[ERROR] No value found for node: " << relatedNode.toString() << std::endl;
                }
            }
        );
    }

}


// callback method for opc ua subscription of alarm info
void OPCUAClientManager::dataChangeCallback(const opcua::NodeId& node, const opcua::DataValue& dv) {

    if (!dv.hasValue()) {

        cout << "No data found for node: " << node.toString() << std::endl;
        return;
    }

    std::lock_guard<std::mutex> lock(sqlMutex);

    try {
        if (node == severityNode) {

            std::cout << "[INFO] Node: " << node.toString() << "is the severity node" << std::endl;
            handleSeverityChange(node,dv.value().to<int16_t>() );
        } else if (node == ackNode) {
            std::cout << "[INFO] Node: " << node.toString() << "is the ack node" << std::endl;
            handleAckChange(node, dv.value().data());
        } else if (node == fixedNode) {
            std::cout << "[INFO] Node: " << node.toString() << "is the fixed node" << std::endl;
            handleFixedChange(node, dv.value().data());
        }
    } catch (const std::exception& e) {
        std::cerr << "[ERROR] Exception extracting data: " << e.what() << std::endl;
    }
}


void OPCUAClientManager::prepareAlarmDataBaseData(const opcua::NodeId& node) {
    const auto itAlarmValues = alarmValues.find(node);
    const auto itActiveAlarm = activeAlarms.find(node);

    // Check if node exists in both maps
    if (itActiveAlarm != activeAlarms.end() && itAlarmValues != alarmValues.end()) {
        // Extract data from activeAlarms
        const AlarmEvent& alarmEvent = itActiveAlarm->second;
        int eventId = alarmEvent.eventId;

        // Extract data from alarmValues
        const auto& values = itAlarmValues->second;
        auto [severity, stateId, subsystemId, objectId, value, errorCode] = values;

        // Combine into new tuple format and insert into alarmDataBaseValues
        alarmDataBaseValues[node] = std::make_tuple(
            severity,    // from alarmValues
            eventId,    // from activeAlarms
            stateId,    // from alarmValues
            subsystemId, // from alarmValues
            objectId,    // from alarmValues
            value,      // from alarmValues
            errorCode   // from alarmValues
        );
    }
    // Optional: Handle cases where node exists in only one map
    else if (itActiveAlarm != activeAlarms.end()) {
        // Node only in activeAlarms - maybe log a warning?

        cout << "something wrong with node id in activeAlarms" << endl;
    }
    else if (itAlarmValues != alarmValues.end()) {
        // Node only in alarmValues - maybe log a warning?

        cout << "something wrong with node id in alarmValues" << endl;
    }
}