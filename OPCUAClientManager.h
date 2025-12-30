//
// Created by felipevillazon on 12/19/24.
//

//
// Created by felipevillazon on 12/19/24.
//

#ifndef OPCUACLIENTMANAGER_H
#define OPCUACLIENTMANAGER_H

#include <iostream>
#include <string>
#include <unordered_map>
#include <tuple>
#include <vector>
#include <mutex>
#include <atomic>
#include "FileManager.h"


#include <open62541pp/client.hpp>
#include <open62541pp/open62541pp.h>

#include "SQLClientManager.h"

using namespace std;
using namespace opcua;

class SQLClientManager;  // forward declaration

class OPCUAClientManager {
public:
    explicit OPCUAClientManager(const string& endpointUrl,
                                const string& username,
                                const string& password,
                                SQLClientManager& dbManager);
    ~OPCUAClientManager();

    bool connect();
    void disconnect();

    // Polling for normal readings (unchanged)
    void pollNodeValues(const std::unordered_map<std::string, std::tuple<int, std::string>>& nodeMap);
    void groupByTableName(const std::unordered_map<std::string, std::tuple<int, std::string, opcua::DataValue>>& monitoredNodes);

    Client client;

    std::unordered_map<std::string, std::tuple<int, std::string, opcua::DataValue>> monitoredNodes;
    std::unordered_map<std::string, std::unordered_map<int, float>> tableObjects;

    // --------------------------
    // Alarm subscription (NEW)
    // --------------------------

    // What type of alarm node changed?
    enum class AlarmField { Severity, Ack, ErrorCode, Value, SystemState };

    // Mapping from a NodeId to (object_id + what it represents)
    struct AlarmNodeRef {
        int object_id = -1;
        int system_id = -1;     // useful for inserting alarms
        AlarmField field = AlarmField::Severity;
    };

    // Cache per object_id to compare old vs new
    struct AlarmStateCache {
        int lastSeverity = 0;
        bool lastAck = false;

        int lastErrorCode = 0;
        float lastValue = 0.0f;
        int lastSystemState = -1;

        bool hasErrorCode = false;
        bool hasValue = false;
        bool hasSystemState = false;

        bool initialized = false;

        bool active = false;
        int eventId = -1;
    };


    // This struct will come from FileManager (Step 2)


    // Set subscription using the mapping (Step 2 will implement the .cpp changes)
    void setSubscription(const double& subscriptionInterval,
                       const double& samplingInterval,
                       const std::vector<FileManager::AlarmNodeMapping>& alarmMappings);


    // Callback entry point for subscription changes
    void onAlarmDataChange(const opcua::NodeId& node, const opcua::DataValue& dv);

    std::atomic<bool> sessionAlive{false};

private:
    SQLClientManager& sqlClientManager;

    string endpointUrl;
    string username;
    string password;

    // Lookup for callback routing: node.toString() -> AlarmNodeRef
    std::unordered_map<std::string, AlarmNodeRef> alarmNodeLookup;

    // Cache per object
    std::unordered_map<int, AlarmStateCache> alarmCache;

    // DB guard (your existing mutex)
    std::mutex sqlMutex;
    std::mutex alarmMutex;
    // One mutex per node
    std::unordered_map<std::string, std::mutex> nodeLocks;
};

#endif // OPCUACLIENTMANAGER_H
