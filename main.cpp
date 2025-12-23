#include <iostream>
#include <thread>
#include <chrono>
#include <csignal>
#include <atomic>
#include <stdexcept>
#include <memory>
#include <vector>
#include <unordered_map>
#include <tuple>
#include <mutex>
#include <algorithm>

#include "Logger.h"
#include "FileManager.h"
#include "SQLClientManager.h"
#include "OPCUAClientManager.h"
#include "Helper.h"

using MapType = std::unordered_map<std::string, std::tuple<int, std::string>>;

// Atomic flag to manage graceful shutdown
std::atomic<bool> keepRunning(true);

// Signal handler for graceful shutdown (Ctrl+C)
void signalHandler(const int signum) {
    keepRunning = false;
    std::cout << "\nReceived signal " << signum << ". Shutting down gracefully...\n";
    ::LOG_INFO("Received signal " + std::to_string(signum) + ". Shutting down gracefully...");
}

// Initialize logging system
void setupLogger() {
    const std::string logFile = "/home/felipevillazon/Xelips/application.log";
    ::Logger &logger = ::Logger::getInstance(logFile);
    (void)logger;
    ::LOG_INFO("Logger initialized.");
}

// Load credentials from a JSON file for both SQL and OPC UA connections
void loadCredentials(FileManager& fileManager,
                     std::vector<std::string>& sql_credentials,
                     std::vector<std::string>& opcua_credentials_plc_1,
                     std::vector<std::string>& opcua_credentials_plc_2,
                     std::vector<std::string>& opcua_credentials_plc_3,
                     std::vector<std::string>& opcua_credentials_plc_4)
{
    const std::string credentialFile = "/home/felipevillazon/Xelips/credentials.JSON";
    fileManager.loadFile(credentialFile);

    sql_credentials = fileManager.getSQLConnectionDetails();
    opcua_credentials_plc_1 = fileManager.getOPCUAServerDetails("PLC_1");
    opcua_credentials_plc_2 = fileManager.getOPCUAServerDetails("PLC_2");
    opcua_credentials_plc_3 = fileManager.getOPCUAServerDetails("PLC_3");
    opcua_credentials_plc_4 = fileManager.getOPCUAServerDetails("PLC_4");

    ::LOG_INFO("Credentials loaded from: " + credentialFile);

    auto check = [](const std::vector<std::string>& c, const std::string& name) {
        if (c.size() < 3) {
            ::LOG_ERROR("Insufficient OPC UA credentials for " + name);
            throw std::runtime_error("Insufficient OPC UA credentials for " + name);
        }
    };

    check(opcua_credentials_plc_1, "PLC_1");
    check(opcua_credentials_plc_2, "PLC_2");
    check(opcua_credentials_plc_3, "PLC_3");
    check(opcua_credentials_plc_4, "PLC_4");
}

// Try to establish a connection to the SQL server with retry mechanism
bool connectToSQL(SQLClientManager& sql_client_manager) {
    while (keepRunning) {
        if (sql_client_manager.connect()) {
            ::LOG_INFO("SQL connected.");
            return true;
        }
        ::LOG_INFO("SQL connection failed. Retrying in 2 seconds...");
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }
    return false;
}

// Try to establish a connection to the OPC UA server with retry mechanism
bool connectToOPCUA(OPCUAClientManager& opcua_client_manager) {
    while (keepRunning) {
        if (opcua_client_manager.connect()) {
            // If connect() already sets this, it's harmless to set again.
            opcua_client_manager.sessionAlive = true;
            ::LOG_INFO("OPC UA connected.");
            return true;
        }
        ::LOG_INFO("OPC UA connection failed. Retrying in 2 seconds...");
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }
    return false;
}

// The main data processing loop for OPC UA and SQL
void runDataProcessingLoop(OPCUAClientManager& opcua_client_manager,
                           SQLClientManager& sql_client_manager,
                           std::shared_ptr<MapType>& mappedDataPtr,
                           std::mutex& mappedMutex,
                           FileManager& file_manager,
                           const std::string& nodeIdFile)
{
    // Per-thread timer (NOT static)
    auto lastCheck = std::chrono::steady_clock::now();

    while (keepRunning && opcua_client_manager.client.isConnected() && opcua_client_manager.sessionAlive) {
        try {
            auto startTime = std::chrono::high_resolution_clock::now();

            // Hot reload check (every 2 seconds)
            auto now = std::chrono::steady_clock::now();
            if (now - lastCheck >= std::chrono::seconds(2)) {
                lastCheck = now;

                if (file_manager.reloadFileIfModified(nodeIdFile)) {
                    auto newMap = std::make_shared<MapType>(file_manager.mapNodeIdToObjectId());
                    {
                        std::lock_guard<std::mutex> lock(mappedMutex);
                        mappedDataPtr = std::move(newMap);
                    }
                    ::LOG_INFO("Reloaded nodeId map from: " + nodeIdFile);
                }
            }

            // Snapshot pointer (no map copy)
            std::shared_ptr<MapType> snapshot;
            {
                std::lock_guard<std::mutex> lock(mappedMutex);
                snapshot = mappedDataPtr;
            }

            // Poll using snapshot
            opcua_client_manager.pollNodeValues(*snapshot);
            opcua_client_manager.groupByTableName(opcua_client_manager.monitoredNodes);
            sql_client_manager.prepareInsertStatements(opcua_client_manager.tableObjects);
            sql_client_manager.insertBatchData(opcua_client_manager.tableObjects);

            auto endTime = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime);

            // 1s loop target
            long long sleepUs = std::max<long long>(0, 1000000LL - duration.count());
            std::this_thread::sleep_for(std::chrono::microseconds(sleepUs));
        }
        catch (const std::exception& e) {
            ::LOG_ERROR(std::string("runDataProcessingLoop(): exception: ") + e.what());
            opcua_client_manager.sessionAlive = false; // exit loop -> reconnect outside
        }
    }
}

// Function to create and start an OPC UA client for a given PLC
void startOPCUAClientThread(const std::vector<std::string>& opcua_credentials,
                            SQLClientManager& sql_client_manager,
                            const std::string& nodeIdFile)
{
    OPCUAClientManager opcua_client_manager(
        opcua_credentials.at(0),
        opcua_credentials.at(1),
        opcua_credentials.at(2),
        sql_client_manager
    );

    // Set callbacks ONCE
    opcua_client_manager.client.onSessionClosed([&] {
        ::LOG_INFO("[INFO] Session closed.");
        opcua_client_manager.sessionAlive = false;
    });
    opcua_client_manager.client.onDisconnected([&] {
        ::LOG_INFO("[INFO] Client disconnected.");
        opcua_client_manager.sessionAlive = false;
    });

    ::LOG_INFO("PLC thread started for endpoint: " + opcua_credentials.at(0));

    // Outer loop: reconnect forever unless shutdown
    while (keepRunning) {

        // Ensure SQL connected (per PLC)
        if (!connectToSQL(sql_client_manager)) return;

        // Ensure OPC UA connected (per PLC)
        if (!connectToOPCUA(opcua_client_manager)) return;

        // Load node registry file
        FileManager file_manager;
        file_manager.loadFile(nodeIdFile);
        ::LOG_INFO("Node ID file loaded from: " + nodeIdFile);

        auto mappedDataPtr = std::make_shared<MapType>(file_manager.mapNodeIdToObjectId());
        std::mutex mappedMutex;

        // Alarm subscription (still static until you later implement alarm hot-reload)
        const auto alarmNodeIds = file_manager.getNodeIdListAlarm();
        opcua_client_manager.setSubscription(100, 100, alarmNodeIds);
        ::LOG_INFO("Alarm node IDs loaded: " + std::to_string(alarmNodeIds.size()));

        // Run until disconnected
        runDataProcessingLoop(opcua_client_manager, sql_client_manager, mappedDataPtr, mappedMutex, file_manager, nodeIdFile);

        if (!keepRunning) break;

        ::LOG_INFO("PLC thread: connection lost, retrying in 2 seconds...");
        // Ensure we flag session dead; connect() will set it alive again.
        opcua_client_manager.sessionAlive = false;
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }
}

// Main initialization function
void initialize() {
    try {
        setupLogger();

        unsigned int n_threads = std::thread::hardware_concurrency();
        std::cout << "Recommended threads: " << n_threads << std::endl;

        FileManager file_manager;
        std::vector<std::string> sql_credentials, opcua_credentials_plc_1, opcua_credentials_plc_2,
                                 opcua_credentials_plc_3, opcua_credentials_plc_4;

        loadCredentials(file_manager, sql_credentials,
                        opcua_credentials_plc_1, opcua_credentials_plc_2,
                        opcua_credentials_plc_3, opcua_credentials_plc_4);

        Helper helper;
        const std::string sql_string = helper.setSQLString(sql_credentials);

        SQLClientManager sql_client_manager_1(sql_string);
        SQLClientManager sql_client_manager_2(sql_string);
        SQLClientManager sql_client_manager_3(sql_string);
        SQLClientManager sql_client_manager_4(sql_string);

        const std::string nodeIdFile_PLC_1 = "/home/felipe/nodeId.plc_1.JSON";
        const std::string nodeIdFile_PLC_2 = "/home/felipe/nodeId.plc_2.JSON";
        const std::string nodeIdFile_PLC_3 = "/home/felipe/nodeId.plc_3.JSON";
        const std::string nodeIdFile_PLC_4 = "/home/felipe/nodeId.plc_4.JSON";

        std::thread opcuaThread1([&] { startOPCUAClientThread(opcua_credentials_plc_1, sql_client_manager_1, nodeIdFile_PLC_1); });
        std::thread opcuaThread2([&] { startOPCUAClientThread(opcua_credentials_plc_2, sql_client_manager_2, nodeIdFile_PLC_2); });
        std::thread opcuaThread3([&] { startOPCUAClientThread(opcua_credentials_plc_3, sql_client_manager_3, nodeIdFile_PLC_3); });
        std::thread opcuaThread4([&] { startOPCUAClientThread(opcua_credentials_plc_4, sql_client_manager_4, nodeIdFile_PLC_4); });

        opcuaThread1.join();
        opcuaThread2.join();
        opcuaThread3.join();
        opcuaThread4.join();

    } catch (const std::exception& e) {
        std::cerr << "[FATAL ERROR] Initialization failed: " << e.what() << std::endl;
        ::LOG_ERROR("Initialization failed: " + std::string(e.what()));
        std::exit(EXIT_FAILURE);
    }
}

int main() {
    signal(SIGINT, signalHandler);
    initialize();
    return 0;
}
