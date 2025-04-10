#include <iostream>
#include <thread>
#include <chrono>
#include <csignal>
#include <atomic>
#include "Logger.h"
#include "FileManager.h"
#include "SQLClientManager.h"
#include "OPCUAClientManager.h"
#include "Helper.h"

// Atomic flag to manage graceful shutdown
std::atomic<bool> keepRunning(true);

// Signal handler for graceful shutdown (when user presses Ctrl+C)
void signalHandler(const int signum) {
    std::cout << "\nReceived signal " << signum << ". Shutting down gracefully...\n";
    keepRunning = false;  // Set the flag to false to stop threads in the main loop
}

// Initialize logging system
void setupLogger() {
    const std::string logFile = "/home/felipevillazon/Xelips/application.log";  // Log file path
    ::Logger &logger = ::Logger::getInstance(logFile);  // Get logger instance
}

// Load credentials from a JSON file for both SQL and OPC UA connections
void loadCredentials(FileManager& fileManager, std::vector<std::string>& sql_credentials,
                     std::vector<std::string>& opcua_credentials_plc_1, std::vector<std::string>& opcua_credentials_plc_2) {
    const std::string credentialFile = "/home/felipevillazon/Xelips/credentials.JSON";  // File with credentials
    fileManager.loadFile(credentialFile);  // Load the file using FileManager
    sql_credentials = fileManager.getSQLConnectionDetails();  // Get SQL credentials from the loaded file
    opcua_credentials_plc_1 = fileManager.getOPCUAServerDetails("PLC_1");  // Get OPC UA details for PLC 1
    opcua_credentials_plc_2 = fileManager.getOPCUAServerDetails("PLC_2");  // Get OPC UA details for PLC 2

    // Ensure that credentials are correctly loaded
    if (opcua_credentials_plc_1.size() < 3) {
        throw std::runtime_error("Insufficient OPC UA credentials for PLC_1");
    }
    if (opcua_credentials_plc_2.size() < 3) {
        throw std::runtime_error("Insufficient OPC UA credentials for PLC_2");
    }
}

// Try to establish a connection to the SQL server with retry mechanism
bool connectToSQL(SQLClientManager& sql_client_manager) {
    // Attempt connection until successful or keepRunning is false
    while (!sql_client_manager.connect() && keepRunning) {
        std::cout << "SQL connection failed. Retrying in 1 second...\n";
        std::this_thread::sleep_for(std::chrono::seconds(1));  // Sleep before retry
    }
    return keepRunning;  // Return true if connection is successful, false if interrupted
}

// Try to establish a connection to the OPC UA server with retry mechanism
bool connectToOPCUA(OPCUAClientManager& opcua_client_manager) {
    // Attempt connection until successful or keepRunning is false
    while (!opcua_client_manager.connect() && keepRunning) {
        std::cout << "OPCUA connection failed. Retrying in 1 second...\n";
        std::this_thread::sleep_for(std::chrono::seconds(1));  // Sleep before retry
    }
    return keepRunning;  // Return true if connection is successful, false if interrupted
}

// The main data processing loop for OPC UA and SQL
void runDataProcessingLoop(OPCUAClientManager& opcua_client_manager, SQLClientManager& sql_client_manager,
                           const std::unordered_map<std::string, std::tuple<int, std::string>>& mappedData) {
    // Run this loop as long as keepRunning is true
    while (keepRunning) {
        auto startTime = std::chrono::high_resolution_clock::now();  // Track loop start time

        // Poll node values from OPC UA servers and update monitored nodes
        opcua_client_manager.pollNodeValues(mappedData);

        // Group the node values by their table name to prepare them for SQL insertion
        opcua_client_manager.groupByTableName(opcua_client_manager.monitoredNodes);

        // Prepare the SQL insert statements for the values obtained from OPC UA
        sql_client_manager.prepareInsertStatements(opcua_client_manager.tableObjects);

        // Insert the batched data into the SQL database
        sql_client_manager.insertBatchData(opcua_client_manager.tableObjects);

        auto endTime = std::chrono::high_resolution_clock::now();  // Track loop end time
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime);  // Get loop duration

        // Ensure no negative sleep time by casting 0 to long int
        int sleepTime = std::max(static_cast<long int>(0), 1000000 - duration.count());  // Adjust the types to match

        std::this_thread::sleep_for(std::chrono::microseconds(sleepTime));  // Sleep until the next cycle
    }
}

// Function to create and start an OPC UA client for a given PLC, and handle data processing in a separate thread
void startOPCUAClientThread(const std::vector<std::string>& opcua_credentials,
                            SQLClientManager& sql_client_manager, const string& nodeIdFile) {
    // Initialize OPC UA client manager with the provided credentials and SQL client manager
    OPCUAClientManager opcua_client_manager(opcua_credentials.at(0), opcua_credentials.at(1), opcua_credentials.at(2), sql_client_manager);

    // Define the static information file for PLC_1 (could be for both PLC_1 and PLC_2 with a different file)
    const std::string& staticInformationFile = nodeIdFile;
    FileManager file_manager;
    file_manager.loadFile(staticInformationFile);  // Load the static information for the nodes
    const auto mappedData = file_manager.mapNodeIdToObjectId();  // Map node IDs to object IDs for SQL insertion
    const auto alarmNodeIds = file_manager.getNodeIdListAlarm();

    // Set up the subscription to monitor specific nodes for alarms or events
    opcua_client_manager.client.onSessionActivated([&] {
        std::thread([&] {
            // Start a separate thread to run the data processing loop
            runDataProcessingLoop(opcua_client_manager, sql_client_manager, mappedData);
        }).detach();  // Detach the thread so it runs independently
    });

    // Set the subscription for monitoring alarm nodes
    opcua_client_manager.setSubscription(100, 100, alarmNodeIds);

    // Event handlers for OPC UA client session events (session closed or disconnected)
    opcua_client_manager.client.onSessionClosed([] {
        std::cout << "[INFO] Session closed." << std::endl;
    });
    opcua_client_manager.client.onDisconnected([] {
        std::cout << "[INFO] Client disconnected." << std::endl;
    });
}

// Main initialization function that sets up components and starts threads for processing
void initialize() {
    try {
        setupLogger();  // Initialize the logging system

        unsigned int n_threads = std::thread::hardware_concurrency();  // Get the number of hardware threads available
        std::cout << "Recommended threads: " << n_threads << std::endl;  // Print the number of threads

        FileManager file_manager;  // FileManager for loading configuration and credentials
        std::vector<std::string> sql_credentials, opcua_credentials_plc_1, opcua_credentials_plc_2;

        // Load credentials from a file
        loadCredentials(file_manager, sql_credentials, opcua_credentials_plc_1, opcua_credentials_plc_2);

        Helper helper;  // Helper class for constructing SQL connection string
        const std::string sql_string = helper.setSQLString(sql_credentials);  // Set SQL string for connection

        // Create SQL client managers for both PLCs (or data sources)
        SQLClientManager sql_client_manager_1(sql_string);
        SQLClientManager sql_client_manager_2(sql_string);

        // Attempt to connect to SQL databases for both clients
        if (!connectToSQL(sql_client_manager_1) || !connectToSQL(sql_client_manager_2)) {
            return;  // If either connection fails, exit early
        }

        std::cout << "Endpoint URL: " << opcua_credentials_plc_1.at(0) << std::endl;  // Log endpoint for OPC UA connection


        const string nodeIdFile_PLC_1 = "/home/felipe/nodeId.plc_1.JSON";
        const string nodeIdFile_PLC_2 = "/home/felipe/nodeId.plc_2.JSON";
        // Start threads for the two OPC UA clients, each handling a different PLC
        std::thread opcuaThread1([&] {
            startOPCUAClientThread(opcua_credentials_plc_1, sql_client_manager_1, nodeIdFile_PLC_1);
        });
        std::thread opcuaThread2([&] {
            startOPCUAClientThread(opcua_credentials_plc_2, sql_client_manager_2, nodeIdFile_PLC_2);
        });

        // Join threads to ensure the program doesn't exit prematurely
        opcuaThread1.join();
        opcuaThread2.join();

    } catch (const std::exception& e) {
        std::cerr << "[FATAL ERROR] Initialization failed: " << e.what() << std::endl;  // Handle any errors during initialization
        std::exit(EXIT_FAILURE);  // Exit the program with an error code
    }
}

// Main function that initializes everything and starts the program
int main() {
    signal(SIGINT, signalHandler);  // Register signal handler for graceful shutdown (Ctrl+C)
    initialize();  // Start the main initialization process
    return 0;  // Return success
}


