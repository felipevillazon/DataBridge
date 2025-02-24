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

// Atomic flag for graceful shutdown
std::atomic<bool> keepRunning(true);

// Signal handler for clean shutdown
void signalHandler(int signum) {
    std::cout << "\nReceived signal " << signum << ". Shutting down gracefully...\n";
    keepRunning = false;
}

// Function to initialize logger
void setupLogger() {
    const std::string logFile = "/home/felipevillazon/Xelips/application.log";
    ::Logger &logger = ::Logger::getInstance(logFile);
}

// Function to load credentials
void loadCredentials(FileManager& fileManager, std::vector<std::string>& sql_credentials, std::vector<std::string>& opcua_credentials) {
    const std::string credentialFile = "/home/felipevillazon/Xelips/credentials.JSON";
    fileManager.loadFile(credentialFile);
    sql_credentials = fileManager.getSQLConnectionDetails();
    opcua_credentials = fileManager.getOPCUAServerDetails();

    if (opcua_credentials.size() < 3) {
        throw std::runtime_error("Insufficient OPC UA credentials.");
    }
}

// Function to connect to SQL server with retry mechanism
bool connectToSQL(SQLClientManager& sql_client_manager) {
    while (!sql_client_manager.connect() && keepRunning) {
        std::cout << "SQL connection failed. Retrying in 1 second...\n";
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    return keepRunning;
}

// Function to connect to OPC UA server with retry mechanism
bool connectToOPCUA(OPCUAClientManager& opcua_client_manager) {
    while (!opcua_client_manager.connect() && keepRunning) {
        std::cout << "OPCUA connection failed. Retrying in 1 second...\n";
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    return keepRunning;
}

// Function to process data in a loop
void runDataProcessingLoop(SQLClientManager& sql_client_manager, OPCUAClientManager& opcua_client_manager,
                           const std::unordered_map<std::string, std::tuple<int, std::string>>& mappedData, int intervalInSeconds) {
    const std::chrono::seconds interval(intervalInSeconds);

    while (keepRunning) {
        opcua_client_manager.getValueFromNodeId(mappedData);
        opcua_client_manager.groupByTableName(opcua_client_manager.monitoredNodes);
        sql_client_manager.insertBatchData(opcua_client_manager.tableObjects);
        std::this_thread::sleep_for(interval);
    }
}

// Main initialization function
void initialize() {
    try {
        setupLogger();

        FileManager file_manager;
        std::vector<std::string> sql_credentials, opcua_credentials;
        loadCredentials(file_manager, sql_credentials, opcua_credentials);

        Helper helper;
        const std::string sql_string = helper.setSQLString(sql_credentials);
        SQLClientManager sql_client_manager(sql_string);

        if (!connectToSQL(sql_client_manager)) return;

        OPCUAClientManager opcua_client_manager(opcua_credentials.at(0), opcua_credentials.at(1), opcua_credentials.at(2));
        if (!connectToOPCUA(opcua_client_manager)) return;

        const std::string databaseSchemeFile = "/home/felipevillazon/Xelips/dbSchema.JSON";
        sql_client_manager.createDatabaseSchema(databaseSchemeFile);

        const std::string staticInformationFile = "/home/felipevillazon/test.JSON";
        file_manager.loadFile(staticInformationFile);
        auto mappedData = file_manager.mapNodeIdToObjectId();

        runDataProcessingLoop(sql_client_manager, opcua_client_manager, mappedData, 1);

    } catch (const std::exception &e) {
        std::cerr << "Error during initialization: " << e.what() << std::endl;
        std::exit(EXIT_FAILURE);
    }
}

int main() {
    signal(SIGINT, signalHandler);  // Handle Ctrl+C for graceful shutdown
    initialize();
    return 0;
}
