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

        //sql_client_manager.insertBatchData(opcua_client_manager.tableObjects);
        std::this_thread::sleep_for(interval);
    }
}
void periodicRead(opcua::Client& client, NamespaceIndex nsIndex, int32_t nodeId, std::chrono::seconds interval) {
    while (keepRunning) {
        // Perform asynchronous read
        opcua::services::readValueAsync(
            client,
            opcua::NodeId(nsIndex, nodeId),
            [nsIndex, nodeId](opcua::Result<opcua::Variant>& result) {
                std::cout << "Read request for NodeId (ns=" << nsIndex << "; id=" << nodeId << ") completed.\n";
                std::cout << "Status code: " << result.code() << std::endl;

                if (result.hasValue()) {
                    try {
                        const auto& value = result.value();
                        std::cout << "  Value: " << (value.to<bool>() ? "true" : "false") << "\n";
                    } catch (const std::exception& e) {
                        std::cerr << "Error extracting value: " << e.what() << std::endl;
                    }
                } else {
                    std::cerr << "Read operation failed for NodeId (ns=" << nsIndex << "; id=" << nodeId << ")\n";
                }
            }
        );

        std::this_thread::sleep_for(interval); // Wait for the specified interval before next read
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

        const std::string databaseSchemeFile = "/home/felipevillazon/Xelips/dbSchema.JSON";
        //sql_client_manager.createDatabaseSchema(databaseSchemeFile);

        OPCUAClientManager opcua_client_manager(opcua_credentials.at(0), opcua_credentials.at(1), opcua_credentials.at(2));


        const std::string staticInformationFile = "/home/felipevillazon/test.JSON";
        file_manager.loadFile(staticInformationFile);
        auto mappedData = file_manager.mapNodeIdToObjectId();



        const std::chrono::seconds interval(2); // Read every 2 seconds

        // Event handlers
        opcua_client_manager.client.onConnected([] { std::cout << "Client connected" << std::endl; });

         opcua_client_manager.client.onSessionActivated([&] {
            std::cout << "Session activated" << std::endl;

  // Step 1: Create a Subscription
opcua::services::createSubscriptionAsync(
    opcua_client_manager.client,
    opcua::SubscriptionParameters{250},  // Example: 1000 ms publishing interval
    true,  // Enable publishing
    {},  // Optional status change callback
    [](opcua::IntegerId subId) {
        std::cout << "Subscription deleted: " << subId << std::endl;
    },
    [&](opcua::CreateSubscriptionResponse& response) {
        std::cout
            << "Subscription created:\n"
            << "- status code: " << response.responseHeader().serviceResult() << "\n"
            << "- subscription id: " << response.subscriptionId() << std::endl;

        // Step 2: Create a Monitored Item using the subscriptionId
        opcua::services::createMonitoredItemDataChangeAsync(
            opcua_client_manager.client,
            response.subscriptionId(),  // Use the subscriptionId from the response
            opcua::ReadValueId(opcua::NodeId(4, 2), opcua::AttributeId::Value),  // NodeId to monitor
            opcua::MonitoringMode::Reporting,  // Monitoring mode
            opcua::MonitoringParametersEx{
                .samplingInterval = 2000  // 2 seconds sampling interval
            },
            [](opcua::IntegerId subId, opcua::IntegerId monId, const opcua::DataValue& dv) {
                std::cout << "New value: " << dv.value().to<bool>()
                          << " (Timestamp: " << dv.sourceTimestamp().format("%a %b %d %H:%M:%S %Y") << ")\n";
            },
             {},// Delete callback – called when the monitored item is deleted
            [](opcua::MonitoredItemCreateResult& result) {
                // Completion callback – called when the monitored item is created
                std::cout << "MonitoredItem created:\n"
                          << "- Status code: " << result.statusCode() << "\n"
                          << "- Monitored item ID: " << result.monitoredItemId() << std::endl;
            }
        );
    }
);


        });

         opcua_client_manager.client.onSessionClosed([] { std::cout << "Session closed" << std::endl; });
         opcua_client_manager.client.onDisconnected([] { std::cout << "Client disconnected" << std::endl; });

        // Try reconnecting indefinitely
        while (keepRunning) {
            try {
                 opcua_client_manager.client.connect(opcua_credentials.at(0));
                 opcua_client_manager.client.run(); // Blocking call - will process all async tasks
            } catch (const opcua::BadStatus& e) {
                 opcua_client_manager.client.disconnect();
                std::cerr << "Error: " << e.what() << "\nRetrying in 3 seconds..." << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(3));
            }
        }

        //runDataProcessingLoop(sql_client_manager, opcua_client_manager, mappedData, 1);

        //if (!connectToOPCUA(opcua_client_manager)) return


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


