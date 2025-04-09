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
void signalHandler(const int signum) {
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
                           const std::unordered_map<std::string, std::tuple<int, std::string>>& mappedData, int intervalInSeconds) {}



// Main initialization function
/*void initialize() {
    try {
        setupLogger();

        FileManager file_manager;
        std::vector<std::string> sql_credentials, opcua_credentials;
        loadCredentials(file_manager, sql_credentials, opcua_credentials);

        Helper helper;

        const std::string sql_string = helper.setSQLString(sql_credentials);
        SQLClientManager sql_client_manager(sql_string);

        cout << sql_string << endl;
        if (!connectToSQL(sql_client_manager)) return;


        //const std::string databaseSchemeFile = "/home/felipevillazon/Xelips/dbSchema.JSON";
        //sql_client_manager.createDatabaseSchema(databaseSchemeFile);

        cout << "endpointUrl is:  " << opcua_credentials.at(0) << endl;
        OPCUAClientManager opcua_client_manager(opcua_credentials.at(0), opcua_credentials.at(1), opcua_credentials.at(2));
        OPCUAClientManager opcua_client_manager_2("opc.tcp://192.168.1.200:4840", opcua_credentials.at(1), opcua_credentials.at(2));
        //opcua_client_manager.connect();
        //opcua_client_manager_2.connect();

        auto node_severity = NodeId(4,2);
        auto node_ack = NodeId(4,3);
        auto node_fixed = NodeId(4,4);




        // const std::string staticInformationFile = "/home/felipevillazon/test.JSON";
        // file_manager.loadFile(staticInformationFile);
        // auto mappedData = file_manager.mapNodeIdToObjectId();

        const std::chrono::seconds interval(2); // Read every 2 seconds

        std::atomic<bool> keepRunning(true);  // Control variable for stopping the loop
        opcua_client_manager.client.onSessionActivated([&] {
        std::thread([&] {
          // Launch a separate thread for periodic execution
          while (keepRunning) {
              // Start measuring time for execution in nanoseconds
              auto startTime = std::chrono::high_resolution_clock::now();

              // opcua server-client interaction
              //opcua_client_manager.pollNodeValues(mappedData);   // poll values from all mapped node ids
              //opcua_client_manager.groupByTableName(opcua_client_manager.monitoredNodes);  // group data and prepared it for sql database

              // sql server-client interaction
              //sql_client_manager.prepareInsertStatements(opcua_client_manager.tableObjects);   // prepare insert statements for sql database
              //sql_client_manager.insertBatchData(opcua_client_manager.tableObjects);    // insert batch data into tables, all at once

              // End measuring time
              auto endTime = std::chrono::high_resolution_clock::now();
              auto duration = std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime);  // Use microseconds for higher precision

              //std::cout << "Execution time for pollNodeValues: " << duration.count() << " µs\n";  // µs for microseconds

              // Calculate remaining time to sleep to maintain 1 second frequency
              const int executionTime = duration.count();  // Time taken by pollNodeValues in microseconds
              int sleepTime = std::max(0, 1000000 - executionTime);  // 1000000 µs = 1 second


              // Print how much time we sleep
              //std::cout << "Sleeping for: " << sleepTime / 1000 << " ms\n";  // Convert sleep time to milliseconds for clarity

              // Sleep for the remaining time to complete 1-second cycle
              std::this_thread::sleep_for(std::chrono::microseconds(sleepTime));
          }
      }).detach();  // Detach thread to run independently

            const std::vector<std::tuple<opcua::NodeId, opcua::NodeId, opcua::NodeId>> alarm = {
            std::make_tuple(opcua::NodeId(4,2), opcua::NodeId(4,3), opcua::NodeId(4,4))
            };
 opcua_client_manager.setSubscription(100, 100, alarm);
  });


         opcua_client_manager.client.onSessionClosed([] { std::cout << "Session plc 1 closed" << std::endl; });
         opcua_client_manager.client.onDisconnected([] { std::cout << "Client plc 1 disconnected" << std::endl; });

        opcua_client_manager_2.client.onSessionActivated([&] {

            const std::vector<std::tuple<opcua::NodeId, opcua::NodeId, opcua::NodeId>> alarm = {
          std::make_tuple(opcua::NodeId(4,2), opcua::NodeId(4,3), opcua::NodeId(4,4))
          };
opcua_client_manager_2.setSubscription(100, 100, alarm);
            opcua_client_manager_2.client.onSessionClosed([] { std::cout << "Session plc 2 closed" << std::endl; });
    opcua_client_manager_2.client.onDisconnected([] { std::cout << "Client plc 2 disconnected" << std::endl; });


        });


        // Try reconnecting indefinitely
        while (keepRunning) {
            try {
                 opcua_client_manager.client.connect(opcua_credentials.at(0));
                 opcua_client_manager_2.client.connect("opc.tcp://192.168.1.200:4840");
                 opcua_client_manager.client.run(); // Blocking call - will process all async tasks
                 opcua_client_manager_2.client.run();
            } catch (const opcua::BadStatus& e) {
                 opcua_client_manager.client.disconnect();
                opcua_client_manager_2.client.disconnect();
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
}*/

void initialize() {
    try {

        unsigned int n_threads = std::thread::hardware_concurrency();
        std::cout << "Recommended threads: " << n_threads << std::endl;

        setupLogger();

        FileManager file_manager;
        std::vector<std::string> sql_credentials, opcua_credentials;
        loadCredentials(file_manager, sql_credentials, opcua_credentials);

        Helper helper;
        const std::string sql_string = helper.setSQLString(sql_credentials);
        SQLClientManager sql_client_manager(sql_string);

        cout << sql_string << endl;

        if (!connectToSQL(sql_client_manager)) return;

        std::cout << "Endpoint URL: " << opcua_credentials.at(0) << std::endl;

        // Create OPC UA Clients for both servers
        OPCUAClientManager opcua_client_manager(opcua_credentials.at(0), opcua_credentials.at(1), opcua_credentials.at(2));
        OPCUAClientManager opcua_client_manager_2("opc.tcp://192.168.1.200:4840", opcua_credentials.at(1), opcua_credentials.at(2));

        std::atomic<bool> keepRunning(true);

        // Capture both clients for async processing
        opcua_client_manager.client.onSessionActivated([&] {
            std::cout << "[INFO] Session activated for PLC 1" << std::endl;

            std::thread([&] {
                while (keepRunning) {
                    auto startTime = std::chrono::high_resolution_clock::now();

                    // Processing logic here (polling, grouping, etc.)

                    auto endTime = std::chrono::high_resolution_clock::now();
                    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime);
                    //int sleepTime = std::max(0, 1000000 - duration.count());
                   // std::this_thread::sleep_for(std::chrono::microseconds(sleepTime));
                }
            }).detach();

            // Subscribe to alarms
            std::vector<std::tuple<opcua::NodeId, opcua::NodeId, opcua::NodeId>> alarm = {
                std::make_tuple(opcua::NodeId(4,5), opcua::NodeId(4,6), opcua::NodeId(4,7))
            };
            opcua_client_manager.setSubscription(100, 100, alarm);
        });

        opcua_client_manager_2.client.onSessionActivated([&] {
            std::cout << "[INFO] Session activated for PLC 2" << std::endl;

            std::vector<std::tuple<opcua::NodeId, opcua::NodeId, opcua::NodeId>> alarm = {
                std::make_tuple(opcua::NodeId(4,5), opcua::NodeId(4,6), opcua::NodeId(4,7))
            };
            opcua_client_manager_2.setSubscription(100, 100, alarm);
        });

        // Set up disconnect handlers
        opcua_client_manager.client.onSessionClosed([] { std::cout << "[INFO] Session PLC 1 closed" << std::endl; });
        opcua_client_manager.client.onDisconnected([] { std::cout << "[INFO] Client PLC 1 disconnected" << std::endl; });

        opcua_client_manager_2.client.onSessionClosed([] { std::cout << "[INFO] Session PLC 2 closed" << std::endl; });
        opcua_client_manager_2.client.onDisconnected([] { std::cout << "[INFO] Client PLC 2 disconnected" << std::endl; });

        // Run both sessions in separate threads to avoid blocking
        std::thread opcuaThread1([&] {
            while (keepRunning) {
                try {
                    opcua_client_manager.client.connect(opcua_credentials.at(0));
                    opcua_client_manager.client.run();
                } catch (const opcua::BadStatus& e) {
                    opcua_client_manager.client.disconnect();
                    std::cerr << "[ERROR] PLC 1: " << e.what() << "\nRetrying in 3 seconds..." << std::endl;
                    std::this_thread::sleep_for(std::chrono::seconds(3));
                }
            }
        });

        std::thread opcuaThread2([&] {
            while (keepRunning) {
                try {
                    opcua_client_manager_2.client.connect("opc.tcp://192.168.1.200:4840");
                    opcua_client_manager_2.client.run();
                } catch (const opcua::BadStatus& e) {
                    opcua_client_manager_2.client.disconnect();
                    std::cerr << "[ERROR] PLC 2: " << e.what() << "\nRetrying in 3 seconds..." << std::endl;
                    std::this_thread::sleep_for(std::chrono::seconds(3));
                }
            }
        });

        // Join threads to keep the program running
        opcuaThread1.join();
        opcuaThread2.join();

    } catch (const std::exception &e) {
        std::cerr << "[FATAL ERROR] Initialization failed: " << e.what() << std::endl;
        std::exit(EXIT_FAILURE);
    }
}


int main() {
    signal(SIGINT, signalHandler);  // Handle Ctrl+C for graceful shutdown
    initialize();
    return 0;
}


