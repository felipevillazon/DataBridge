#include <iostream>
#include "Logger.h"
#include "SQLClientManager.h"
#include "OPCUAClientManager.h"
#include "FileManager.h"
#include "Helper.h"

// initialize application
void initialize() {}

// run application
void run(){};

// stop application
void shutdown(){};

int main() {


    ::Logger &Logger = ::Logger::getInstance("/home/felipevillazon/Xelips/application.log");

    // testing configManager for credential file
    FileManager config_manager_credentials;  // create instance of ConfigManager
    config_manager_credentials.loadFile("/home/felipevillazon/Xelips/credentials.JSON");   // open credential file
    const vector<string> sql_credentials = config_manager_credentials.getSQLConnectionDetails();  // get vector with sql credentials (DO NOT DISPLAY!)
    const vector<string> opcua_credentials = config_manager_credentials.getOPCUAServerDetails();  // get vector with opc ua credentials (DO NOT DISPLAY!)

    // testing DataProcessor
    Helper data_processor; // create instance of DataProcessor
    const string sql_string = data_processor.setSQLString(sql_credentials);  // create sql string (DO NOT DISPLAY!)

    // testing SQL connection
    SQLClientManager sql_client_manager(sql_string);  // create instance of SQLClientManager
    sql_client_manager.connect();   // try connection to sql database


    // testing creation of database structure (tables) from JSON file
    //sql_client_manager.createDatabaseSchema("/home/felipevillazon/Xelips/dbSchema.JSON");  // create database schema


    // testing batch method for nodeIds
    config_manager_credentials.loadFile("/home/felipevillazon/test.JSON");
    unordered_map<string, int> nodeID = config_manager_credentials.mapNodeIdToObjectId();

    //for (const auto& [nodeId, paramId] : nodeID) {
    //    cout << "NodeId: " << nodeId << " -> ParameterId: " << paramId << endl;
    //}

    // testing OPC UA connection
    OPCUAClientManager opcua_client_manager(opcua_credentials.at(0), opcua_credentials.at(1), opcua_credentials.at(2)); // create instance of OPCUAClientManager and input OPC UA credentials
    opcua_client_manager.connect();


    // Map of nodes <nodeId, <objectId, tableName>>
    std::unordered_map<std::string, std::tuple<int, std::string>> nodeIdMap = {
        {"ns=4;i=3", {101, "temperature_table"}},
        {"ns=2;i=1001", {202, "pressure_table"}},
        {"ns=3;i=25", {303, "flow_table"}}
    };

    // Start monitoring nodes
    opcua_client_manager.getValueFromNodeId(nodeIdMap);

    // Periodically check values
    while (true) {
        const auto& updatedValues = opcua_client_manager.getLatestValues();  // No copy, just a reference

        for (const auto& [nodeId, info] : updatedValues) {
            int objectId;
            std::string tableName;
            opcua::DataValue value;
            std::tie(objectId, tableName, value) = info;

            std::cout << "Latest Value for Node " << nodeId << ": " << value.value().to<string>()
                      << " (Object ID: " << objectId << ", Table: " << tableName << ")\n";
        }

        std::this_thread::sleep_for(std::chrono::seconds(1));  // Wait for 1 second
    }

    // Disconnect when done
    opcua_client_manager.disconnect();




    return 0;

    /*// endless loop to automatically (try to) reconnect to server.
    while (true) {
        try {
            client.connect(endpointUrl);
            // Run the client's main loop to process callbacks and events.
            // This will block until client.stop() is called or an exception is thrown.
            client.run();
        } catch (const opcua::BadStatus&) {
            client.disconnect();
            std::cout << "Disconnected. Retry to connect in 3 seconds\n";
            std::this_thread::sleep_for(std::chrono::seconds(3));
        }
    }*/
}

// TIP See CLion help at <a
// href="https://www.jetbrains.com/help/clion/">jetbrains.com/help/clion/</a>.
//  Also, you can try interactive lessons for CLion by selecting
//  'Help | Learn IDE Features' from the main menu.