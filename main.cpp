#include <iostream>
#include "Logger.h"
#include "SQLClientManager.h"
#include "OPCUAClientManager.h"
#include "ConfigManager.h"
#include "DataProcessor.h"

// initialize application
void initialize() {}

// run application
void run(){};

// stop application
void shutdown(){};



// TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
int main() {


    ::Logger &Logger = ::Logger::getInstance("/home/felipevillazon/Xelips/application.log");

    // testing configManager
    ConfigManager config_manager("/home/felipevillazon/Xelips/credentials.JSON");  // create instance of ConfigManager
    config_manager.loadConfig();   // open credential file
    const vector<string> sql_credentials = config_manager.getSQLConnectionDetails();  // get vector with sql credentials (DO NOT DISPLAY!)
    const vector<string> opcua_credentials = config_manager.getOPCUAServerDetails();  // get vector with opc ua credentials (DO NOT DISPLAY!)

    // testing DataProcessor
    DataProcessor data_processor; // create instance of DataProcessor
    const string sql_string = data_processor.setSQLString(sql_credentials);  // create sql string (DO NOT DISPLAY!)

    // testing SQL connection
    SQLClientManager sql_client_manager(sql_string);  // create instance of SQLClientManager
    sql_client_manager.connect();   // try connection to sql database

    // testing OPC UA connection
    OPCUAClientManager opcua_client_manager(opcua_credentials.at(0), opcua_credentials.at(1), opcua_credentials.at(2)); // create instance of OPCUAClientManager and input OPC UA credentials
    opcua_client_manager.connect();

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