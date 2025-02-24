#include <iostream>
#include "Logger.h"
#include "SQLClientManager.h"
#include "OPCUAClientManager.h"
#include "FileManager.h"
#include "Helper.h"

// initialize application
void initialize() {

    // we use initialize to start connection between server and clients. We also may use it to load files.
     try {
         // log into Log file
         const string logFile = "/home/felipevillazon/Xelips/application.log";  // path to Log File
         ::Logger &Logger = ::Logger::getInstance(logFile);  // Logger instance


         // manage login credentials
         FileManager file_manager;  // create instance of ConfigManager
         const string credentialFile = "/home/felipevillazon/Xelips/credentials.JSON";  // path to credential file
         file_manager.loadFile(credentialFile);   // open credential file
         const vector<string> sql_credentials = file_manager.getSQLConnectionDetails();  // get vector with sql credentials (DO NOT DISPLAY!)
         const vector<string> opcua_credentials = file_manager.getOPCUAServerDetails();  // get vector with opc ua credentials (DO NOT DISPLAY!)
         Helper helper; // create instance of Helper
         const string sql_string = helper.setSQLString(sql_credentials);  // create sql string for SQL connection (DO NOT DISPLAY!)


         // try connection to SQL server
         SQLClientManager sql_client_manager(sql_string);  // create instance of SQLClientManager
         while (!sql_client_manager.connect()) {  // if not connected to SQL server
             try {
                 sql_client_manager.connect();   // try connection to sql database
             } catch (const exception &e){
                 sql_client_manager.disconnect(); // disconnect from SQL server
                 std::cout << "Disconnected. Retry to connect in 1 seconds\n";  // try to connect again
                 std::this_thread::sleep_for(std::chrono::seconds(1)); // wait n seconds for retry connection
             }
         }


         // try connection to OPCUA server
         OPCUAClientManager opcua_client_manager(opcua_credentials.at(0), opcua_credentials.at(1), opcua_credentials.at(2)); // create instance of OPCUAClientManager and input OPC UA credentials
         while (!opcua_client_manager.connect()) {  // if not connected to OPCUA server
             try {
                 opcua_client_manager.connect();   // try connection to OPCUA server
                 opcua_client_manager.client.run();
             } catch (const BadStatus&){
                 opcua_client_manager.disconnect(); // disconnect from OPCUA server
                 std::cout << "Disconnected. Retry to connect in 1 seconds\n";  // try to connect again
                 std::this_thread::sleep_for(std::chrono::seconds(1)); // wait n seconds for retry connection
             }
         }


         // build up SQL database if not done yet
         const string databaseSchemeFile = "/home/felipevillazon/Xelips/dbSchema.JSON";  // path to SQL database scheme
         sql_client_manager.createDatabaseSchema(databaseSchemeFile);  // create SQL database scheme


         // space for inserting static data into static table in SQL database
         // missing insert of static data, needs to prepare files with official data
         //
         //
         //


         // map nodeID to objectID and table name
         const string staticInformationFile = "/home/felipevillazon/test.JSON";  // path to file containing static information
         file_manager.loadFile(staticInformationFile);  // load file
         unordered_map<std::string, std::tuple<int, std::string>> mappedData = file_manager.mapNodeIdToObjectId();

         constexpr int intervalInSeconds = 1;
         const std::chrono::seconds interval(intervalInSeconds);

         while (true) {


             // retrieve from OPC UA server
             opcua_client_manager.getValueFromNodeId(mappedData); // get DataValue from nodeIds


             // prepared monitored data from SQL insertion
             opcua_client_manager.groupByTableName(opcua_client_manager.monitoredNodes); // structure data in map object with table_name as key


             // execute SQL query
             sql_client_manager.insertBatchData(opcua_client_manager.tableObjects); // batch insert into SQL database


             // Wait for the next interval
             std::this_thread::sleep_for(interval);
         }

     } catch (const exception &e) {

         std::cerr << "Error during initialization: " << e.what() << std::endl;
         std::exit(EXIT_FAILURE);  // Terminate the program if initialization fails
     }
}


int main() {

    initialize();

    return 0;
}
