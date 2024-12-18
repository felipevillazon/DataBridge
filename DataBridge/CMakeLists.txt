cmake_minimum_required(VERSION 3.30)
project(DataBridge)

set(CMAKE_CXX_STANDARD 20)

# Find ODBC library
find_package(ODBC REQUIRED)
find_package(open62541 REQUIRED COMPONENTS Events FullNamespace)
find_package(nlohmann_json 3.2.0 REQUIRED)


# Add your source files
add_executable(DataBridge main.cpp
        SQLClientManager.cpp
        SQLClientManager.h
        OPCUAClientManager.cpp
        OPCUAClientManager.h
        DataProcessor.cpp
        DataProcessor.h
        ConfigManager.cpp
        ConfigManager.h
        Logger.cpp
        Logger.h)

# Link the ODBC library
target_link_libraries(DataBridge ODBC::ODBC)
target_link_libraries(DataBridge open62541::open62541)
target_link_libraries(DataBridge nlohmann_json::nlohmann_json)
