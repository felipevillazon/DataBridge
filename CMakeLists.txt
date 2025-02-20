cmake_minimum_required(VERSION 3.30)
project(DataBridge)

set(CMAKE_CXX_STANDARD 23)

# Find packages
find_package(ODBC REQUIRED)
#find_package(open62541 CONFIG REQUIRED)

find_package(open62541pp CONFIG REQUIRED)

add_executable(DataBridge main.cpp
        SQLClientManager.cpp
        SQLClientManager.h
        Logger.cpp
        Logger.h
        FileManager.cpp
        FileManager.h
        Helper.cpp
        Helper.h
        OPCUAClientManager.cpp
        OPCUAClientManager.h)

# Link libraries
target_link_libraries(DataBridge PRIVATE ODBC::ODBC)
#target_link_libraries(DataBridge PRIVATE open62541::open62541)
target_link_libraries(DataBridge PRIVATE open62541pp::open62541pp)

