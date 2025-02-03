//
// Created by felipevillazon on 12/19/24.
//

#ifndef LOGGER_H
#define LOGGER_H



#include <string>
#include <fstream>
#include <mutex>
#include <string>
#include <sstream>
#include <iostream>

using namespace std;

class Logger {

public:
    ~Logger(); // destructor

    static Logger& getInstance(const string& filename = "default.log");  // method that returns a reference to an instance of the class (Singleton)

    void logInfo(const string& message); // log important general messages
    void logError(const string& message); // log error messages
    void logDebug(const string& message);  // log debug messages

private:
    explicit Logger(const string& filename); // constructor private!

    ofstream logFile;  // file stream for the log file
    mutex mtx;         // mutex for thread safety

    string getCurrentTimestamp();  // helper method to get the current timestamp

    void writeLog(const string& level, const string& message); // helper method to write a log entry

    // private copy constructor and assignment operator (disable copy)
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;


};




#endif //LOGGER_H
