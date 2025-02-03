//
// Created by felipevillazon on 12/19/24.
//

#include "Logger.h"

// constructor
Logger::Logger(const string& filename) {

    /* The constructor will input the file path and will try to open the file in append mode */

    logFile.open(filename, ios::app); // open in append mode
    if (!logFile) {    // if not able to open file
        throw runtime_error("Unable to open log file: " + filename);  // lunch error message
    }
}

// destructor
Logger::~Logger() {

    /* The destructor will check if file is open and if that is the case, will close the file */

    if (logFile.is_open()) {  // if file open
        logFile.close();      // close file
    }
}

// get current timestamp
string Logger::getCurrentTimestamp() {

    /* Get current time stamp of the running machine. Return time as a string */

    time_t now = time(nullptr);  // create time_t type variable and initialize with null pointer
    char buf[20];  // create a buffer with 20 characters max
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", localtime(&now));  // get current time and placed it in buffer
    return string(buf);    // return buffer
}

// write log entry
void Logger::writeLog(const string& level, const string& message) {

    /* write log entry in log file */

    lock_guard<mutex> lock(mtx); // ensure thread safety
    if (logFile.is_open()) {    // check if file is open
        logFile << "[" << getCurrentTimestamp() << "] [" << level << "] " << message << endl;  // entry format [timestamp] [log level] [message]
    } else {  // if file is not open
        cerr << "Logger error: Log file is not open!" << endl;  // print error
    }
}

// singleton instance accessor
Logger& Logger::getInstance(const string& filename) {
    static Logger instance(filename);
    return instance;
}

// log info
void Logger::logInfo(const string& message) {
    writeLog("INFO", message); // call helper writelog to insert in file
}

// log errors
void Logger::logError(const string& message) {
    writeLog("ERROR", message);  // call helper writelog to insert in file
}

// log debug info
void Logger::logDebug(const string& message) {
    writeLog("DEBUG", message);   // call helper writelog to insert in file
}
