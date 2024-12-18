//
// Created by felipe on 12/11/24.
//

#ifndef LOGGER_H
#define LOGGER_H



class Logger {

    public:
           Logger(); // constructor
           ~Logger(); // destructor

           void logInfo(); // log important general messages
           void logError(); // log error messages
           void logDebug();  // log debug messages

};



#endif //LOGGER_H
