//
// Created by felipe on 12/11/24.
//

#ifndef DATAPROCESSOR_H
#define DATAPROCESSOR_H

#include <string>
#include <vector>
#include <iostream>

using namespace std;

class DataProcessor {

    public:
           DataProcessor(); // constructor
           ~DataProcessor(); // destructor

           void processData();  // process data from OPC UA Server and get format for SQL database
           string setSQLString(vector<string>& credentialsSQL); // set sql string from input credentials placed in a vector

    private:

            string sqlString;   // sql string

};



#endif //DATAPROCESSOR_H
