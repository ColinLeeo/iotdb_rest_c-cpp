#include "rest_client.h"

#define FAILED_EXIST(ret, errmsg)             \
    do {                                      \
        if (!ret) {                           \
            std::cout << errmsg << std::endl; \
            return -1;                        \
        }                                     \
    } while (0)

int main() {
    int measurement_num = 5;
    int row_num = 100;
    rest_client::RestClient client("127.0.0.1", 18080, "root", "root");

    // check if IoTDB is running
    FAILED_EXIST(client.pingIoTDB(), "connect to IoTDB failed");
    std::cout<<"connect to IoTDB success"<<std::endl;

    FAILED_EXIST(client.createDatabase("root.sg1"), "create database fialed");
    std::cout<<"create database success"<<std::endl;

    FAILED_EXIST(
        client.createTimeseries("root.sg1.d1.s10", rest_client::INT32,
                                rest_client::PLAIN, rest_client::UNCOMPRESSED),
        "create timeseries failed");
    std::cout<<"create timeseries success"<<std::endl;

    std::vector<std::string> paths;
    std::vector<rest_client::TSDataType> dataTypes;
    std::vector<rest_client::TSEncoding> encodings;
    std::vector<rest_client::CompressionType> compressions;
    std::vector<std::string> measurement_names;
    for (int i = 0; i < measurement_num; i++) {
        paths.push_back("root.sg1.d1.s" + rest_client::to_string(i));
        dataTypes.push_back(rest_client::INT32);
        encodings.push_back(rest_client::RLE);
        compressions.push_back(rest_client::SNAPPY);
        measurement_names.push_back("s" + rest_client::to_string(i));
    }
    FAILED_EXIST(
        client.createMultiTimeseries(paths, dataTypes, encodings, compressions),
        "create multi timeseries failed");
    std::cout<<"create multi timeseries success"<<std::endl;

    FAILED_EXIST(
        client.createAlingedTimeseries("root.sg1.d2", measurement_names,
                                       dataTypes, encodings, compressions),
        "create aligned timeseries failed");
    std::cout<<"create aligned timeseries success"<<std::endl;

    std::vector<std::pair<std::string, rest_client::TSDataType> > schemas;
    schemas.push_back(std::make_pair("s0", rest_client::INT32));
    schemas.push_back(std::make_pair("s1", rest_client::INT32));
    schemas.push_back(std::make_pair("s2", rest_client::INT32));
    schemas.push_back(std::make_pair("s3", rest_client::INT32));
    schemas.push_back(std::make_pair("s4", rest_client::INT32));

   rest_client::Tablet tablet("root.sg1.d2", schemas);
    for (int64_t time = 0; time < row_num; time++) {
        int row = tablet.rowSize++;
        tablet.timestamps[row] = time;
        for (int i = 0; i < measurement_num; i++) {
            tablet.addValue(i, row, &i);
            tablet.bitMaps[i].mark(row);
        }
    }
    FAILED_EXIST(client.insertTablet(tablet), "insert tablet failed");
    std::cout<<"insert tablet success"<<std::endl;

    FAILED_EXIST(
        client.insertRecord<int32_t>("root.sg1.d1", "s1", rest_client::INT32, 1000, 1),
        "insert record failed");
    std::cout<<"insert record success"<<std::endl;

    std::vector<std::pair<std::string, rest_client::TSDataType> > result_schemas;
    result_schemas.push_back(std::make_pair("s1", rest_client::INT32));

    rest_client::Tablet result("root.sg1.d1", result_schemas);

    FAILED_EXIST(client.queryTimeseriesByTime(
                     "root.sg1.d1", "s1", rest_client::INT32, 0, 100, result),
                 "query timeseries by time failed");
    std::cout<<"query timeseries by time success"<<std::endl;

    std::cout << result.toJson();
    uint64_t timestamp;
    int32_t last_value;
    FAILED_EXIST(client.queryTimeseriesLatestValue<int32_t>("root.sg1.d1", "s1", timestamp, last_value),
                 "query timeseries latest value failed");
    std::cout<<"query timeseries latest value success"<<std::endl;
    std::cout << "timestamp: " << timestamp << ", value: " << last_value << std::endl;
}
