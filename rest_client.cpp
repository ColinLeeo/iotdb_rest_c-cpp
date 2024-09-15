#include "rest_client.h"

#include <curl/curl.h>
#include <json/json.h>

#include <sstream>

namespace rest_client {

/** ------ Tablet defination ------ */
void Tablet::createColumns() {
    for (size_t i = 0; i < schemas.size(); i++) {
        TSDataType dataType = schemas[i].second;
        switch (dataType) {
            case TSDataType::BOOLEAN:
                values[i] = new bool[maxRowNumber];
                break;
            case TSDataType::INT32:
                values[i] = new int[maxRowNumber];
                break;
            case TSDataType::INT64:
                values[i] = new int64_t[maxRowNumber];
                break;
            case TSDataType::FLOAT:
                values[i] = new float[maxRowNumber];
                break;
            case TSDataType::DOUBLE:
                values[i] = new double[maxRowNumber];
                break;
            case TSDataType::TEXT:
                values[i] = new std::string[maxRowNumber];
                break;
            default:
                std::cout << "createColumns() default" << std::endl;
        }
    }
}

void Tablet::deleteColumns() {
    for (size_t i = 0; i < schemas.size(); i++) {
        TSDataType dataType = schemas[i].second;
        switch (dataType) {
            case TSDataType::BOOLEAN: {
                bool* valueBuf = (bool*)(values[i]);
                delete[] valueBuf;
                break;
            }
            case TSDataType::INT32: {
                int* valueBuf = (int*)(values[i]);
                delete[] valueBuf;
                break;
            }
            case TSDataType::INT64: {
                int64_t* valueBuf = (int64_t*)(values[i]);
                delete[] valueBuf;
                break;
            }
            case TSDataType::FLOAT: {
                float* valueBuf = (float*)(values[i]);
                delete[] valueBuf;
                break;
            }
            case TSDataType::DOUBLE: {
                double* valueBuf = (double*)(values[i]);
                delete[] valueBuf;
                break;
            }
            case TSDataType::TEXT: {
                std::string* valueBuf = (std::string*)(values[i]);
                delete[] valueBuf;
                break;
            }
            default:
                std::cout << "deleteColumns() default" << std::endl;
        }
    }
}

void Tablet::addValue(size_t schemaId, size_t rowIndex, void* value) {
    if (schemaId >= schemas.size()) {
        char tmpStr[100];
        sprintf(tmpStr,
                "Tablet::addValue(), schemaId >= schemas.size(). schemaId=%ld, "
                "schemas.size()=%ld.",
                schemaId, schemas.size());
        throw std::out_of_range(tmpStr);
    }

    if (rowIndex >= rowSize) {
        char tmpStr[100];
        sprintf(tmpStr,
                "Tablet::addValue(), rowIndex >= rowSize. rowIndex=%ld, "
                "rowSize.size()=%ld.",
                rowIndex, rowSize);
        throw std::out_of_range(tmpStr);
    }

    TSDataType dataType = schemas[schemaId].second;
    switch (dataType) {
        case TSDataType::BOOLEAN: {
            bool* valueBuf = (bool*)(values[schemaId]);
            valueBuf[rowIndex] = *((bool*)value);
            break;
        }
        case TSDataType::INT32: {
            int* valueBuf = (int*)(values[schemaId]);
            valueBuf[rowIndex] = *((int*)value);
            break;
        }
        case TSDataType::INT64: {
            int64_t* valueBuf = (int64_t*)(values[schemaId]);
            valueBuf[rowIndex] = *((int64_t*)value);
            break;
        }
        case TSDataType::FLOAT: {
            float* valueBuf = (float*)(values[schemaId]);
            valueBuf[rowIndex] = *((float*)value);
            break;
        }
        case TSDataType::DOUBLE: {
            double* valueBuf = (double*)(values[schemaId]);
            valueBuf[rowIndex] = *((double*)value);
            break;
        }
        case TSDataType::TEXT: {
            std::string* valueBuf = (std::string*)(values[schemaId]);
            valueBuf[rowIndex] = *(std::string*)value;
            break;
        }
        default:
            std::cout << "addValue() default" << std::endl;
    }
}

Json::Value Tablet::toJson() {
    Json::Value value;
    value["deviceId"] = deviceId;
    value["is_aligned"] = isAligned;
    for (size_t i = 0; i < rowSize; i++) {
        value["timestamps"].append(timestamps[i]);
        for (int ts_ind = 0; ts_ind < values.size(); ts_ind++) {
            if (bitMaps[ts_ind].isMarked(i)) {
                switch (schemas[ts_ind].second) {
                    case TSDataType::BOOLEAN: {
                        bool* valueBuf = (bool*)(values[ts_ind]);
                        value["values"][ts_ind].append(valueBuf[i]);
                        break;
                    }
                    case TSDataType::INT32: {
                        int* valueBuf = (int*)(values[ts_ind]);
                        value["values"][ts_ind].append(valueBuf[i]);
                        break;
                    }
                    case TSDataType::INT64: {
                        int64_t* valueBuf = (int64_t*)(values[ts_ind]);
                        value["values"][ts_ind].append(valueBuf[i]);
                        break;
                    }
                    case TSDataType::FLOAT: {
                        float* valueBuf = (float*)(values[ts_ind]);
                        value["values"][ts_ind].append(valueBuf[i]);
                        break;
                    }
                    case TSDataType::DOUBLE: {
                        double* valueBuf = (double*)(values[ts_ind]);
                        value["values"][ts_ind].append(valueBuf[i]);
                        break;
                    }
                    case TSDataType::TEXT: {
                        std::string* valueBuf = (std::string*)(values[ts_ind]);
                        value["values"][ts_ind].append(valueBuf[i]);
                        break;
                    }
                    default:
                        std::cout << "toJson() default" << std::endl;
                }
            } else {
                value["values"][ts_ind].append(Json::Value::null);
            }
        }
    }
    for (size_t i = 0; i < schemas.size(); i++) {
        value["measurements"].append(schemas[i].first);
        value["data_types"].append(DatatypeToString(schemas[i].second));
    }
    return value;
}

void Tablet::reset() {
    rowSize = 0;
    for (size_t i = 0; i < schemas.size(); i++) {
        bitMaps[i].reset();
    }
}

size_t Tablet::getTimeBytesSize() { return rowSize * 8; }

size_t Tablet::getValueByteSize() {
    size_t valueOccupation = 0;
    for (size_t i = 0; i < schemas.size(); i++) {
        switch (schemas[i].second) {
            case TSDataType::BOOLEAN:
                valueOccupation += rowSize;
                break;
            case TSDataType::INT32:
                valueOccupation += rowSize * 4;
                break;
            case TSDataType::INT64:
                valueOccupation += rowSize * 8;
                break;
            case TSDataType::FLOAT:
                valueOccupation += rowSize * 4;
                break;
            case TSDataType::DOUBLE:
                valueOccupation += rowSize * 8;
                break;
            case TSDataType::TEXT: {
                valueOccupation += rowSize * 4;
                std::string* valueBuf = (std::string*)(values[i]);
                for (size_t j = 0; j < rowSize; j++) {
                    valueOccupation += valueBuf[j].size();
                }
                break;
            }
            default:
                std::cout << "getValueByteSize() default" << std::endl;
        }
    }
    return valueOccupation;
}

void Tablet::setAligned(bool isAligned) { this->isAligned = isAligned; }

/** ------ end tablet defination ------ */

// curl call back function
size_t WriteCallback(void* contents, size_t size, size_t nmemb, void* userp) {
    ((std::string*)userp)->append((char*)contents, size * nmemb);
    return size * nmemb;
}

bool RestClient::pingIoTDB() {
    CURL* curl;
    CURLcode res;
    std::string readBuffer;
    curl_global_init(CURL_GLOBAL_DEFAULT);
    curl = curl_easy_init();
    bool ret = false;

    if (curl) {
        curl_easy_setopt(curl, CURLOPT_URL, (url_base_ + "/ping").c_str());

        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);

        res = curl_easy_perform(curl);

        if (res != CURLE_OK) {
            std::cerr << "Failed to ping IoTDB: " << curl_easy_strerror(res)
                      << std::endl;
        } else {
            Json::Value json_resp;
            Json::CharReaderBuilder builder;
            std::string errs;
            std::istringstream s(readBuffer);
            if (Json::parseFromStream(builder, s, &json_resp, &errs)) {
                int code = json_resp["code"].asInt();
                if (code == 200) {
                    std::cout << "Ping successful" << std::endl;
                    ret = true;
                } else {
                    std::cerr << "Failed to ping IoTDB: "
                              << json_resp["message"].asString() << std::endl;
                }
            }
        }

        curl_easy_cleanup(curl);
        // curl_slist_free_all(headers);
    }
    curl_global_cleanup();
    return ret;
}

bool RestClient::validatePath(std::string path) {
    if (path.substr(0, root_path.size()) != root_path) {
        std::cout << "path should begin with root:" << path << std::endl;
        return false;
    }
    return true;
}

bool RestClient::createTimeseries(std::string path, TSDataType dataType,
                                  TSEncoding encoding,
                                  CompressionType compression) {
    if (!validatePath(path)) {
        return false;
    }
    int code;
    std::ostringstream oss;
    oss << "CREATE TIMESERIES" << path
        << " WITH DATATYPE=" << DatatypeToString(dataType)
        << ", ENCODING=" << EncodingToString(encoding)
        << ", COMPRESSOR=" << CompressionToString(compression);
    std::string errmesg;
    code = runNonQuery(oss.str(), errmesg);
    if (code != 200) {
        std::cout << "create timeseries failed :" << errmesg;
        return false;
    }
    return true;
}

bool RestClient::createMultiTimeseries(
    std::vector<std::string> paths, std::vector<TSDataType> dataTypes,
    std::vector<TSEncoding> encodings,
    std::vector<CompressionType> compressions) {
    for (int i = 0; i < paths.size(); i++) {
        if (!validatePath(paths[i])) {
            return false;
        }
    }
    int path_size = paths.size();

    if (path_size != dataTypes.size() || path_size != encodings.size() ||
        path_size != compressions.size()) {
        std::cout
            << "The number of paramters does not match the number of paths";
        return false;
    }
    int code;
    std::string errmesg;
    std::ostringstream oss;
    for (int i = 0; i < paths.size(); i++) {
        oss << "CREATE TIMESERIES" << paths[i]
            << " WITH DATATYPE=" << DatatypeToString(dataTypes[i])
            << ", ENCODING=" << EncodingToString(encodings[i])
            << ", COMPRESSOR=" << CompressionToString(compressions[i]) << ";";
    }
    code = runNonQuery(oss.str(), errmesg);
    if (code != 200) {
        std::cout << "create timeseries failed :" << errmesg;
        return false;
    }
    return true;
}
bool RestClient::createAlingedTimeseries(
    std::string device_path, std::vector<std::string> sensor_list,
    std::vector<TSDataType> dataTypes, std::vector<TSEncoding> encodings,
    std::vector<CompressionType> compressions) {
    if (!validatePath(device_path)) {
        return false;
    }
    int sensor_size = sensor_list.size();
    if (sensor_size != dataTypes.size() || sensor_size != encodings.size() ||
        sensor_size != compressions.size()) {
        std::cout
            << "The number of paramters does not match the number of paths";
        return false;
    }

    int code;
    std::ostringstream oss;
    oss << "CREATE ALIGNED TIMESERIES " << device_path << "(";
    for (int i = 0; i < sensor_size; i++) {
        oss << sensor_list[i] << " " << DatatypeToString(dataTypes[i]) << " "
            << "ENCODING=" << EncodingToString(encodings[i])
            << " COMPRESSOR=" << CompressionToString(compressions[i]) << ",";
    }
    oss << ")";
    std::string errmesg;
    code = runNonQuery(oss.str(), errmesg);
    if (code != 200) {
        std::cout << "create timeseries failed :" << errmesg;
        return false;
    }
    return true;
}

bool RestClient::createDataRegion(std::string path) {
    if (path.substr(0, root_path.size()) != root_path) {
        std::cout << "path should begin with root" << std::endl;
        return false;
    }

    int code;
    std::string errmesg;
    code = runNonQuery("create database " + path, errmesg);
    if (code != 200) {
        std::cout << " create database failed: " << errmesg;
        return false;
    }
    return true;
}

int RestClient::runNonQuery(std::string sql, std::string& errmesg) {
    CURL* curl;
    CURLcode res;
    std::string readBuffer;
    curl_global_init(CURL_GLOBAL_DEFAULT);
    curl = curl_easy_init();
    bool res = false;
    int code = -1;

    if (curl) {
        curl_easy_setopt(curl, CURLOPT_URL,
                         (url_base_ + "/rest/v2/nonQuery").c_str());
        struct curl_slist* headers = NULL;
        headers = curl_slist_append(headers, "Content-Type:application/json");
        headers = curl_slist_append(headers, user_auth_header_.c_str());

        std::string json_data = "{\"sql\":\"" + sql + "\"}";
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_data.c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);

        res = curl_easy_perform(curl);

        if (res != CURLE_OK) {
            std::cout << "connect to database error: "
                      << curl_easy_strerror(res) << std::endl;
        } else {
            Json::Value json_resp;
            Json::CharReaderBuilder builder;
            std::string errs;
            std::istringstream s(readBuffer);
            if (Json::parseFromStream(builder, s, &json_resp, &errs)) {
                code = json_resp["code"].asInt();
                errmesg = json_resp["message"].asString();
            } else {
                std::cout << "parse response failed" << std::endl;
            }
        }
        curl_easy_cleanup(curl);
    }
    curl_global_cleanup();
    return code;
}

bool RestClient::runQuery(std::string sql, Json::Value& value) {
    CURL* curl;
    CURLcode res;
    std::string readBuffer;
    curl_global_init(CURL_GLOBAL_DEFAULT);
    curl = curl_easy_init();
    bool res = false;
    int code = -1;
    if (curl) {
        curl_easy_setopt(curl, CURLOPT_URL,
                         (url_base_ + "/rest/v2/query").c_str());
        struct curl_slist* headers = NULL;
        headers = curl_slist_append(headers, "Content-Type:application/json");
        headers = curl_slist_append(headers, user_auth_header_.c_str());
        if (res != CURLE_OK) {
            std::cout << "connect to IoTDB error" << curl_easy_strerror(res)
                      << std::endl;
        } else {
            Json::CharReaderBuilder builder;
            std::string errs;
            std::istringstream s(readBuffer);
            if (!Json::parseFromStream(builder, s, &value, &errs)) {
                std::cout << "parse json response failed" << std::endl;
                curl_easy_cleanup(curl);
                return false;
            }
        }
        curl_easy_cleanup(curl);
    }
    curl_global_cleanup();
    return true;
}

Tablet RestClient::queryTimeseriesByTime(std::string device_path,
                                         std::string sensor_name,
                                         TSDataType data_type, uint64_t begin,
                                         uint64_t end) {
    std::ostringstream oss;
    oss << "select " << sensor_name << " from " << device_path
        << "where time >=" << begin << " and time <= " << end << std::endl;

    Json::Value value;
    if (!runQuery(oss.str(), value)) {
        return;
    }
    const Json::Value timestamps = value["timestamps"];
    int size = timestamps.size();
    // there only one sensor in the tablet
    const Json::Value values = value["values"];
    Tablet tablet(device_path, {{sensor_name, data_type}}, size);
    for (int i = 0; i < size; i++) {
        int64_t timestampValue = timestamps[i].asInt64();
        tablet.addValue(0, i, (void*)&timestampValue);
        if (!values[0][i].isNull()) {
            tablet.bitMaps[0].mark(i);
            continue;
        }
        switch (data_type) {
            case TSDataType::BOOLEAN: {
                bool value = values[0][i].asBool();
                tablet.addValue(0, i, (void*)&value);
                break;
            }
            case TSDataType::INT32: {
                int value = values[0][i].asInt();
                tablet.addValue(0, i, (void*)&value);
                break;
            }
            case TSDataType::INT64: {
                int64_t value = values[0][i].asInt64();
                tablet.addValue(0, i, (void*)&value);
                break;
            }
            case TSDataType::FLOAT: {
                float value = values[0][i].asFloat();
                tablet.addValue(0, i, (void*)&value);
                break;
            }
            case TSDataType::DOUBLE: {
                double value = values[0][i].asDouble();
                tablet.addValue(0, i, (void*)&value);
                break;
            }
            case TSDataType::TEXT: {
                std::string value = values[0][i].asString();
                tablet.addValue(0, i, (void*)&value);
                break;
            }
            default:
                std::cout << "queryTimeseriesByTime() default" << std::endl;
        }
    }

    return tablet;
}

template <typename T>
bool RestClient::insertRecord(std::string device_path, std::string measurement,
                              TSDataType data_type, uint64_t timestamp,
                              T value) {
    CURL* curl;
    CURLcode res;
    std::string readBuffer;
    curl_global_init(CURL_GLOBAL_DEFAULT);
    curl = curl_easy_init();
    bool res = false;
    int code = -1;
    if (curl) {
        curl_easy_setopt(curl, CURLOPT_URL,
                         (url_base_ + "/rest/v2/insertRecords").c_str());
        struct curl_slist* headers = NULL;
        headers = curl_slist_append(headers, "Content-Type:application/json");
        headers = curl_slist_append(headers, user_auth_header_.c_str());
        Json::Value json_data;
        json_data["is_aligned"] = false;
        json_data["device"].append(device_path);
        json_data["timestamp"].append(timestamp);
        Json::Value measurements;
        measurements.append(measurement);
        json_data["measurements_list"].append(measurements);
        Json::Value dataTypes;
        dataTypes.append(DatatypeToString(data_type));
        json_data["data_types"].append(dataTypes);
        Json::Value values;
        values.append(value);
        json_data["values_list"].append(values);
        Json::StreamWriterBuilder builder;
        std::string json_str = Json::writeString(builder, json_data);
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_str.c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);
        res = curl_easy_perform(curl);
        if (res != CURLE_OK) {
            std::cout << "connect to IoTDB error" << curl_easy_strerror(res)
                      << std::endl;
        } else {
            Json::Value json_resp;
            Json::CharReaderBuilder builder;
            std::string errs;
            std::istringstream s(readBuffer);
            if (!Json::parseFromStream(builder, s, &json_resp, &errs)) {
                std::cout << "parse json response failed" << std::endl;
                curl_easy_cleanup(curl);
                return false;
            } else {
                code = json_resp["code"].asInt();
                if (code != 200) {
                    std::cout << "insert record failed" << std::endl;
                    return false;
                }
                return true;
            }
        }
    }

        bool RestClient::insertTablet(Tablet tablet) {
            CURL* curl;
            CURLcode res;
            std::string readBuffer;
            curl_global_init(CURL_GLOBAL_DEFAULT);
            curl = curl_easy_init();
            bool res = false;
            int code = -1;
            if (curl) {
                curl_easy_setopt(curl, CURLOPT_URL,
                                 (url_base_ + "/rest/v2/insertTablet").c_str());
                struct curl_slist* headers = NULL;
                headers =
                    curl_slist_append(headers, "Content-Type:application/json");
                headers = curl_slist_append(headers, user_auth_header_.c_str());
                Json::StreamWriterBuilder builder;
                std::string json_data =
                    Json::writeString(builder, tablet.toJson());
                curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
                curl_easy_setopt(curl, CURLOPT_POST, 1L);
                curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_data.c_str());
                curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
                curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);
                res = curl_easy_perform(curl);
                if (res != CURLE_OK) {
                    std::cout << "connect to IoTDB error"
                              << curl_easy_strerror(res) << std::endl;
                } else {
                    Json::Value json_resp;
                    Json::CharReaderBuilder builder;
                    std::string errs;
                    std::istringstream s(readBuffer);
                    if (!Json::parseFromStream(builder, s, &json_resp, &errs)) {
                        std::cout << "parse json response failed" << std::endl;
                        curl_easy_cleanup(curl);
                        return false;
                    }
                    code = json_resp["code"].asInt();
                }
            }
            if (code != 200) {
                std::cout << "insert tablet failed" << std::endl;
                return false;
            }
            return true;
        }

    }  // namespace rest_client
