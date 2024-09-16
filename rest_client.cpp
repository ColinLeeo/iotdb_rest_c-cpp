#include "rest_client.h"

#include <sstream>

namespace rest_client {

/** ------ Tablet defination ------ */
void Tablet::createColumns() {
    for (size_t i = 0; i < schemas.size(); i++) {
        TSDataType dataType = schemas[i].second;
        switch (dataType) {
            case BOOLEAN:
                values[i] = new bool[maxRowNumber];
                break;
            case INT32:
                values[i] = new int[maxRowNumber];
                break;
            case INT64:
                values[i] = new int64_t[maxRowNumber];
                break;
            case FLOAT:
                values[i] = new float[maxRowNumber];
                break;
            case DOUBLE:
                values[i] = new double[maxRowNumber];
                break;
            case TEXT:
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
            case BOOLEAN: {
                bool* valueBuf = (bool*)(values[i]);
                delete[] valueBuf;
                break;
            }
            case INT32: {
                int* valueBuf = (int*)(values[i]);
                delete[] valueBuf;
                break;
            }
            case INT64: {
                int64_t* valueBuf = (int64_t*)(values[i]);
                delete[] valueBuf;
                break;
            }
            case FLOAT: {
                float* valueBuf = (float*)(values[i]);
                delete[] valueBuf;
                break;
            }
            case DOUBLE: {
                double* valueBuf = (double*)(values[i]);
                delete[] valueBuf;
                break;
            }
            case TEXT: {
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
        case BOOLEAN: {
            bool* valueBuf = (bool*)(values[schemaId]);
            valueBuf[rowIndex] = *((bool*)value);
            break;
        }
        case INT32: {
            int* valueBuf = (int*)(values[schemaId]);
            valueBuf[rowIndex] = *((int*)value);
            break;
        }
        case INT64: {
            int64_t* valueBuf = (int64_t*)(values[schemaId]);
            valueBuf[rowIndex] = *((int64_t*)value);
            break;
        }
        case FLOAT: {
            float* valueBuf = (float*)(values[schemaId]);
            valueBuf[rowIndex] = *((float*)value);
            break;
        }
        case DOUBLE: {
            double* valueBuf = (double*)(values[schemaId]);
            valueBuf[rowIndex] = *((double*)value);
            break;
        }
        case TEXT: {
            std::string* valueBuf = (std::string*)(values[schemaId]);
            valueBuf[rowIndex] = *(std::string*)value;
            break;
        }
        default:
            std::cout << "addValue() default" << std::endl;
    }
}

Json::Value Tablet::toJson() const {
    Json::Value value;
    value["device"] = deviceId;
    value["is_aligned"] = isAligned;
    for (size_t i = 0; i < rowSize; i++) {
        value["timestamps"].append(timestamps[i]);
        for (int ts_ind = 0; ts_ind < values.size(); ts_ind++) {
            if (bitMaps[ts_ind].isMarked(i)) {
                switch (schemas[ts_ind].second) {
                    case BOOLEAN: {
                        bool* valueBuf = (bool*)(values[ts_ind]);
                        value["values"][ts_ind].append(valueBuf[i]);
                        break;
                    }
                    case INT32: {
                        int* valueBuf = (int*)(values[ts_ind]);
                        value["values"][ts_ind].append(valueBuf[i]);
                        break;
                    }
                    case INT64: {
                        int64_t* valueBuf = (int64_t*)(values[ts_ind]);
                        value["values"][ts_ind].append(valueBuf[i]);
                        break;
                    }
                    case FLOAT: {
                        float* valueBuf = (float*)(values[ts_ind]);
                        value["values"][ts_ind].append(valueBuf[i]);
                        break;
                    }
                    case DOUBLE: {
                        double* valueBuf = (double*)(values[ts_ind]);
                        value["values"][ts_ind].append(valueBuf[i]);
                        break;
                    }
                    case TEXT: {
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
            case BOOLEAN:
                valueOccupation += rowSize;
                break;
            case INT32:
                valueOccupation += rowSize * 4;
                break;
            case INT64:
                valueOccupation += rowSize * 8;
                break;
            case FLOAT:
                valueOccupation += rowSize * 4;
                break;
            case DOUBLE:
                valueOccupation += rowSize * 8;
                break;
            case TEXT: {
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
    bool ret = false;
    Json::Value value;
    if (curl_connection_) {
        curl_perfrom("/ping", "", value, false, false);
        if (value["code"].asInt() == 200) {
            ret = true;
        }
    }
    return ret;
}

bool RestClient::validatePath(std::string path) {
    if (path.substr(0, root_path.size()) != root_path) {
        std::cout << "path should begin with root: " << path << std::endl;
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
    oss << "CREATE TIMESERIES " << path
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
    int path_size = paths.size();
    if (path_size != dataTypes.size() || path_size != encodings.size() ||
        path_size != compressions.size()) {
        std::cout
            << "The number of paramters does not match the number of paths";
        return false;
    }
    std::ostringstream oss;
    for (int i = 0; i < paths.size(); i++) {
        if (!createTimeseries(paths[i], dataTypes[i], encodings[i],
                              compressions[i])) {
            std::cout << "create timeseries failed" << paths[i] << std::endl;
            return false;
        }
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
            << " COMPRESSOR=" << CompressionToString(compressions[i])
            << (i == sensor_size - 1 ? "" : ",");
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

bool RestClient::createDatabase(std::string path) {
    if (!validatePath(path)) {
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

bool RestClient::curl_perfrom(std::string api, std::string data,
                              Json::Value& value, bool need_auth_info,
                              bool is_post) {
    CURLcode res;
    std::string readBuffer;
    if (curl_connection_) {
        curl_easy_reset(curl_connection_);
        curl_easy_setopt(curl_connection_, CURLOPT_URL,
                         (url_base_ + api).c_str());
        if (need_auth_info) {
            curl_easy_setopt(curl_connection_, CURLOPT_HTTPHEADER, headers_);
        }

        curl_easy_setopt(curl_connection_, CURLOPT_POST, is_post ? 1L : 0L);
        if (!data.empty()) {
            curl_easy_setopt(curl_connection_, CURLOPT_POSTFIELDS,
                             data.c_str());
        }
        curl_easy_setopt(curl_connection_, CURLOPT_WRITEFUNCTION,
                         WriteCallback);
        curl_easy_setopt(curl_connection_, CURLOPT_WRITEDATA, &readBuffer);
        // maintain connections to reduce connection latency.
        curl_easy_setopt(curl_connection_, CURLOPT_FORBID_REUSE, 0L);
        res = curl_easy_perform(curl_connection_);
        if (res != CURLE_OK) {
            std::cout << "failed to perform api" << api
                      << " error: " << curl_easy_strerror(res) << std::endl;
            return false;
        } else {
            Json::Value json_resp;
            Json::CharReaderBuilder builder;
            std::string errs;
            std::istringstream s(readBuffer);
            if (!Json::parseFromStream(builder, s, &json_resp, &errs)) {
                std::cout<< readBuffer << std::endl;
                std::cout << "parse json response failed: " << errs
                          << std::endl;
                return false;
            }
            value = json_resp;
            return true;
        }
    }
}

int RestClient::runNonQuery(std::string sql, std::string& errmesg) {
    std::string json_data = "{\"sql\":\"" + sql + "\"}";
    Json::Value value;
    if (!curl_perfrom("/rest/v2/nonQuery", json_data, value)) {
        std::cout << "curl_perfrom failed" << std::endl;
        return -1;
    }
    int code = value["code"].asInt();
    errmesg = value["message"].asString();
    return code;
}

bool RestClient::runQuery(std::string sql, Json::Value& value) {
    Json::Value json_data;
    json_data["sql"] = sql;
    Json::StreamWriterBuilder writer;
    std::string json_str = Json::writeString(writer, json_data);
    if (!curl_perfrom("/rest/v2/query", json_str, value)) {
        std::cout << "query perform failed" << std::endl;
        return false;
    }
    return true;
}

bool RestClient::queryTimeseriesByTime(std::string device_path,
                                       std::string sensor_name,
                                       TSDataType data_type, uint64_t begin,
                                       uint64_t end, Tablet& tablet) {
    std::ostringstream oss;
    oss << "select " << sensor_name << " from " << device_path
        << " where time >=" << begin << " and time <= " << end << std::endl;
    std::cout<<oss.str() << std::endl;
    Json::Value value;
    if (!runQuery(oss.str(), value)) {
        return false;
    }
    const Json::Value timestamps = value["timestamps"];
    int size = timestamps.size();
    // there only one sensor in the tablet
    const Json::Value values = value["values"];
    for (int i = 0; i < size; i++) {
        int64_t timestampValue = timestamps[i].asInt64();
        tablet.addValue(0, i, (void*)&timestampValue);
        if (!values[0][i].isNull()) {
            tablet.bitMaps[0].mark(i);
            continue;
        }
        switch (data_type) {
            case BOOLEAN: {
                bool value = values[0][i].asBool();
                tablet.addValue(0, i, (void*)&value);
                break;
            }
            case INT32: {
                int value = values[0][i].asInt();
                tablet.addValue(0, i, (void*)&value);
                break;
            }
            case INT64: {
                int64_t value = values[0][i].asInt64();
                tablet.addValue(0, i, (void*)&value);
                break;
            }
            case FLOAT: {
                float value = values[0][i].asFloat();
                tablet.addValue(0, i, (void*)&value);
                break;
            }
            case DOUBLE: {
                double value = values[0][i].asDouble();
                tablet.addValue(0, i, (void*)&value);
                break;
            }
            case TEXT: {
                std::string value = values[0][i].asString();
                tablet.addValue(0, i, (void*)&value);
                break;
            }
            default:
                std::cout << "queryTimeseriesByTime() default" << std::endl;
        }
    }

    return true;
}

bool RestClient::insertTablet(const Tablet& tablet) {
    CURLcode res;
    std::string readBuffer;
    int code = -1;
    if (curl_connection_) {
        Json::Value json_resp;
        Json::StreamWriterBuilder builder;
        std::string json_data = Json::writeString(builder, tablet.toJson());
        if (curl_perfrom("/rest/v2/insertTablet", json_data, json_resp)) {
            code = json_resp["code"].asInt();
            if (code != 200) {
                std::cout << "insert tablet failed" << std::endl;
                std::cout << "code is " << code << std::endl;
                std::cout << "message" << json_resp["message"].asString()
                          << std::endl;
                return false;
            }
            return true;
        }
    }
    return false;
}

}  // namespace rest_client
