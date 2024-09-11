#include "rest_client.h"

#include <curl/curl.h>
#include <json/json.h>

#include <sstream>

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

void queryTimeseriesByTime(std::string device_path, std::string sensor_name,
                           uint64_t begin, uint64_t end) {
std::ostringstream oss;
oss << "select " << sensor_name << "from " << device_path << "where time"




                           }
