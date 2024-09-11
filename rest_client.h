#ifndef REST_CLIENT_H
#define REST_CLIENT_H

#include <cstdlib>
#include <iostream>
#include <sstream>
#include <vector>

static const std::string base64_chars =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz"
    "0123456789+/";

static const std::string root_path = "root";
static const std::string create_timeseries_req =
    "create timeseries %s WITH DATATYPE=%s, ENCODING=%s, COMPRESSOR=%s";
inline std::string to_string(int value) {
    std::ostringstream os;
    os << value;
    return os.str();
}

inline std::string base64_encode(unsigned char const* bytes_to_encode,
                                 unsigned int in_len) {
    std::string ret;
    int i = 0;
    int j = 0;
    unsigned char char_array_3[3];
    unsigned char char_array_4[4];

    while (in_len--) {
        char_array_3[i++] = *(bytes_to_encode++);
        if (i == 3) {
            char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
            char_array_4[1] = ((char_array_3[0] & 0x03) << 4) +
                              ((char_array_3[1] & 0xf0) >> 4);
            char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) +
                              ((char_array_3[2] & 0xc0) >> 6);
            char_array_4[3] = char_array_3[2] & 0x3f;

            for (i = 0; (i < 4); i++) ret += base64_chars[char_array_4[i]];
            i = 0;
        }
    }

    if (i) {
        for (j = i; j < 3; j++) char_array_3[j] = '\0';

        char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
        char_array_4[1] =
            ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
        char_array_4[2] =
            ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
        char_array_4[3] = char_array_3[2] & 0x3f;

        for (j = 0; (j < i + 1); j++) ret += base64_chars[char_array_4[j]];

        while ((i++ < 3)) ret += '=';
    }

    return ret;
}

enum TSDataType {
#define TS_DATATYPE_ENUM(name) name,
#include "schema_enum.def"
#undef TS_DATATYPE_ENUM
};

enum TSEncoding {
#define TS_ENCODING_ENUM(name) name,
#include "schema_enum.def"
#undef TS_ENCODING_ENUM
};

enum CompressionType {
#define TS_COMPRESSION_ENUM(name) name,
#include "schema_enum.def"
#undef TS_COMPRESSION_ENUM
};

inline std::string DatatypeToString(TSDataType type) {
    switch (type) {
#define TS_DATATYPE_ENUM(name) \
    case name:                 \
        return #name;
#include "schema_enum.def"
#undef TS_DATATYPE_ENUM
    }
    return "UNKNOWN";
}

inline std::string EncodingToString(TSEncoding encoding) {
    switch (encoding) {
#define TS_ENCODING_ENUM(name) \
    case name:                 \
        return #name;
#include "schema_enum.def"
#undef TS_ENCODING_ENUM
    }
    return "UNKNOWN";
}

inline std::string CompressionToString(CompressionType compression) {
    switch (compression) {
#define TS_COMPRESSION_ENUM(name) \
    case name:                    \
        return #name;
#include "schema_enum.def"
#undef TS_COMPRESSION_ENUM
    }
    return "UNKNOWN";
}

class RestClient {
   public:
    RestClient(std::string ip, int port, std::string username,
               std::string password)
        : username_(username), password_(password) {
        if (port < 0 || port > 65535) {
            std::cerr << "Invalid port number" << std::endl;
            std::exit(1);
        }
        std::string credentials = username + ":" + password;
        std::string encoded_credentials = base64_encode(
            reinterpret_cast<const unsigned char*>(credentials.c_str()),
            credentials.length());
        user_auth_header_ = "Authorization: Basic " + encoded_credentials;
        url_base_ = "http://" + ip + ":" + to_string(port);
    }

    bool pingIoTDB();
    bool createTimeseries(std::string path, TSDataType dataType,
                          TSEncoding encoding, CompressionType compression);

    bool createAlingedTimeseries(std::string device_path,
                                 std::vector<std::string> sensor_list,
                                 std::vector<TSDataType> dataTypes,
                                 std::vector<TSEncoding> encodings,
                                 std::vector<CompressionType> compressions);

    bool createDataRegion(std::string path);

    bool createMultiTimeseries(std::vector<std::string> paths,
                               std::vector<TSDataType> dataTypes,
                               std::vector<TSEncoding> encodings,
                               std::vector<CompressionType> compressions);
    int runNonQuery(std::string sql, std::string& errmesg);
    Json::Value runQuery(std::string sql);

    void queryTimeseriesByTime(std::string path, uint64_t begin, uint64_t end);

    

   private:
    bool validatePath(std::string path);
    std::string username_;
    std::string password_;
    std::string user_auth_header_;
    std::string url_base_;
};

#endif  // REST_CLIENT_H