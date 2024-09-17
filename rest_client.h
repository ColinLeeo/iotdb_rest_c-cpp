#ifndef REST_CLIENT_H
#define REST_CLIENT_H

#include <curl/curl.h>
#include <json/json.h>

#include <cstdlib>
#include <iostream>
#include <sstream>
#include <vector>

#if defined(_MSC_VER) && (_MSC_VER <= 1500)
typedef __int64 int64_t;
typedef __int32 int32_t;
typedef unsigned __int64 uint64_t;
typedef unsigned __int32 uint32_t;
#endif


namespace rest_client {
enum errcode {
    OK = 0,
    OVERFLOW = 1,
    UNSUPPORT = 2,
};

/** ------- encoding base64 ------ */
static const std::string base64_chars =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz"
    "0123456789+/";

inline std::string base64_encode(unsigned char const *bytes_to_encode,
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

/** ------- end encoding base64 ------ */

/** ------- schema defination and tostring func ------ */
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

/** ------- end schema defination and tostring func ------ */

/** ------- Bit map in Tablet ------ */
// Used to indicate whether there are nulls in the result

class BitMap {
   public:
    explicit BitMap(size_t size = 0) { resize(size); }

    void resize(size_t size) {
        this->size = size;
        this->bits.resize((size >> 3) + 1);  // equal to "size/8 + 1"
        reset();
    }

    bool mark(size_t position) {
        if (position >= size) return false;

        bits[position >> 3] |= (char)1 << (position % 8);
        return true;
    }

    bool unmark(size_t position) {
        if (position >= size) return false;

        bits[position >> 3] &= ~((char)1 << (position % 8));
        return true;
    }

    void markAll() { std::fill(bits.begin(), bits.end(), (char)0XFF); }

    void reset() { std::fill(bits.begin(), bits.end(), (char)0); }

    bool isMarked(size_t position) const {
        if (position >= size) return false;

        return (bits[position >> 3] & ((char)1 << (position % 8))) != 0;
    }

    bool isAllUnmarked() const {
        size_t j;
        for (j = 0; j < size >> 3; j++) {
            if (bits[j] != (char)0) {
                return false;
            }
        }
        for (j = 0; j < size % 8; j++) {
            if ((bits[size >> 3] & ((char)1 << j)) != 0) {
                return false;
            }
        }
        return true;
    }

    bool isAllMarked() const {
        size_t j;
        for (j = 0; j < size >> 3; j++) {
            if (bits[j] != (char)0XFF) {
                return false;
            }
        }
        for (j = 0; j < size % 8; j++) {
            if ((bits[size >> 3] & ((char)1 << j)) == 0) {
                return false;
            }
        }
        return true;
    }

    const std::vector<char> &getByteArray() const { return this->bits; }

    size_t getSize() const { return this->size; }

   private:
    size_t size;
    std::vector<char> bits;
};

/** ------- end Bit map in Tablet ------ */

/** ------ tablet ------ */
class Tablet {
   private:
    static const int DEFAULT_ROW_SIZE = 1024;

    void createColumns();
    void deleteColumns();

   public:
    std::string deviceId;  // deviceId of this tablet
    std::vector<std::pair<std::string, TSDataType> >
        schemas;  // the list of measurement schemas for creating the tablet
    std::vector<int64_t> timestamps;  // timestamps in this tablet
    std::vector<void *> values;  // each object is a primitive type array, which
                                 // represents values of one measurement
    std::vector<BitMap> bitMaps;  // each bitmap represents the existence of
                                  // each value in the current column
    size_t rowSize;       // the number of rows to include in this tablet
    size_t maxRowNumber;  // the maximum number of rows for this tablet
    bool isAligned;  // whether this tablet store data of aligned timeseries or
                     // not

    Tablet() {}

    /**
     * Return a tablet with default specified row number. This is the standard
     * constructor (all Tablet should be the same size).
     *
     * @param deviceId   the name of the device specified to be written in
     * @param timeseries the list of measurement schemas for creating the tablet
     */
    Tablet(const std::string &deviceId,
           const std::vector<std::pair<std::string, TSDataType> > &timeseries)
        : deviceId(deviceId), schemas(timeseries) {
        maxRowNumber = DEFAULT_ROW_SIZE;
        init();
    }

    /**
     * Return a tablet with the specified number of rows (maxBatchSize). Only
     * call this constructor directly for testing purposes. Tablet should
     * normally always be default size.
     *
     * @param deviceId     the name of the device specified to be written in
     * @param schemas   the list of measurement schemas for creating the row
     *                     batch
     * @param maxRowNumber the maximum number of rows for this tablet
     */
    Tablet(const std::string &deviceId,
           const std::vector<std::pair<std::string, TSDataType> > &schemas,
           size_t maxRowNumber, bool _isAligned = false)
        : deviceId(deviceId),
          schemas(schemas),
          maxRowNumber(maxRowNumber),
          isAligned(_isAligned) {
        init();
    }

    void init() {
        timestamps.resize(maxRowNumber);
        values.resize(schemas.size());
        createColumns();
        bitMaps.resize(schemas.size());
        for (size_t i = 0; i < schemas.size(); i++) {
            bitMaps[i].resize(maxRowNumber);
        }
        this->rowSize = 0;
    }

    ~Tablet() { deleteColumns(); }

    bool addValue(size_t schemaId, size_t rowIndex, void *value);

    Json::Value toJson() const;

    void reset();  // Reset Tablet to the default state - set the rowSize to 0

    size_t getTimeBytesSize();

    size_t getValueByteSize();  // total byte size that values occupies

    void setAligned(bool isAligned);
};

/** ------ end tablet ------ */

/** ------ rest client ------ */
static const std::string root_path = "root";
static const std::string create_timeseries_req =
    "create timeseries %s WITH DATATYPE=%s, ENCODING=%s, COMPRESSOR=%s";
inline std::string to_string(int value) {
    std::ostringstream os;
    os << value;
    return os.str();
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
            reinterpret_cast<const unsigned char *>(credentials.c_str()),
            credentials.length());
        headers_ = NULL;
        headers_ =
            curl_slist_append(headers_, "Content-Type: application/json");
        headers_ = curl_slist_append(
            headers_, ("Authorization: Basic " + encoded_credentials).c_str());
        url_base_ = "http://" + ip + ":" + to_string(port);
        curl_global_init(CURL_GLOBAL_DEFAULT);
        curl_connection_ = curl_easy_init();
    }

    ~RestClient() {
        curl_slist_free_all(headers_);
        if (curl_connection_) {
            curl_easy_cleanup(curl_connection_);
        }
        curl_global_cleanup();
    }
    // check connection between client and IoTDB
    bool pingIoTDB();

    // create database
    bool createDatabase(std::string path);

    // create timeseries
    bool createTimeseries(std::string path, TSDataType dataType,
                          TSEncoding encoding, CompressionType compression);
    bool createAlingedTimeseries(std::string device_path,
                                 std::vector<std::string> measurement_list,
                                 std::vector<TSDataType> dataTypes,
                                 std::vector<TSEncoding> encodings,
                                 std::vector<CompressionType> compressions);
    bool createMultiTimeseries(std::vector<std::string> paths,
                               std::vector<TSDataType> dataTypes,
                               std::vector<TSEncoding> encodings,
                               std::vector<CompressionType> compressions);

    // insert data into timeseries/device
    template <typename T>
    bool insertRecord(std::string device_path, std::string measurement,
                      TSDataType data_type, uint64_t timestamp, T value) {
        CURLcode res;
        std::string readBuffer;
        if (curl_connection_) {
            Json::Value json_data;
            json_data["is_aligned"] = false;
            json_data["devices"].append(device_path);
            json_data["timestamps"].append(timestamp);
            Json::Value measurements;
            measurements.append(measurement);
            json_data["measurements_list"].append(measurements);
            Json::Value dataTypes;
            dataTypes.append(DatatypeToString(data_type));
            json_data["data_types_list"].append(dataTypes);
            Json::Value values;
            values.append(value);
            json_data["values_list"].append(values);
            Json::StreamWriterBuilder builder;
            std::string json_str = Json::writeString(builder, json_data);
            Json::Value json_resp;
            if (curl_perfrom("/rest/v2/insertRecords", json_str, json_resp)) {
                int code = json_resp["code"].asInt();
                if (code != 200) {
                    std::cout << "insert record failed" << std::endl;
                    std::cout << "code is " << code << std::endl;
                    std::cout << "message" << json_resp["message"].asString()
                              << std::endl;
                    return false;
                }
                return true;
            }
            return false;
        }
        return false;
    }
    bool insertTablet(const Tablet &tablet);

    // query data from timeseries
    bool queryTimeseriesByTime(std::string device_path,
                               std::string measurement_name,
                               TSDataType data_type, uint64_t begin,
                               uint64_t end, Tablet &tablet);
    template <typename T>
    bool queryTimeseriesLatestValue(std::string device_path,
                                    std::string measurement_name,
                                    uint64_t &timestamp, T &value) {
        std::ostringstream oss;
        oss << "select last " << measurement_name << " from " << device_path;
        Json::Value resp;
        if (!runQuery(oss.str(), resp)) {
            return false;
        }
        std::cout << resp.toStyledString() << std::endl;
        const Json::Value timestamps = resp["timestamps"];
        const Json::Value values = resp["values"];
        timestamp = timestamps[0].asUInt64();
        value = parseJsonValue<T>(values[1][0]);
        return true;
    }

    bool runQuery(std::string sql, Json::Value &value);
    int runNonQuery(std::string sql, std::string &errmesg);

   private:
    bool curl_perfrom(std::string api, std::string data, Json::Value &value,
                      bool need_auth_info = true, bool is_post = true);
    bool validatePath(std::string path);
    template <typename T>
    T parseJsonValue(const Json::Value &value);
    CURL *curl_connection_;
    std::string username_;
    std::string password_;
    struct curl_slist *headers_;
    std::string url_base_;
};

/** ------ end rest client ------ */

}  // namespace rest_client
#endif  // REST_CLIENT_H