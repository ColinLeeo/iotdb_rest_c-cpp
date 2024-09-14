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

class BitMap {
   public:
    /** Initialize a BitMap with given size. */
    explicit BitMap(size_t size = 0) { resize(size); }

    /** change the size  */
    void resize(size_t size) {
        this->size = size;
        this->bits.resize((size >> 3) + 1);  // equal to "size/8 + 1"
        reset();
    }

    /** mark as 1 at the given bit position. */
    bool mark(size_t position) {
        if (position >= size) return false;

        bits[position >> 3] |= (char)1 << (position % 8);
        return true;
    }

    /** mark as 0 at the given bit position. */
    bool unmark(size_t position) {
        if (position >= size) return false;

        bits[position >> 3] &= ~((char)1 << (position % 8));
        return true;
    }

    /** mark as 1 at all positions. */
    void markAll() { std::fill(bits.begin(), bits.end(), (char)0XFF); }

    /** mark as 0 at all positions. */
    void reset() { std::fill(bits.begin(), bits.end(), (char)0); }

    /** returns the value of the bit with the specified index. */
    bool isMarked(size_t position) const {
        if (position >= size) return false;

        return (bits[position >> 3] & ((char)1 << (position % 8))) != 0;
    }

    /** whether all bits are zero, i.e., no Null value */
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

    /** whether all bits are one, i.e., all are Null */
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

class Tablet {
   private:
    static const int DEFAULT_ROW_SIZE = 1024;

    void createColumns();
    void deleteColumns();

   public:
    std::string deviceId;  // deviceId of this tablet
    std::vector<std::pair<std::string, TSDataType>>
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
           const std::vector<std::pair<std::string, TSDataType>> &timeseries)
        : Tablet(deviceId, timeseries, DEFAULT_ROW_SIZE) {}

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
           const std::vector<std::pair<std::string, TSDataType>> &schemas,
           size_t maxRowNumber, bool _isAligned = false)
        : deviceId(deviceId),
          schemas(schemas),
          maxRowNumber(maxRowNumber),
          isAligned(_isAligned) {
        // create timestamp column
        timestamps.resize(maxRowNumber);
        // create value columns
        values.resize(schemas.size());
        createColumns();
        // create bitMaps
        bitMaps.resize(schemas.size());
        for (size_t i = 0; i < schemas.size(); i++) {
            bitMaps[i].resize(maxRowNumber);
        }
        this->rowSize = 0;
    }

    ~Tablet() { deleteColumns(); }

    void addValue(size_t schemaId, size_t rowIndex, void *value);

    void reset();  // Reset Tablet to the default state - set the rowSize to 0

    size_t getTimeBytesSize();

    size_t getValueByteSize();  // total byte size that values occupies

    void setAligned(bool isAligned);
};

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
    bool runQuery(std::string sql, Json::Value &value);

    bool createMultiTimeseries(std::vector<std::string> paths,
                               std::vector<TSDataType> dataTypes,
                               std::vector<TSEncoding> encodings,
                               std::vector<CompressionType> compressions);
    int runNonQuery(std::string sql, std::string &errmesg);

    void queryTimeseriesByTime(std::string device_path, std::string ensor_name,
                               uint64_t begin, uint64_t end);

    void insertTimeseriesRecord();

   private:
    bool validatePath(std::string path);
    std::string username_;
    std::string password_;
    std::string user_auth_header_;
    std::string url_base_;
};

#endif  // REST_CLIENT_H