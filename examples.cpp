#include "rest_client.h"

int main() {
    RestClient client("127.0.0.1", 18080, "root", "root");
    bool ret;
    ret = client.pingIoTDB();
    return ret;
}


