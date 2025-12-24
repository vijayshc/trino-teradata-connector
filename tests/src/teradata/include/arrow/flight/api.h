/* Mock arrow/flight/api.h for syntax verification */
#pragma once
#include <arrow/api.h>

namespace arrow {
namespace flight {
    class Location {
    public:
        static Status ForGrpcTcp(const std::string& host, int port, Location* out) { return Status::OK(); }
    };

    class FlightDescriptor {
    public:
        static FlightDescriptor Path(const std::vector<std::string>& path) { return FlightDescriptor(); }
    };

    class FlightCallOptions {
    public:
        std::vector<std::pair<std::string, std::string>> headers;
    };

    class FlightStreamWriter {
    public:
        Status WriteRecordBatch(const RecordBatch& batch) { return Status::OK(); }
        Status Close() { return Status::OK(); }
    };

    class FlightMetadataReader {};

    class FlightClient {
    public:
        static Status Connect(const Location& location, std::unique_ptr<FlightClient>* out) { return Status::OK(); }
        Status DoPut(const FlightCallOptions& options, const FlightDescriptor& descriptor, std::shared_ptr<Schema> schema, std::unique_ptr<FlightStreamWriter>* writer, std::unique_ptr<FlightMetadataReader>* reader) { return Status::OK(); }
    };
}
}
