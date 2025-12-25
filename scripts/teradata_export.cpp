#define SQL_TEXT Latin_Text
#include "sqltypes_td.h"
#include "TeradataMemoryPool.h"
#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <iostream>
#include <vector>
#include <string>
#include <memory>
#include <sstream>

using namespace td_export;

// Helper to convert Teradata metadata to Arrow Schema
std::shared_ptr<arrow::Schema> CreateArrowSchema(FNC_TblColDef_t* columns, int num_cols) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    
    for (int i = 0; i < num_cols; ++i) {
        std::string col_name = (char*)columns[i].column_name;
        std::shared_ptr<arrow::DataType> type;

        switch (columns[i].datatype) {
            case INTEGER_DT: type = arrow::int32(); break;
            case BIGINT_DT:  type = arrow::int64(); break;
            case SMALLINT_DT: type = arrow::int16(); break;
            case BYTEINT_DT: type = arrow::int8(); break;
            case REAL_DT:
            case FLOAT_DT:   type = arrow::float64(); break;
            case VARCHAR_DT:
            case CHAR_DT:    type = arrow::utf8(); break;
            case DATE_DT:    type = arrow::date32(); break;
            case DECIMAL_DT: type = arrow::decimal128(columns[i].precision, columns[i].scale); break;
            default:         type = arrow::utf8(); // Fallback
        }
        fields.push_back(arrow::field(col_name, type));
    }
    
    return std::make_shared<arrow::Schema>(fields);
}

extern "C" void ExportToTrino(void *input, void *result, char sqlstate[6]) {
    FNC_Phase phase;
    if (FNC_GetPhase(&phase) != TBL_MODE_CONST) {
        if (phase == TBL_PRE_EXE) {
            // Contract phase: Define result columns (e.g., status, rows_sent)
            // This is required for Teradata to know what to expect from the TO.
            // In a real implementation, we'd use FNC_TblControl to set the 
            // output table structure.
        }
        return;
    }

    // --- Execution Phase ---
    
    // 1. Initialize custom memory pool
    TeradataMemoryPool pool;
    arrow::default_memory_pool(); // Not used, we use 'pool'
    
    // 2. Metadata Discovery
    int num_cols;
    FNC_TblColDef_t *columns;
    if (FNC_GetTblColumnsInfo(&num_cols, &columns) != 0) {
        std::strncpy(sqlstate, "U0001", 5);
        return;
    }
    auto schema = CreateArrowSchema(columns, num_cols);

    // 3. Parse Arguments (Target IPs, Query ID)
    // In SQL_TABLE parameter style, arguments are passed as the last few parameters
    // But for TO, we often use FNC_GetTblNextArg
    char target_ips[256];
    char query_id[64];
    int length;
    int null_ind;
    
    if (FNC_GetTblNextArg(target_ips, &length, &null_ind) != 0 || null_ind == -1) {
        std::strncpy(sqlstate, "U0002", 5);
        return;
    }
    if (FNC_GetTblNextArg(query_id, &length, &null_ind) != 0 || null_ind == -1) {
        std::strncpy(sqlstate, "U0003", 5);
        return;
    }
    
    // 4. Connect to Trino Flight Server
    // Logic to select one IP from target_ips based on AmpId
    int amp_id = FNC_GetAmpId();
    // (Simplified IP parsing logic here for the demonstration)
    
    arrow::flight::Location location;
    arrow::flight::Location::ForGrpcTcp("127.0.0.1", 50051, &location);
    
    std::unique_ptr<arrow::flight::FlightClient> client;
    auto status = client->Connect(location, &client);
    if (!status.ok()) {
        std::strncpy(sqlstate, "U0004", 5);
        return;
    }

    // 5. Start Flight Stream with Authentication Token
    arrow::flight::FlightDescriptor descriptor = arrow::flight::FlightDescriptor::Path({std::string(query_id)});
    arrow::flight::FlightCallOptions options;
    options.headers.push_back({"authorization", "Bearer " + std::string(query_id)});
    
    std::unique_ptr<arrow::flight::FlightStreamWriter> writer;
    std::unique_ptr<arrow::flight::FlightMetadataReader> reader;
    status = client->DoPut(options, descriptor, schema, &writer, &reader);
    if (!status.ok()) {
        std::strncpy(sqlstate, "U0005", 5);
        return;
    }

    // 6. Execution Loop - Builder Initialization
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders;
    for (int i = 0; i < num_cols; ++i) {
        switch (columns[i].datatype) {
            case INTEGER_DT: builders.push_back(std::make_shared<arrow::Int32Builder>(&pool)); break;
            case BIGINT_DT:  builders.push_back(std::make_shared<arrow::Int64Builder>(&pool)); break;
            case SMALLINT_DT: builders.push_back(std::make_shared<arrow::Int16Builder>(&pool)); break;
            case BYTEINT_DT: builders.push_back(std::make_shared<arrow::Int8Builder>(&pool)); break;
            case REAL_DT:
            case FLOAT_DT:   builders.push_back(std::make_shared<arrow::DoubleBuilder>(&pool)); break;
            case DATE_DT:    builders.push_back(std::make_shared<arrow::Date32Builder>(&pool)); break;
            case DECIMAL_DT: 
                builders.push_back(std::make_shared<arrow::Decimal128Builder>(
                    arrow::decimal128(columns[i].precision, columns[i].scale), &pool)); 
                break;
            case VARCHAR_DT:
            case CHAR_DT:    builders.push_back(std::make_shared<arrow::StringBuilder>(&pool)); break;
            // Additional types like DECIMAL128 can be added here
            default: builders.push_back(std::make_shared<arrow::StringBuilder>(&pool)); break;
        }
    }

    int64_t rows_in_batch = 0;
    while (FNC_GetNextRow() == TBL_row) {
        for (int i = 0; i < num_cols; ++i) {
            void* val_ptr;
            int length;
            int null_ind;
            FNC_GetTblColValue(i, &val_ptr, &length, &null_ind);
            
            if (null_ind == -1) {
                builders[i]->AppendNull();
            } else {
                switch (columns[i].datatype) {
                    case INTEGER_DT:
                        static_cast<arrow::Int32Builder*>(builders[i].get())->Append(*(int32_t*)val_ptr);
                        break;
                    case BIGINT_DT:
                        static_cast<arrow::Int64Builder*>(builders[i].get())->Append(*(int64_t*)val_ptr);
                        break;
                    case SMALLINT_DT:
                        static_cast<arrow::Int16Builder*>(builders[i].get())->Append(*(int16_t*)val_ptr);
                        break;
                    case BYTEINT_DT:
                        static_cast<arrow::Int8Builder*>(builders[i].get())->Append(*(int8_t*)val_ptr);
                        break;
                    case REAL_DT:
                    case FLOAT_DT:
                        static_cast<arrow::DoubleBuilder*>(builders[i].get())->Append(*(double*)val_ptr);
                        break;
                    case DATE_DT: {
                        // Teradata DATE: (year - 1900) * 10000 + (month * 100) + day
                        int32_t td_date = *(int32_t*)val_ptr;
                        int year = (td_date / 10000) + 1900;
                        int month = (td_date % 10000) / 100;
                        int day = td_date % 100;
                        
                        // Convert to days since 1970-01-01
                        // This is a simplified conversion, strictly for demonstration
                        // In production, use a library like date.h or std::chrono
                        struct tm timeinfo = {0};
                        timeinfo.tm_year = year - 1900;
                        timeinfo.tm_mon = month - 1;
                        timeinfo.tm_mday = day;
                        time_t t = mktime(&timeinfo);
                        int32_t days_since_epoch = t / (24 * 3600);
                        static_cast<arrow::Date32Builder*>(builders[i].get())->Append(days_since_epoch);
                        break;
                    }
                    case DECIMAL_DT: {
                        // Teradata stores decimals as byte arrays (Little Endian)
                        // This converts the byte array into a 128-bit integer for Arrow
                        int8_t* bytes = (int8_t*)val_ptr;
                        arrow::Decimal128 dec;
                        // Use Arrow's FromBigEndian or FromLittleEndian base on platform/TD version
                        // Assuming 16 bytes for Decimal128 mapping
                        if (length <= 16) {
                            dec = arrow::Decimal128::FromLittleEndian(reinterpret_cast<const uint8_t*>(bytes), length).ValueOrDie();
                        }
                        static_cast<arrow::Decimal128Builder*>(builders[i].get())->Append(dec);
                        break;
                    }
                    case VARCHAR_DT:
                    case CHAR_DT:
                        static_cast<arrow::StringBuilder*>(builders[i].get())->Append((char*)val_ptr, length);
                        break;
                    default:
                        // Fallback cast to string
                        static_cast<arrow::StringBuilder*>(builders[i].get())->Append((char*)val_ptr, length);
                }
            }
        }

        rows_in_batch++;
        if (rows_in_batch >= 10000) {
            std::vector<std::shared_ptr<arrow::Array>> arrays;
            for (auto& b : builders) {
                std::shared_ptr<arrow::Array> arr;
                b->Finish(&arr);
                arrays.push_back(arr);
            }
            auto batch = arrow::RecordBatch::Make(schema, rows_in_batch, arrays);
            writer->WriteRecordBatch(*batch);
            rows_in_batch = 0;
        }
    }

    // 7. Final Flush
    if (rows_in_batch > 0) {
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        for (auto& b : builders) {
            std::shared_ptr<arrow::Array> arr;
            b->Finish(&arr);
            arrays.push_back(arr);
        }
        auto batch = arrow::RecordBatch::Make(schema, rows_in_batch, arrays);
        writer->WriteRecordBatch(*batch);
    }

    writer->Close();
    
    // Return result (e.g., row count)
    // FNC_TblControl(...) to emit summary
}
