/**
 * ExportToTrino - Teradata Table Operator with Socket-based Data Transfer
 * 
 * High-Performance Massively Parallel Data Export from Teradata to Trino
 * 
 * This version sends data to a Python Arrow Bridge service via TCP socket.
 * The bridge service then forwards the data to Trino via Arrow Flight.
 * 
 * Key Features:
 * - FULLY DYNAMIC: Handles any input table schema
 * - SOCKET-BASED: Sends data via TCP to the Arrow Bridge
 * - PARALLEL EXECUTION: Runs on all AMPs simultaneously
 * 
 * Parameters (via environment variables):
 * - EXPORT_BRIDGE_HOST: Bridge service host (default: environment variable or 172.27.251.157)
 * - EXPORT_BRIDGE_PORT: Bridge service port (default: 9999)
 * - EXPORT_QUERY_ID: Query ID for routing (default: default-query)
 * - EXPORT_BATCH_SIZE: Rows per batch (default: 1000)
 * 
 * Based on Teradata SQL External Routine Programming documentation.
 */

#define SQL_TEXT Latin_Text
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include "sqltypes_td.h"

#define SetError(e, m) strcpy((char *)sqlstate, (e)); strcpy((char *)error_message, (m))
#define BATCH_SIZE 1000
#define MAX_COLUMNS 256
#define BUFFER_SIZE 1048576  /* 1MB buffer */

/* ============================================================
 * Data Structures
 * ============================================================ */

typedef struct {
    int colcount;
    FNC_TblOpColumnDef_t *iCols;
    FNC_TblOpHandle_t *Handle;
    int is_eof;
    int dimension;
} InputInfo_t;

typedef struct {
    char bridge_host[256];
    int bridge_port;
    char query_id[256];
    int batch_size;
} ExportParams_t;

typedef struct {
    INTEGER amp_id;
    BIGINT rows_processed;
    BIGINT bytes_sent;
    BIGINT null_count;
    BIGINT batches_sent;
    int error_code;
    char error_message[256];
} ExportStats_t;

/* ============================================================
 * Network Helpers
 * ============================================================ */

static int write_uint32(unsigned char *buf, unsigned int val) {
    buf[0] = (val >> 24) & 0xFF;
    buf[1] = (val >> 16) & 0xFF;
    buf[2] = (val >> 8) & 0xFF;
    buf[3] = val & 0xFF;
    return 4;
}

static int write_uint16(unsigned char *buf, unsigned short val) {
    buf[0] = (val >> 8) & 0xFF;
    buf[1] = val & 0xFF;
    return 2;
}

static int write_int32(unsigned char *buf, int val) {
    buf[0] = (val >> 24) & 0xFF;
    buf[1] = (val >> 16) & 0xFF;
    buf[2] = (val >> 8) & 0xFF;
    buf[3] = val & 0xFF;
    return 4;
}

static int write_int64(unsigned char *buf, long long val) {
    buf[0] = (val >> 56) & 0xFF;
    buf[1] = (val >> 48) & 0xFF;
    buf[2] = (val >> 40) & 0xFF;
    buf[3] = (val >> 32) & 0xFF;
    buf[4] = (val >> 24) & 0xFF;
    buf[5] = (val >> 16) & 0xFF;
    buf[6] = (val >> 8) & 0xFF;
    buf[7] = val & 0xFF;
    return 8;
}

/* ============================================================
 * Parameter Parsing
 * ============================================================ */

static void parse_params(ExportParams_t *params) {
    char *env_val;
    
    /* Initialize defaults */
    strcpy(params->bridge_host, "172.27.251.157");
    params->bridge_port = 9999;
    strcpy(params->query_id, "default-query");
    params->batch_size = BATCH_SIZE;
    
    /* BridgeHost */
    env_val = getenv("EXPORT_BRIDGE_HOST");
    if (env_val && strlen(env_val) > 0 && strlen(env_val) < 255) {
        strcpy(params->bridge_host, env_val);
    }
    
    /* BridgePort */
    env_val = getenv("EXPORT_BRIDGE_PORT");
    if (env_val) {
        int port = atoi(env_val);
        if (port > 0 && port <= 65535) {
            params->bridge_port = port;
        }
    }
    
    /* QueryID */
    env_val = getenv("EXPORT_QUERY_ID");
    if (env_val && strlen(env_val) > 0 && strlen(env_val) < 255) {
        strcpy(params->query_id, env_val);
    }
    
    /* BatchSize */
    env_val = getenv("EXPORT_BATCH_SIZE");
    if (env_val) {
        int batch_size = atoi(env_val);
        if (batch_size > 0 && batch_size <= 100000) {
            params->batch_size = batch_size;
        }
    }
}

static int calculate_value_size(FNC_TblOpColumnDef_t *cols, int col_idx, void *value, int length) {
    int bytesize = cols->column_types[col_idx].bytesize;
    return (bytesize > 0) ? bytesize : (length > 0 ? length : 0);
}

/* ============================================================
 * Contract Function
 * ============================================================ */

void ExportToTrino_contract(
    INTEGER *Result,
    int *indicator_Result,
    char sqlstate[6],
    SQL_TEXT extname[129],
    SQL_TEXT specific_name[129],
    SQL_TEXT error_message[257])
{
    FNC_TblOpColumnDef_t *oCols;
    InputInfo_t *icolinfo;
    int incount, outcount;
    int i;
    Stream_Fmt_en format;
    char mycontract[] = "ExportToTrino v4.0 - Socket Bridge to Arrow Flight";

    (void)extname;
    (void)specific_name;

    FNC_TblOpGetStreamCount(&incount, &outcount);
    
    if (incount == 0) {
        SetError("U0001", "ExportToTrino requires at least one input stream.");
        *Result = -1;
        return;
    }

    /* Read input column info */
    icolinfo = (InputInfo_t *)FNC_malloc(incount * sizeof(InputInfo_t));
    
    for (i = 0; i < incount; i++) {
        icolinfo[i].colcount = FNC_TblOpGetColCount(i, ISINPUT);
        icolinfo[i].iCols = (FNC_TblOpColumnDef_t *)FNC_malloc(TblOpSIZECOLDEF(icolinfo[i].colcount));
        TblOpINITCOLDEF(icolinfo[i].iCols, icolinfo[i].colcount);
        FNC_TblOpGetColDef(i, ISINPUT, icolinfo[i].iCols);
    }

    /* Output schema: 7 columns */
    oCols = (FNC_TblOpColumnDef_t *)FNC_malloc(TblOpSIZECOLDEF(7));
    memset(oCols, 0, TblOpSIZECOLDEF(7));
    oCols->num_columns = 7;
    oCols->length = TblOpSIZECOLDEF(7) - (2 * sizeof(int));
    TblOpINITCOLDEF(oCols, 7);

    /* Column 0: amp_id INTEGER */
    oCols->column_types[0].datatype = INTEGER_DT;
    oCols->column_types[0].size.length = 4;
    oCols->column_types[0].bytesize = 4;

    /* Column 1: rows_processed BIGINT */
    oCols->column_types[1].datatype = BIGINT_DT;
    oCols->column_types[1].size.length = 8;
    oCols->column_types[1].bytesize = 8;

    /* Column 2: bytes_sent BIGINT */
    oCols->column_types[2].datatype = BIGINT_DT;
    oCols->column_types[2].size.length = 8;
    oCols->column_types[2].bytesize = 8;

    /* Column 3: null_count BIGINT */
    oCols->column_types[3].datatype = BIGINT_DT;
    oCols->column_types[3].size.length = 8;
    oCols->column_types[3].bytesize = 8;

    /* Column 4: batches_sent BIGINT */
    oCols->column_types[4].datatype = BIGINT_DT;
    oCols->column_types[4].size.length = 8;
    oCols->column_types[4].bytesize = 8;

    /* Column 5: input_columns INTEGER */
    oCols->column_types[5].datatype = INTEGER_DT;
    oCols->column_types[5].size.length = 4;
    oCols->column_types[5].bytesize = 4;

    /* Column 6: status VARCHAR(256) */
    oCols->column_types[6].datatype = VARCHAR_DT;
    oCols->column_types[6].size.length = 256;
    oCols->column_types[6].charset = LATIN_CT;
    oCols->column_types[6].bytesize = 258;

    FNC_TblOpSetContractDef(mycontract, strlen(mycontract) + 1);
    FNC_TblOpSetOutputColDef(0, oCols);
    
    format = INDICFMT1;
    for (i = 0; i < incount; i++) {
        FNC_TblOpSetFormat("RECFMT", i, ISINPUT, &format, sizeof(format));
    }
    FNC_TblOpSetFormat("RECFMT", 0, ISOUTPUT, &format, sizeof(format));

    FNC_free(oCols);
    for (i = 0; i < incount; i++) {
        FNC_free(icolinfo[i].iCols);
    }
    FNC_free(icolinfo);

    *Result = 1;
    *indicator_Result = 0;
}

/* ============================================================
 * Main Execution Function
 * ============================================================ */

void ExportToTrino(void)
{
    int i, j, col;
    FNC_TblOpColumnDef_t *iCols;
    FNC_TblOpHandle_t *inHandle, *outHandle;
    int incount, outcount;
    InputInfo_t *icolinfo;
    int result;
    static int amp_counter = 0;
    
    /* Parameters and statistics */
    ExportParams_t params;
    ExportStats_t stats;
    int total_input_columns = 0;
    
    /* Socket connection */
    int sock_fd = -1;
    struct sockaddr_in server_addr;
    
    /* Batch buffer */
    unsigned char *batch_buffer;
    int batch_offset;
    int rows_in_batch;
    
    /* Output values */
    static INTEGER out_amp_id;
    static BIGINT out_rows_processed;
    static BIGINT out_bytes_sent;
    static BIGINT out_null_count;
    static BIGINT out_batches_sent;
    static INTEGER out_input_columns;
    static char out_status[258];
    short status_len;

    /* Parse parameters */
    parse_params(&params);

    /* Initialize stats */
    stats.amp_id = amp_counter++;
    stats.rows_processed = 0;
    stats.bytes_sent = 0;
    stats.null_count = 0;
    stats.batches_sent = 0;
    stats.error_code = 0;
    strcpy(stats.error_message, "SUCCESS");

    FNC_TblOpGetStreamCount(&incount, &outcount);
    icolinfo = (InputInfo_t *)FNC_malloc(incount * sizeof(InputInfo_t));

    /* Open input streams */
    for (i = 0; i < incount; i++) {
        icolinfo[i].colcount = FNC_TblOpGetColCount(i, ISINPUT);
        total_input_columns += icolinfo[i].colcount;
        
        icolinfo[i].iCols = (FNC_TblOpColumnDef_t *)FNC_malloc(TblOpSIZECOLDEF(icolinfo[i].colcount));
        TblOpINITCOLDEF(icolinfo[i].iCols, icolinfo[i].colcount);
        FNC_TblOpGetColDef(i, ISINPUT, icolinfo[i].iCols);
        icolinfo[i].Handle = (FNC_TblOpHandle_t *)FNC_TblOpOpen(i, 'r', 0);
        icolinfo[i].dimension = FNC_TblOpIsDimension(i, ISINPUT);
        icolinfo[i].is_eof = 0;
    }

    outHandle = (FNC_TblOpHandle_t *)FNC_TblOpOpen(0, 'w', 0);
    
    /* Allocate batch buffer */
    batch_buffer = (unsigned char *)FNC_malloc(BUFFER_SIZE);
    batch_offset = 0;
    rows_in_batch = 0;

    /* Connect to bridge service */
    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        stats.error_code = 1;
        strcpy(stats.error_message, "Socket creation failed");
        goto cleanup;
    }
    
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(params.bridge_port);
    if (inet_pton(AF_INET, params.bridge_host, &server_addr.sin_addr) <= 0) {
        stats.error_code = 2;
        snprintf(stats.error_message, sizeof(stats.error_message), 
                 "Invalid address: %s", params.bridge_host);
        goto cleanup;
    }
    
    if (connect(sock_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        stats.error_code = 3;
        snprintf(stats.error_message, sizeof(stats.error_message), 
                 "Connect failed to %s:%d", params.bridge_host, params.bridge_port);
        goto cleanup;
    }
    
    /* Send header: query_id */
    {
        unsigned char header[512];
        int hdr_offset = 0;
        char schema_json[2048];
        int schema_len;
        
        /* Query ID */
        int qid_len = strlen(params.query_id);
        hdr_offset += write_uint32(header + hdr_offset, qid_len);
        memcpy(header + hdr_offset, params.query_id, qid_len);
        hdr_offset += qid_len;
        
        /* Build schema JSON */
        strcpy(schema_json, "{\"columns\":[");
        for (col = 0; col < icolinfo[0].colcount; col++) {
            char col_def[256];
            const char *type_name;
            
            switch (icolinfo[0].iCols->column_types[col].datatype) {
                case INTEGER_DT: type_name = "INTEGER"; break;
                case BIGINT_DT: type_name = "BIGINT"; break;
                case VARCHAR_DT:
                case CHAR_DT: type_name = "VARCHAR"; break;
                default: type_name = "VARCHAR"; break;
            }
            
            snprintf(col_def, sizeof(col_def), 
                     "%s{\"name\":\"col_%d\",\"type\":\"%s\"}",
                     col > 0 ? "," : "", col, type_name);
            strcat(schema_json, col_def);
        }
        strcat(schema_json, "]}");
        
        schema_len = strlen(schema_json);
        hdr_offset += write_uint32(header + hdr_offset, schema_len);
        
        send(sock_fd, header, hdr_offset, 0);
        send(sock_fd, schema_json, schema_len, 0);
    }

    /* Reserve first 4 bytes for num_rows in batch */
    batch_offset = 4;
    rows_in_batch = 0;

    /* Main row processing loop */
    while (1) {
        int all_streams_eof = 1;

        for (j = 0; j < incount; j++) {
            if (icolinfo[j].is_eof == 0) {
                result = FNC_TblOpRead(icolinfo[j].Handle);
            }
            if (result == TBLOP_EOF) {
                icolinfo[j].is_eof = 1;
            } else if (result == TBLOP_SUCCESS) {
                icolinfo[j].is_eof = 0;
            }
            all_streams_eof = all_streams_eof & icolinfo[j].is_eof;
        }

        if (all_streams_eof) break;

        for (j = 0; j < incount; j++) {
            if (icolinfo[j].is_eof == 0) {
                inHandle = icolinfo[j].Handle;
                iCols = icolinfo[j].iCols;
                
                stats.rows_processed++;
                rows_in_batch++;
                
                /* Serialize row to batch buffer */
                for (col = 0; col < iCols->num_columns; col++) {
                    void *value = inHandle->row->columnptr[col];
                    int length = inHandle->row->lengths[col];
                    int is_null = TBLOPISNULL(inHandle->row->indicators, col);
                    
                    /* Write null indicator */
                    batch_buffer[batch_offset++] = is_null ? 1 : 0;
                    
                    if (is_null) {
                        stats.null_count++;
                    } else {
                        int dtype = iCols->column_types[col].datatype;
                        
                        if (dtype == INTEGER_DT) {
                            batch_offset += write_int32(batch_buffer + batch_offset, *(int*)value);
                            stats.bytes_sent += 4;
                        } else if (dtype == BIGINT_DT) {
                            batch_offset += write_int64(batch_buffer + batch_offset, *(long long*)value);
                            stats.bytes_sent += 8;
                        } else {
                            /* VARCHAR - get length from first 2 bytes, then data */
                            short str_len = *(short*)value;
                            batch_offset += write_uint16(batch_buffer + batch_offset, str_len);
                            memcpy(batch_buffer + batch_offset, (char*)value + 2, str_len);
                            batch_offset += str_len;
                            stats.bytes_sent += 2 + str_len;
                        }
                    }
                }
                
                /* Send batch when full or buffer getting large */
                if (rows_in_batch >= params.batch_size || batch_offset > BUFFER_SIZE - 4096) {
                    /* Write num_rows at the beginning of batch */
                    write_uint32(batch_buffer, rows_in_batch);
                    
                    /* Send batch length then data */
                    unsigned char len_buf[4];
                    write_uint32(len_buf, batch_offset);
                    send(sock_fd, len_buf, 4, 0);
                    send(sock_fd, batch_buffer, batch_offset, 0);
                    
                    stats.batches_sent++;
                    batch_offset = 4;
                    rows_in_batch = 0;
                }
            }
        }
    }
    
    /* Send final batch if any rows remaining */
    if (rows_in_batch > 0) {
        write_uint32(batch_buffer, rows_in_batch);
        
        unsigned char len_buf[4];
        write_uint32(len_buf, batch_offset);
        send(sock_fd, len_buf, 4, 0);
        send(sock_fd, batch_buffer, batch_offset, 0);
        
        stats.batches_sent++;
    }
    
    /* Send end marker (length = 0) */
    {
        unsigned char end_marker[4] = {0, 0, 0, 0};
        send(sock_fd, end_marker, 4, 0);
    }
    
    /* Wait for acknowledgment from bridge */
    {
        char ack[8];
        recv(sock_fd, ack, 2, 0);
    }
    
    snprintf(stats.error_message, sizeof(stats.error_message), 
             "SUCCESS [%s:%d] %lld rows", 
             params.bridge_host, params.bridge_port, (long long)stats.rows_processed);

cleanup:
    if (sock_fd >= 0) {
        close(sock_fd);
    }
    
    if (batch_buffer) {
        FNC_free(batch_buffer);
    }

    /* Write output summary row */
    out_amp_id = stats.amp_id;
    out_rows_processed = stats.rows_processed;
    out_bytes_sent = stats.bytes_sent;
    out_null_count = stats.null_count;
    out_batches_sent = stats.batches_sent;
    out_input_columns = total_input_columns;
    
    status_len = (short)strlen(stats.error_message);
    memcpy(out_status, &status_len, 2);
    strcpy(out_status + 2, stats.error_message);

    outHandle->row->columnptr[0] = (void *)&out_amp_id;
    outHandle->row->lengths[0] = sizeof(INTEGER);
    outHandle->row->columnptr[1] = (void *)&out_rows_processed;
    outHandle->row->lengths[1] = sizeof(BIGINT);
    outHandle->row->columnptr[2] = (void *)&out_bytes_sent;
    outHandle->row->lengths[2] = sizeof(BIGINT);
    outHandle->row->columnptr[3] = (void *)&out_null_count;
    outHandle->row->lengths[3] = sizeof(BIGINT);
    outHandle->row->columnptr[4] = (void *)&out_batches_sent;
    outHandle->row->lengths[4] = sizeof(BIGINT);
    outHandle->row->columnptr[5] = (void *)&out_input_columns;
    outHandle->row->lengths[5] = sizeof(INTEGER);
    outHandle->row->columnptr[6] = (void *)out_status;
    outHandle->row->lengths[6] = 2 + status_len;

    memset(outHandle->row->indicators, 0, sizeof(outHandle->row->indicators));
    FNC_TblOpWrite(outHandle);

    for (i = 0; i < incount; i++) {
        FNC_free(icolinfo[i].iCols);
        FNC_TblOpClose(icolinfo[i].Handle);
    }
    FNC_free(icolinfo);
    FNC_TblOpClose(outHandle);
}
