/**
 * ExportToTrino - Teradata Table Operator with Socket-based Data Transfer
 * 
 * High-Performance Massively Parallel Data Export from Teradata to Trino
 */

#define SQL_TEXT Latin_Text
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <zlib.h>
#include <netinet/tcp.h>
#include "lz4.h"
#include "sqltypes_td.h"

/* Real Teradata Internal Data Type Codes confirmed by binary diagnostics */
/* Real Teradata Internal Data Type Codes confirmed by binary diagnostics */
/* Using standard SQLTYPES_TD.H enums instead of manual defines to prevent duplicate case errors */
/* #define TD_CHAR 1 */
/* #define TD_VARCHAR 2 */ 
/* ... Relying on system headers ... */

#define BATCH_SIZE 1000
#define BUFFER_SIZE 16777216  /* 16MB buffer for throughput - safe for FNC_malloc */

typedef struct {
    char bridge_host[256];
    int bridge_port;
    char query_id[256];
    char security_token[256];
    int batch_size;
    int compression_type;
} ExportParams_t;

typedef struct {
    INTEGER amp_id;
    BIGINT rows_processed;
    BIGINT bytes_sent;
    BIGINT null_count;
    BIGINT batches_sent;
    int error_code;
    char error_message[250];
} ExportStats_t;

/* Prototypes to avoid warnings */
void ExportToTrino(void);
void ExportToTrino_contract(INTEGER *Result, int *indicator_Result, char sqlstate[6], SQL_TEXT extname[129], SQL_TEXT specific_name[129], SQL_TEXT error_message[257]);

/* Network Helpers - Big Endian Swapping */
static int write_uint32(unsigned char *buf, unsigned int val) {
    buf[0] = (val >> 24) & 0xFF; buf[1] = (val >> 16) & 0xFF;
    buf[2] = (val >> 8) & 0xFF;  buf[3] = val & 0xFF;
    return 4;
}

static int send_all(int sock_fd, const void *buf, size_t len) {
    const char *p = (const char *)buf;
    while (len > 0) {
        ssize_t r = send(sock_fd, p, len, 0);
        if (r < 0) return -1;
        if (r == 0) return -1;
        p += r;
        len -= r;
    }
    return 0;
}
static int write_uint16(unsigned char *buf, unsigned short val) {
    buf[0] = (val >> 8) & 0xFF; buf[1] = val & 0xFF;
    return 2;
}
static int write_int32(unsigned char *buf, int val) {
    buf[0] = (val >> 24) & 0xFF; buf[1] = (val >> 16) & 0xFF;
    buf[2] = (val >> 8) & 0xFF;  buf[3] = val & 0xFF;
    return 4;
}
static int write_int64(unsigned char *buf, long long val) {
    buf[0] = (val >> 56) & 0xFF; buf[1] = (val >> 48) & 0xFF;
    buf[2] = (val >> 40) & 0xFF; buf[3] = (val >> 32) & 0xFF;
    buf[4] = (val >> 24) & 0xFF; buf[5] = (val >> 16) & 0xFF;
    buf[6] = (val >> 8) & 0xFF;  buf[7] = val & 0xFF;
    return 8;
}

/* Date/Time Helpers */
static int ymd_to_epoch_days(int y, int m, int d) {
    if (m <= 2) { y -= 1; m += 12; }
    int era = (y >= 0 ? y : y - 399) / 400;
    unsigned yoe = (unsigned)(y - era * 400);
    unsigned doy = (153 * (m - 3) + 2) / 5 + d - 1;
    unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (int)doe - 719468;
}

static long long time_to_picos(void *val) {
    unsigned int s_scaled; memcpy(&s_scaled, val, 4);
    unsigned char hour = ((unsigned char*)val)[4], min = ((unsigned char*)val)[5];
    /* Trino TIME expects picos since midnight */
    return ((long long)(hour % 24) * 3600 + (long long)(min % 60) * 60) * 1000000000000LL + (long long)s_scaled * 1000000LL;
}

static long long timestamp_to_micros(void *val) {
    unsigned int s_scaled; memcpy(&s_scaled, val, 4);
    unsigned short year; memcpy(&year, (char*)val + 4, 2);
    unsigned char mon = ((unsigned char*)val)[6], day = ((unsigned char*)val)[7], 
                  hour = ((unsigned char*)val)[8], min = ((unsigned char*)val)[9];
    int days = ymd_to_epoch_days(year, mon, day);
    /* Trino TIMESTAMP expects micros since epoch */
    return (long long)days * 86400000000LL + (long long)(hour % 24) * 3600000000LL + (long long)(min % 60) * 60000000LL + (long long)s_scaled;
}

static int send_batch_to_bridge(int sock_fd, unsigned char *bb, int batch_offset, int rows, int compression_type, unsigned char **dest_ptr, unsigned long *dest_cap) {
    write_uint32(bb, rows);
    if (compression_type == 0) {  /* 0 = None */
        unsigned char lb[4]; write_uint32(lb, batch_offset);
        if (send_all(sock_fd, lb, 4) < 0 || send_all(sock_fd, bb, batch_offset) < 0) return -1;
        return 0;
    }
    
    /* Ensure compression buffer is large enough */
    unsigned long bound = (compression_type == 2) ? LZ4_compressBound(batch_offset) : compressBound(batch_offset);
    if (!*dest_ptr || *dest_cap < bound) {
        if (*dest_ptr) FNC_free(*dest_ptr);
        *dest_ptr = (unsigned char *)FNC_malloc(bound);
        if (!*dest_ptr) return -1;
        *dest_cap = bound;
    }

    unsigned long actual_len;
    if (compression_type == 2) { /* 2 = LZ4 */
        actual_len = LZ4_compress_default((const char*)bb, (char*)*dest_ptr, batch_offset, *dest_cap);
        if (actual_len <= 0) return -1;
    } else { /* 1 = ZLIB */
        actual_len = *dest_cap;
        if (compress(*dest_ptr, &actual_len, bb, batch_offset) != Z_OK) return -1;
    }
    
    unsigned char lb[4]; write_uint32(lb, (unsigned int)actual_len);
    if (send_all(sock_fd, lb, 4) < 0 || send_all(sock_fd, *dest_ptr, actual_len) < 0) return -1;
    return 0;
}

static int write_unicode_to_utf8(unsigned char *buf, const unsigned char *val, int bytes) {
    int i = 0, j = 0;
    unsigned char *out = buf + 2;
    while (i + 1 < bytes) {
        unsigned int cp;
        unsigned short w1 = val[i] | (val[i+1] << 8);
        i += 2;
        if (w1 >= 0xD800 && w1 <= 0xDBFF && i + 1 < bytes) {
            unsigned short w2 = val[i] | (val[i+1] << 8);
            i += 2;
            cp = (((w1 & 0x3FF) << 10) | (w2 & 0x3FF)) + 0x10000;
        } else {
            cp = w1;
        }
        if (cp < 0x80) out[j++] = cp;
        else if (cp < 0x800) { out[j++] = (cp >> 6)|0xC0; out[j++] = (cp&0x3F)|0x80; }
        else if (cp < 0x10000) { out[j++] = (cp >> 12)|0xE0; out[j++] = ((cp >> 6)&0x3F)|0x80; out[j++] = (cp&0x3F)|0x80; }
        else { out[j++] = (cp >> 18)|0xF0; out[j++] = ((cp >> 12)&0x3F)|0x80; out[j++] = ((cp >> 6)&0x3F)|0x80; out[j++] = (cp&0x3F)|0x80; }
    }
    write_uint16(buf, (unsigned short)j);
    return 2 + j;
}

static void parse_params_from_stream(ExportParams_t *params, FNC_TblOpHandle_t *param_stream) {
    char *target_ips = (char *)FNC_malloc(32768);
    if (target_ips) target_ips[0] = '\0';
    params->query_id[0] = '\0';
    params->batch_size = BATCH_SIZE;

    if (param_stream && FNC_TblOpRead(param_stream) == TBLOP_SUCCESS && target_ips) {
        int c;
        for (c = 0; c < 5; c++) {
            if (c >= FNC_TblOpGetColCount(1, ISINPUT)) break;
            void *val = param_stream->row->columnptr[c];
            if (!val || TBLOPISNULL(param_stream->row->indicators, c)) continue;
            
            int actual_len = param_stream->row->lengths[c];
            if (actual_len < 0) continue;
            
            if (c == 3) {
                int bs = 0;
                memcpy(&bs, val, 4);
                if (bs > 0) params->batch_size = bs;
                continue;
            }

            char tmp[1024] = "";
            char *src = (char*)val;
            int src_len = actual_len;

            /* Check for VARCHAR prefix (2 bytes length) */
            if (actual_len >= 2) {
                unsigned short vlen = *(unsigned short*)val;
                if (vlen == (unsigned short)(actual_len - 2)) {
                    src += 2; src_len = vlen;
                }
            }

            if (src_len > 0) {
                /* Detect UTF-16: if second byte is 0 */
                if (src_len >= 2 && src[1] == '\0') {
                    int i, j = 0;
                    for (i = 0; i < src_len && j < 1023; i += 2) {
                        tmp[j++] = src[i];
                    }
                    tmp[j] = '\0';
                } else {
                    int copy_len = (src_len > 1023) ? 1023 : src_len;
                    memcpy(tmp, src, copy_len);
                    tmp[copy_len] = '\0';
                }
            }

            /* Trim trailing spaces */
            int len = strlen(tmp);
            if (len > 0) {
                char *end = tmp + len - 1;
                while(end >= tmp && (*end == ' ' || *end == '\n' || *end == '\r' || *end == '\0')) { *end = '\0'; end--; }
            }

            if (c == 0) { strncpy(params->bridge_host, tmp, 255); params->bridge_host[255] = '\0'; strcpy(target_ips, tmp); }
            else if (c == 1) { strncpy(params->query_id, tmp, 255); params->query_id[255] = '\0'; }
            else if (c == 2) { strncpy(params->security_token, tmp, 255); params->security_token[255] = '\0'; }
            else if (c == 4) {
                if (strstr(tmp, "LZ4")) params->compression_type = 2;
                else if (strstr(tmp, "ZLIB")) params->compression_type = 1;
                else params->compression_type = 0;
            }
        }
    }

    /* Fallback for Security Token */
    if (params->security_token[0] == '\0') {
        char *env = getenv("EXPORT_SECURITY_TOKEN");
        if (env) strcpy(params->security_token, env);
    }

    /* Fallback for Target IPs */
    if (target_ips[0] == '\0') {
        char *env = getenv("EXPORT_BRIDGE_HOSTS");
        strcpy(target_ips, env ? env : "172.27.251.157:9999");
    }
    /* Fallback for Query ID */
    if (params->query_id[0] == '\0') {
        char *env = getenv("EXPORT_QUERY_ID");
        strcpy(params->query_id, env ? env : "default-query");
    }

    /* Select IP based on process ID for load balancing.
     * Each AMP vproc runs as a separate process with unique PID.
     * FNC_TblOpGetUniqID may return same value for all AMPs. */
    INTEGER amp_id = (INTEGER)getpid();
    char *ips[1024]; int ip_count = 0;
    char *saveptr;
    char *token = strtok_r(target_ips, ",", &saveptr);
    while (token && ip_count < 1024) {
        while (*token == ' ') token++; /* skip leading spaces */
        ips[ip_count++] = token;
        token = strtok_r(NULL, ",", &saveptr);
    }

    if (ip_count > 0) {
        char *chosen = ips[amp_id % ip_count];
        char *colon = strchr(chosen, ':');
        if (colon) {
            *colon = '\0'; strcpy(params->bridge_host, chosen);
            params->bridge_port = atoi(colon + 1);
        } else {
            strcpy(params->bridge_host, chosen); params->bridge_port = 9999;
        }
    } else {
        strcpy(params->bridge_host, "172.27.251.157"); params->bridge_port = 9999;
    }
    if (target_ips) FNC_free(target_ips);
}

static int write_hex_string(unsigned char *buf, void *value, int bytesize) {
    char hex[] = "0123456789ABCDEF";
    unsigned char *p = (unsigned char*)value;
    /* Limit hex string to avoid internal buffer overflows in batch */
    int len = bytesize * 2;
    if (len > 32767) len = 32767; 
    write_uint16(buf, (unsigned short)len);
    int i;
    for (i = 0; i < len/2; i++) {
        buf[2 + i*2] = hex[(p[i] >> 4) & 0xF];
        buf[2 + i*2 + 1] = hex[p[i] & 0xF];
    }
    return 2 + len;
}

static int write_decimal_binary(unsigned char *buf, void *value, int bytesize) {
    if (bytesize <= 8) {
        long long v = 0;
        if (bytesize == 1) v = *(__int8_t*)value;
        else if (bytesize == 2) v = *(__int16_t*)value;
        else if (bytesize == 4) v = *(__int32_t*)value;
        else if (bytesize == 8) v = *(long long*)value;
        return write_int64(buf, v);
    } else {
        /* 16-byte decimal, Trino expects Big Endian. Teradata is Little Endian. */
        unsigned char *p = (unsigned char *)value;
        int i;
        for (i = 0; i < 16; i++) {
            buf[i] = p[15 - i];
        }
        return 16;
    }
}

void ExportToTrino_contract(INTEGER *Result, int *indicator_Result, char sqlstate[6], SQL_TEXT extname[129], SQL_TEXT specific_name[129], SQL_TEXT error_message[257]) {
    FNC_TblOpColumnDef_t *oCols;
    int incount, outcount, i;
    Stream_Fmt_en format = INDICFMT1;
    char mycontract[] = "ExportToTrino v4.18";
    FNC_TblOpGetStreamCount(&incount, &outcount);
    oCols = (FNC_TblOpColumnDef_t *)FNC_malloc(TblOpSIZECOLDEF(7));
    TblOpINITCOLDEF(oCols, 7);
    oCols->num_columns = 7;
    oCols->column_types[0].datatype = INTEGER_DT; oCols->column_types[0].bytesize = 4;
    oCols->column_types[1].datatype = BIGINT_DT;  oCols->column_types[1].bytesize = 8;
    oCols->column_types[2].datatype = BIGINT_DT;  oCols->column_types[2].bytesize = 8;
    oCols->column_types[3].datatype = BIGINT_DT;  oCols->column_types[3].bytesize = 8;
    oCols->column_types[4].datatype = BIGINT_DT;  oCols->column_types[4].bytesize = 8;
    oCols->column_types[5].datatype = INTEGER_DT; oCols->column_types[5].bytesize = 4;
    oCols->column_types[6].datatype = VARCHAR_DT; oCols->column_types[6].bytesize = 258; oCols->column_types[6].size.length = 256; oCols->column_types[6].charset = LATIN_CT;
    FNC_TblOpSetContractDef(mycontract, strlen(mycontract) + 1);
    FNC_TblOpSetOutputColDef(0, oCols);
    /* Set format for primary data stream and output stream */
    FNC_TblOpSetFormat("RECFMT", 0, ISINPUT, &format, sizeof(format));
    FNC_TblOpSetFormat("RECFMT", 0, ISOUTPUT, &format, sizeof(format));
    FNC_free(oCols); *Result = 1; *indicator_Result = 0;
}

void ExportToTrino(void) {
    FNC_TblOpHandle_t *in = NULL, *out = NULL, *param_in = NULL;
    int col, sock_fd = -1, batch_offset = 4, rows_in_batch = 0, tic = 0;
    FNC_TblOpColumnDef_t *iCols = NULL;
    ExportParams_t params;
    ExportStats_t stats;
    unsigned char *bb = NULL;
    unsigned char *dest = NULL;
    unsigned long dest_cap = 0;
    int incount, outcount;

    memset(&stats, 0, sizeof(stats));
    FNC_TblOpGetStreamCount(&incount, &outcount);
    
    in = FNC_TblOpOpen(0, 'r', 0);
    out = FNC_TblOpOpen(0, 'w', 0);
    if (incount > 1) param_in = FNC_TblOpOpen(1, 'r', 0);

    parse_params_from_stream(&params, param_in);

    if (!in || !out) {
        stats.error_code = 1001; strcpy(stats.error_message, "Stream open failed"); goto send_status;
    }

    tic = FNC_TblOpGetColCount(0, ISINPUT);
    iCols = (FNC_TblOpColumnDef_t *)FNC_malloc(TblOpSIZECOLDEF(tic));
    TblOpINITCOLDEF(iCols, tic);
    FNC_TblOpGetColDef(0, ISINPUT, iCols);

    bb = (unsigned char *)FNC_malloc(BUFFER_SIZE);
    if (!bb) {
        stats.error_code = 1005; strcpy(stats.error_message, "Batch buffer malloc failed"); goto send_status;
    }
    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr; memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET; addr.sin_port = htons(params.bridge_port);
    inet_pton(AF_INET, params.bridge_host, &addr.sin_addr);
    if (connect(sock_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        stats.error_code = errno; snprintf(stats.error_message, 250, "Connect to %s:%d failed", params.bridge_host, params.bridge_port); 
        goto send_status;
    }

    /* Optimization D: Socket Resource Management */
    int flag = 1;
    setsockopt(sock_fd, IPPROTO_TCP, TCP_NODELAY, (char *)&flag, sizeof(int));
    int sndbuf = 4194304; /* 4MB */
    setsockopt(sock_fd, SOL_SOCKET, SO_SNDBUF, (char *)&sndbuf, sizeof(int));

    unsigned char ph[4096]; int ho = 0; 
    
    /* 1. Security Token (if configured) */
    if (params.security_token[0] != '\0') {
        int tl = strlen(params.security_token);
        ho += write_uint32(ph + ho, tl);
        memcpy(ph + ho, params.security_token, tl);
        ho += tl;
    }

    /* 2. Query ID */
    int ql = strlen(params.query_id);
    ho += write_uint32(ph + ho, ql); memcpy(ph+ho, params.query_id, ql); ho += ql;

    /* 3. Compression Type Flag */
    ho += write_uint32(ph + ho, params.compression_type);
    
    /* Allocate enough space for potentially large column metadata JSON */
    int sj_size = tic * 256 + 128;
    char *sj = (char *)FNC_malloc(sj_size);
    if (!sj) {
        stats.error_code = 1002; strcpy(stats.error_message, "Metadata malloc failed"); goto send_status;
    }
    strcpy(sj, "{\"columns\":[");
    for (col = 0; col < tic; col++) {
        char cd[256]; const char *tn; int dt = iCols->column_types[col].datatype;
        switch(dt) {
            case CHAR_DT: case VARCHAR_DT: tn = "VARCHAR"; break;
            case INTEGER_DT: case SMALLINT_DT: case BYTEINT_DT: tn = "INTEGER"; break;
            case BIGINT_DT: tn = "BIGINT"; break;
            case REAL_DT: tn = "DOUBLE"; break;
            case DATE_DT: tn = "DATE"; break; 
            case TIME_DT: tn = "TIME"; break; 
            case TIMESTAMP_DT: tn = "TIMESTAMP"; break; 
            case DECIMAL1_DT: case DECIMAL2_DT: case DECIMAL4_DT: case DECIMAL8_DT: 
            /* case 10: removed duplicate */
                tn = "DECIMAL_SHORT"; break;
            case DECIMAL16_DT:
                tn = "DECIMAL_LONG"; break;
            default: tn = "VARCHAR"; break;
        }
        snprintf(cd, 256, "%s{\"name\":\"col_%d\",\"type\":\"%s\"}", col > 0 ? "," : "", col, tn); strcat(sj, cd);
    }
    strcat(sj, "]}"); int sj_len = strlen(sj);
    ho += write_uint32(ph + ho, sj_len);
    if (send_all(sock_fd, ph, ho) < 0 || send_all(sock_fd, sj, sj_len) < 0) {
        stats.error_code = 1003; strcpy(stats.error_message, "Handshake send failed"); 
        FNC_free(sj); sj = NULL; goto send_status;
    }
    FNC_free(sj); sj = NULL;

    while (FNC_TblOpRead(in) == TBLOP_SUCCESS) {
        stats.rows_processed++; rows_in_batch++;
        for (col = 0; col < tic; col++) {
            bb[batch_offset++] = TBLOPISNULL(in->row->indicators, col) ? 1 : 0;
            if (TBLOPISNULL(in->row->indicators, col)) stats.null_count++;
            else {
                int dt = iCols->column_types[col].datatype;
                int cs = iCols->column_types[col].charset;
                void *val = in->row->columnptr[col];
                
                if (dt == VARCHAR_DT || dt == 2) { /* 2=TD_VARCHAR */
                    short blen = *(short*)val;
                    if (cs == 2 || cs == 6) batch_offset += write_unicode_to_utf8(bb + batch_offset, (unsigned char*)val + 2, blen);
                    else {
                        write_uint16(bb + batch_offset, blen); memcpy(bb + batch_offset + 2, (char*)val + 2, blen);
                        batch_offset += 2 + blen;
                    }
                } else if (dt == CHAR_DT || dt == 1) { /* 1=TD_CHAR */
                    int blen = iCols->column_types[col].bytesize;
                    if (cs == 2 || cs == 6) batch_offset += write_unicode_to_utf8(bb + batch_offset, (unsigned char*)val, blen);
                    else {
                        write_uint16(bb + batch_offset, (unsigned short)blen); memcpy(bb + batch_offset + 2, (char*)val, blen);
                        batch_offset += 2 + blen;
                    }
                } else if (dt == INTEGER_DT) batch_offset += write_int32(bb + batch_offset, *(int*)val);
                else if (dt == BIGINT_DT) batch_offset += write_int64(bb + batch_offset, *(long long*)val);
                else if (dt == SMALLINT_DT) batch_offset += write_int32(bb + batch_offset, (int)*(short*)val);
                else if (dt == BYTEINT_DT) batch_offset += write_int32(bb + batch_offset, (int)*(__int8_t*)val);
                else if (dt == REAL_DT) {
                    long long lv; memcpy(&lv, val, 8);
                    batch_offset += write_int64(bb + batch_offset, lv);
                } else if (dt == DATE_DT) {
                    int d = *(int*)val;
                    int y_off = d / 10000;
                    int md = d % 10000;
                    if (md < 0) { y_off--; md += 10000; }
                    int year = y_off + 1900;
                    int month = md / 100;
                    int day = md % 100;
                    batch_offset += write_int32(bb + batch_offset, ymd_to_epoch_days(year, month, day));
                } else if (dt == TIME_DT) {
                    batch_offset += write_int64(bb + batch_offset, time_to_picos(val));
                } else if (dt == TIMESTAMP_DT) {
                    batch_offset += write_int64(bb + batch_offset, timestamp_to_micros(val));
                } else if (dt == DECIMAL1_DT || dt == DECIMAL2_DT || dt == DECIMAL4_DT || dt == DECIMAL8_DT || dt == 14) { /* 14=TD_DECIMAL */
                    batch_offset += write_decimal_binary(bb + batch_offset, val, iCols->column_types[col].bytesize);
                } else if (dt == DECIMAL16_DT) {
                    batch_offset += write_decimal_binary(bb + batch_offset, val, 16);
                } else {
                    batch_offset += write_hex_string(bb + batch_offset, val, iCols->column_types[col].bytesize);
                }
            }
        }
        /* Safety check: ensure we don't overflow bb even with wide rows. 
           Max Teradata row is 1MB, so we check for 1MB safety margin. */
        if (rows_in_batch >= params.batch_size || batch_offset > BUFFER_SIZE - 1048576) {
            if (send_batch_to_bridge(sock_fd, bb, batch_offset, rows_in_batch, params.compression_type, &dest, &dest_cap) < 0) {
                stats.error_code = 1004; strcpy(stats.error_message, "Batch send failed"); break;
            }
            stats.batches_sent++; stats.bytes_sent += batch_offset;
            batch_offset = 4; rows_in_batch = 0;
        }
    }
    if (rows_in_batch > 0 && stats.error_code == 0) {
        send_batch_to_bridge(sock_fd, bb, batch_offset, rows_in_batch, params.compression_type, &dest, &dest_cap);
        stats.batches_sent++; stats.bytes_sent += batch_offset;
    }
    unsigned char emsg[4] = {0,0,0,0}; send_all(sock_fd, emsg, 4); 

send_status:
    if (sock_fd >= 0) close(sock_fd);
    static INTEGER ra; static BIGINT rr, rb, rn, rba; static INTEGER rc; static char rs[300];
    ra = 0; FNC_TblOpGetUniqID(&ra);
    rr = stats.rows_processed; rb = stats.bytes_sent; rn = stats.null_count; rba = stats.batches_sent; rc = tic;
    int char_len;
    if (stats.error_code == 0) {
        char_len = snprintf(rs + 2, 256, "[%s:%d] AMP:%d PID:%d SUCCESS (Query: %s)", params.bridge_host, params.bridge_port, ra, (int)getpid(), params.query_id);
    } else char_len = snprintf(rs + 2, 256, "ERROR %d: %s", stats.error_code, stats.error_message);
    if (char_len > 256) char_len = 256;
    unsigned short slen = (unsigned short)char_len;
    memcpy(rs, &slen, 2);
    if (out) {
        out->row->columnptr[0] = (void *)&ra; out->row->columnptr[1] = (void *)&rr; out->row->columnptr[2] = (void *)&rb;
        out->row->columnptr[3] = (void *)&rn; out->row->columnptr[4] = (void *)&rba; out->row->columnptr[5] = (void *)&rc;
        out->row->columnptr[6] = (void *)rs; out->row->lengths[0] = 4; out->row->lengths[1] = 8; out->row->lengths[2] = 8;
        out->row->lengths[3] = 8; out->row->lengths[4] = 8; out->row->lengths[5] = 4; out->row->lengths[6] = 2 + slen;
        memset(out->row->indicators, 0, 7); FNC_TblOpWrite(out); FNC_TblOpClose(out);
    }
    if (iCols) FNC_free(iCols);
    if (bb) FNC_free(bb);
    if (dest) FNC_free(dest);
    if (in) FNC_TblOpClose(in);
    if (param_in) FNC_TblOpClose(param_in);
}
