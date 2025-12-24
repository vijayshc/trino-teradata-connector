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
#include "sqltypes_td.h"

/* Real Teradata Internal Data Type Codes confirmed by binary diagnostics */
#define TD_CHAR 1
#define TD_VARCHAR 2
#define TD_BYTEINT 7
#define TD_SMALLINT 8
#define TD_INTEGER 9
#define TD_FLOAT 10
#define TD_DECIMAL 14
#define TD_DATE 15
#define TD_TIME 16
#define TD_TIMESTAMP 17
#define TD_BIGINT 36

#define BATCH_SIZE 1000
#define BUFFER_SIZE 4194304

typedef struct {
    char bridge_host[256];
    int bridge_port;
    char query_id[256];
    char security_token[256];
    int batch_size;
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

/* UTF-16LE to UTF-8 Conversion */
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
    char target_ips[2048] = "";
    params->query_id[0] = '\0';
    params->batch_size = BATCH_SIZE;

    if (param_stream && FNC_TblOpRead(param_stream) == TBLOP_SUCCESS) {
        int c;
        for (c = 0; c < 2; c++) {
            void *val = param_stream->row->columnptr[c];
            if (TBLOPISNULL(param_stream->row->indicators, c)) continue;
            
            int actual_len = param_stream->row->lengths[c];
            char tmp[1024] = "";
            char *src = (char*)val;
            int src_len = actual_len;

            /* Check for VARCHAR prefix */
            if (actual_len >= 2) {
                short vlen = *(short*)val;
                if (vlen == (short)(actual_len - 2)) {
                    src += 2; src_len = vlen;
                }
            }

            if (src_len > 0) {
                /* Detect UTF-16: if second byte is 0 and fourth byte is 0 */
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
            char *end = tmp + strlen(tmp) - 1;
            while(end >= tmp && (*end == ' ' || *end == '\0')) { *end = '\0'; end--; }

            if (c == 0) strcpy(target_ips, tmp);
            else if (c == 1) strcpy(params->query_id, tmp);
            else if (c == 2) strcpy(params->security_token, tmp);
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

    /* Select IP based on AMP ID for load balancing */
    INTEGER amp_id = 0; FNC_TblOpGetUniqID(&amp_id);
    char *ips[128]; int ip_count = 0;
    char *saveptr;
    char *token = strtok_r(target_ips, ",", &saveptr);
    while (token && ip_count < 128) {
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
}

static int write_hex_string(unsigned char *buf, void *value, int bytesize) {
    char hex[] = "0123456789ABCDEF";
    unsigned char *p = (unsigned char*)value;
    /* Limit hex string to avoid internal buffer overflows in batch */
    int len = bytesize * 2;
    if (len > 32767) len = 32767; 
    write_uint16(buf, (unsigned short)len);
    for (int i = 0; i < len/2; i++) {
        buf[2 + i*2] = hex[(p[i] >> 4) & 0xF];
        buf[2 + i*2 + 1] = hex[p[i] & 0xF];
    }
    return 2 + len;
}

static int write_decimal_as_string(unsigned char *buf, void *value, int bytesize, int scale) {
    char str[100], tmp[64];
    int len = 0, i = 0;
    unsigned __int128 abs_val; __int128 val = 0;
    if (bytesize == 1) val = *(__int8_t*)value;
    else if (bytesize == 2) val = *(__int16_t*)value;
    else if (bytesize == 4) val = *(__int32_t*)value;
    else if (bytesize == 8) val = *(__int64_t*)value;
    else if (bytesize >= 16) memcpy(&val, value, 16);
    if (val < 0) { str[len++] = '-'; abs_val = (unsigned __int128)(-val); } else abs_val = (unsigned __int128)val;
    if (abs_val == 0) tmp[i++] = '0';
    while (abs_val > 0) { tmp[i++] = (char)(abs_val % 10 + '0'); abs_val /= 10; }
    while (i <= scale) tmp[i++] = '0';
    for (int j = 0; j < i; j++) {
        if (i - j == scale && scale > 0) str[len++] = '.';
        str[len++] = tmp[i - 1 - j];
    }
    str[len] = '\0';
    write_uint16(buf, (short)len); memcpy(buf + 2, str, len);
    return 2 + len;
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
    /* Only set format for the primary data stream (0) and output stream */
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
    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr; memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET; addr.sin_port = htons(params.bridge_port);
    inet_pton(AF_INET, params.bridge_host, &addr.sin_addr);
    if (connect(sock_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        stats.error_code = errno; snprintf(stats.error_message, 250, "Connect to %s:%d failed", params.bridge_host, params.bridge_port); 
        goto send_status;
    }

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
            case TD_CHAR: case TD_VARCHAR: tn = "VARCHAR"; break;
            case TD_INTEGER: case TD_SMALLINT: case TD_BYTEINT: tn = "INTEGER"; break;
            case TD_BIGINT: tn = "BIGINT"; break;
            case TD_FLOAT: tn = "DOUBLE"; break;
            default: tn = "VARCHAR"; break;
        }
        snprintf(cd, 256, "%s{\"name\":\"col_%d\",\"type\":\"%s\"}", col > 0 ? "," : "", col, tn); strcat(sj, cd);
    }
    strcat(sj, "]}"); int sj_len = strlen(sj);
    ho += write_uint32(ph + ho, sj_len);
    if (send(sock_fd, ph, ho, 0) < 0 || send(sock_fd, sj, sj_len, 0) < 0) {
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
                
                if (dt == TD_VARCHAR) {
                    short blen = *(short*)val;
                    if (cs == 2 || cs == 6) batch_offset += write_unicode_to_utf8(bb + batch_offset, (unsigned char*)val + 2, blen);
                    else {
                        write_uint16(bb + batch_offset, blen); memcpy(bb + batch_offset + 2, (char*)val + 2, blen);
                        batch_offset += 2 + blen;
                    }
                } else if (dt == TD_CHAR) {
                    int blen = iCols->column_types[col].bytesize;
                    if (cs == 2 || cs == 6) batch_offset += write_unicode_to_utf8(bb + batch_offset, (unsigned char*)val, blen);
                    else {
                        write_uint16(bb + batch_offset, (unsigned short)blen); memcpy(bb + batch_offset + 2, (char*)val, blen);
                        batch_offset += 2 + blen;
                    }
                } else if (dt == TD_INTEGER) batch_offset += write_int32(bb + batch_offset, *(int*)val);
                else if (dt == TD_BIGINT) batch_offset += write_int64(bb + batch_offset, *(long long*)val);
                else if (dt == TD_SMALLINT) batch_offset += write_int32(bb + batch_offset, (int)*(short*)val);
                else if (dt == TD_BYTEINT) batch_offset += write_int32(bb + batch_offset, (int)*(__int8_t*)val);
                else if (dt == TD_FLOAT) {
                    long long lv; memcpy(&lv, val, 8);
                    batch_offset += write_int64(bb + batch_offset, lv);
                } else if (dt == TD_DATE) {
                    int d = *(int*)val;
                    int y_off = d / 10000;
                    int md = d % 10000;
                    if (md < 0) { y_off--; md += 10000; }
                    int year = y_off + 1900;
                    int month = md / 100;
                    int day = md % 100;
                    char ds[16]; int l = sprintf(ds, "%04d-%02d-%02d", year, month, day);
                    write_uint16(bb + batch_offset, (short)l); memcpy(bb + batch_offset + 2, ds, l); batch_offset += 2 + l;
                } else if (dt == TD_TIME) {
                    unsigned int s_scaled; memcpy(&s_scaled, val, 4);
                    unsigned char hour = ((unsigned char*)val)[4], min = ((unsigned char*)val)[5];
                    double sec = s_scaled / 1000000.0;
                    char ts[32]; int l = sprintf(ts, "%02d:%02d:%09.6f", hour % 24, min % 60, sec);
                    write_uint16(bb + batch_offset, (short)l); memcpy(bb + batch_offset + 2, ts, l); batch_offset += 2 + l;
                } else if (dt == TD_TIMESTAMP) {
                    unsigned int s_scaled; memcpy(&s_scaled, val, 4);
                    unsigned short year; memcpy(&year, (char*)val + 4, 2);
                    unsigned char mon = ((unsigned char*)val)[6], day = ((unsigned char*)val)[7], hour = ((unsigned char*)val)[8], min = ((unsigned char*)val)[9];
                    double sec = s_scaled / 1000000.0;
                    char tss[64]; int l = sprintf(tss, "%04d-%02d-%02d %02d:%02d:%09.6f", year, mon, day, hour, min, sec);
                    write_uint16(bb + batch_offset, (short)l); memcpy(bb + batch_offset + 2, tss, l); batch_offset += 2 + l;
                } else if (dt == TD_DECIMAL) {
                    batch_offset += write_decimal_as_string(bb + batch_offset, val, iCols->column_types[col].bytesize, iCols->column_types[col].size.range.fracdigit);
                } else {
                    batch_offset += write_hex_string(bb + batch_offset, val, iCols->column_types[col].bytesize);
                }
            }
        }
        /* Safety check: ensure we don't overflow bb even with wide rows. 
           Max Teradata row is 1MB, so we check for 1MB safety margin. */
        if (rows_in_batch >= params.batch_size || batch_offset > BUFFER_SIZE - 1048576) {
            write_uint32(bb, rows_in_batch); unsigned char lb[4]; write_uint32(lb, batch_offset);
            if (send(sock_fd, lb, 4, 0) < 0 || send(sock_fd, bb, batch_offset, 0) < 0) {
                stats.error_code = 1004; strcpy(stats.error_message, "Batch send failed"); break;
            }
            stats.batches_sent++; stats.bytes_sent += batch_offset;
            batch_offset = 4; rows_in_batch = 0;
        }
    }
    if (rows_in_batch > 0 && stats.error_code == 0) {
        write_uint32(bb, rows_in_batch); unsigned char lb[4]; write_uint32(lb, batch_offset);
        send(sock_fd, lb, 4, 0); send(sock_fd, bb, batch_offset, 0); 
        stats.batches_sent++; stats.bytes_sent += batch_offset;
    }
    unsigned char emsg[4] = {0,0,0,0}; send(sock_fd, emsg, 4, 0); 

send_status:
    if (sock_fd >= 0) close(sock_fd);
    static INTEGER ra; static BIGINT rr, rb, rn, rba; static INTEGER rc; static char rs[300];
    ra = 0; FNC_TblOpGetUniqID(&ra);
    rr = stats.rows_processed; rb = stats.bytes_sent; rn = stats.null_count; rba = stats.batches_sent; rc = tic;
    int char_len;
    if (stats.error_code == 0) {
        char_len = snprintf(rs + 2, 256, "[%s:%d] SUCCESS (Query: %s)", params.bridge_host, params.bridge_port, params.query_id);
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
    if (in) FNC_TblOpClose(in);
    if (param_in) FNC_TblOpClose(param_in);
}
