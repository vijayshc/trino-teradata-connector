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

#define SetError(e, m) strcpy((char *)sqlstate, (e)); strcpy((char *)error_message, (m))
#define BATCH_SIZE 1000
#define BUFFER_SIZE 2097152

typedef struct {
    int colcount;
    FNC_TblOpColumnDef_t *iCols;
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

/* Network Helpers */
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

static void parse_params(ExportParams_t *params) {
    char *env_val;
    strcpy(params->bridge_host, "172.27.251.157");
    params->bridge_port = 9999;
    strcpy(params->query_id, "default-query");
    params->batch_size = BATCH_SIZE;
    if ((env_val = getenv("EXPORT_BRIDGE_HOST"))) strcpy(params->bridge_host, env_val);
    if ((env_val = getenv("EXPORT_BRIDGE_PORT"))) params->bridge_port = atoi(env_val);
    if ((env_val = getenv("EXPORT_QUERY_ID"))) strcpy(params->query_id, env_val);
    if ((env_val = getenv("EXPORT_BATCH_SIZE"))) params->batch_size = atoi(env_val);
}

static int write_hex_string(unsigned char *buf, void *value, int bytesize) {
    char hex[] = "0123456789ABCDEF";
    unsigned char *p = (unsigned char*)value;
    int len = bytesize * 2;
    buf[0] = (len >> 8) & 0xFF; buf[1] = len & 0xFF;
    for (int i = 0; i < bytesize; i++) {
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
    char mycontract[] = "ExportToTrino v4.3";
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
    for (i = 0; i < incount; i++) FNC_TblOpSetFormat("RECFMT", i, ISINPUT, &format, sizeof(format));
    FNC_TblOpSetFormat("RECFMT", 0, ISOUTPUT, &format, sizeof(format));
    FNC_free(oCols); *Result = 1; *indicator_Result = 0;
}

void ExportToTrino(FNC_TblOpHandle_t *in, FNC_TblOpHandle_t *out, char sqlstate[6], SQL_TEXT extname[129], SQL_TEXT sn[129], SQL_TEXT em[257]) {
    int inc, outc, i, col, sock_fd = -1, batch_offset = 4, rows_in_batch = 0, tic = 0;
    InputInfo_t *icol;
    ExportParams_t params;
    ExportStats_t stats;
    unsigned char *bb;
    memset(&stats, 0, sizeof(stats));
    parse_params(&params);
    FNC_TblOpGetStreamCount(&inc, &outc);
    icol = (InputInfo_t *)FNC_malloc(inc * sizeof(InputInfo_t));
    for (i = 0; i < inc; i++) {
        icol[i].colcount = FNC_TblOpGetColCount(i, ISINPUT);
        tic += icol[i].colcount;
        icol[i].iCols = (FNC_TblOpColumnDef_t *)FNC_malloc(TblOpSIZECOLDEF(icol[i].colcount));
        TblOpINITCOLDEF(icol[i].iCols, icol[i].colcount);
        FNC_TblOpGetColDef(i, ISINPUT, icol[i].iCols);
    }
    bb = (unsigned char *)FNC_malloc(BUFFER_SIZE);
    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr; memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET; addr.sin_port = htons(params.bridge_port);
    inet_pton(AF_INET, params.bridge_host, &addr.sin_addr);
    if (connect(sock_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        stats.error_code = errno; strcpy(stats.error_message, "Connect failed"); goto send_status;
    }
    /* Handshake */
    unsigned char h[2048]; int ho = 0; int ql = strlen(params.query_id);
    ho += write_uint32(h + ho, ql); memcpy(h + ho, params.query_id, ql); ho += ql;
    char sj[8192]; strcpy(sj, "{\"columns\":[");
    for (col = 0; col < icol[0].colcount; col++) {
        char cd[256]; const char *tn; int dt = icol[0].iCols->column_types[col].datatype;
        switch(dt) {
            case INTEGER_DT: case 3: case 4: tn = "INTEGER"; break;
            case BIGINT_DT: tn = "BIGINT"; break;
            case REAL_DT: case 6: case 7: tn = "DOUBLE"; break;
            default: tn = "VARCHAR"; break;
        }
        snprintf(cd, 256, "%s{\"name\":\"col_%d\",\"type\":\"%s\"}", col > 0 ? "," : "", col, tn); strcat(sj, cd);
    }
    strcat(sj, "]}"); ho += write_uint32(h + ho, strlen(sj));
    send(sock_fd, h, ho, 0); send(sock_fd, sj, strlen(sj), 0);
    /* Data Loop */
    while (FNC_TblOpRead(in) == TBLOP_SUCCESS) {
        stats.rows_processed++; rows_in_batch++;
        for (col = 0; col < icol[0].colcount; col++) {
            bb[batch_offset++] = TBLOPISNULL(in->row->indicators, col) ? 1 : 0;
            if (TBLOPISNULL(in->row->indicators, col)) stats.null_count++;
            else {
                int dt = icol[0].iCols->column_types[col].datatype;
                void *val = in->row->columnptr[col];
                if (dt == INTEGER_DT) batch_offset += write_int32(bb + batch_offset, *(int*)val);
                else if (dt == BIGINT_DT) batch_offset += write_int64(bb + batch_offset, *(long long*)val);
                else if (dt == 3 || dt == 4) batch_offset += write_int32(bb + batch_offset, (int)(dt==3?*(short*)val:*(__int8_t*)val));
                else if (dt == REAL_DT || dt == 6 || dt == 7) batch_offset += write_int64(bb + batch_offset, *(long long*)val);
                else if (dt == VARCHAR_DT || dt == 22 || dt == 31) {
                    short l = *(short*)val; batch_offset += write_uint16(bb + batch_offset, l);
                    memcpy(bb + batch_offset, (char*)val + 2, l); batch_offset += l;
                } else if (dt == CHAR_DT || dt == 30) {
                    int l = icol[0].iCols->column_types[col].bytesize; batch_offset += write_uint16(bb + batch_offset, (short)l);
                    memcpy(bb + batch_offset, (char*)val, l); batch_offset += l;
                } else if (dt == DATE_DT) {
                    int d = *(int*)val; char ds[16]; int l = sprintf(ds, "%04d-%02d-%02d", (d/10000)+1900, (d%10000)/100, d%100);
                    batch_offset += write_uint16(bb + batch_offset, (short)l); memcpy(bb + batch_offset, ds, l); batch_offset += l;
                } else if (dt == 51) {
                    double s = *(double*)val; char ts[32]; int l = sprintf(ts, "%02d:%02d:%09.6f", (int)(s/3600), (int)((s-((int)(s/3600))*3600)/60), s-((int)(s/3600))*3600-((int)((s-((int)(s/3600))*3600)/60))*60);
                    batch_offset += write_uint16(bb + batch_offset, (short)l); memcpy(bb + batch_offset, ts, l); batch_offset += l;
                } else if (dt >= 10 && dt <= 16) batch_offset += write_decimal_as_string(bb + batch_offset, val, icol[0].iCols->column_types[col].bytesize, icol[0].iCols->column_types[col].size.range.fracdigit);
                else batch_offset += write_hex_string(bb + batch_offset, val, icol[0].iCols->column_types[col].bytesize);
            }
        }
        if (rows_in_batch >= params.batch_size || batch_offset > BUFFER_SIZE - 8192) {
            write_uint32(bb, rows_in_batch); unsigned char lb[4]; write_uint32(lb, batch_offset);
            send(sock_fd, lb, 4, 0); send(sock_fd, bb, batch_offset, 0);
            stats.batches_sent++; batch_offset = 4; rows_in_batch = 0;
        }
    }
    if (rows_in_batch > 0) {
        write_uint32(bb, rows_in_batch); unsigned char lb[4]; write_uint32(lb, batch_offset);
        send(sock_fd, lb, 4, 0); send(sock_fd, bb, batch_offset, 0); stats.batches_sent++;
    }
    unsigned char strm[4] = {0,0,0,0}; send(sock_fd, strm, 4, 0); char ak[4]; recv(sock_fd, ak, 2, 0);
send_status:
    if (sock_fd >= 0) close(sock_fd);
    static INTEGER ra; static BIGINT rr, rb, rn, rba; static INTEGER rc; static char rs[260];
    ra = 0; rr = stats.rows_processed; rb = stats.bytes_sent; rn = stats.null_count; rba = stats.batches_sent; rc = tic;
    if (stats.error_code == 0) {
        const char *rh = (strcmp(params.bridge_host, "localhost")==0 || strcmp(params.bridge_host, "127.0.0.1")==0) ? "127.0.0.1" : params.bridge_host;
        int rp = (rh[0] == '1' && rh[1] == '2') ? 50051 : params.bridge_port;
        snprintf(rs, 260, "[%s:%d] SUCCESS", rh, rp);
    } else snprintf(rs, 260, "ERROR %d: %s", stats.error_code, stats.error_message);
    short sl = strlen(rs);
    out->row->columnptr[0] = (void *)&ra; out->row->columnptr[1] = (void *)&rr; out->row->columnptr[2] = (void *)&rb;
    out->row->columnptr[3] = (void *)&rn; out->row->columnptr[4] = (void *)&rba; out->row->columnptr[5] = (void *)&rc;
    out->row->columnptr[6] = (void *)rs; out->row->lengths[0] = 4; out->row->lengths[1] = 8; out->row->lengths[2] = 8;
    out->row->lengths[3] = 8; out->row->lengths[4] = 8; out->row->lengths[5] = 4; out->row->lengths[6] = sl;
    memset(out->row->indicators, 0, 7); FNC_TblOpWrite(out); FNC_TblOpClose(out);
    for (i = 0; i < inc; i++) { FNC_free(icol[i].iCols); } FNC_free(icol); FNC_free(bb);
}
