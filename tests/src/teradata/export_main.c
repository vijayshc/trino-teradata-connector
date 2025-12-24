/**
 * ExportToTrino Execution Function
 * 
 * This file contains ONLY the main execution function for the Table Operator.
 */

#define SQL_TEXT Latin_Text
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "sqltypes_td.h"

#define BATCH_SIZE 10000

/**
 * InputInfo_t - Struct to hold input stream information
 */
typedef struct {
    int colcount;
    FNC_TblOpColumnDef_t *iCols;
    FNC_TblOpHandle_t *Handle;
    int is_eof;
    int dimension;
} InputInfo_t;

/**
 * ExportToTrino - Main execution function for the Table Operator
 */
void ExportToTrino(void)
{
    int i, j;
    FNC_TblOpColumnDef_t *iCols;
    FNC_TblOpHandle_t *inHandle, *outHandle;
    int incount, outcount;
    InputInfo_t *icolinfo;
    int result;
    static int amp_counter = 0;
    int amp_id;
    BIGINT rows_sent = 0;
    BIGINT bytes_sent = 0;
    char status[100] = "SUCCESS";

    static INTEGER out_amp_id;
    static BIGINT out_rows_sent;
    static BIGINT out_bytes_sent;
    static char out_status[102];
    short status_len;

    amp_id = amp_counter++;

    FNC_TblOpGetStreamCount(&incount, &outcount);

    icolinfo = (InputInfo_t *)FNC_malloc(incount * sizeof(InputInfo_t));

    for (i = 0; i < incount; i++) {
        icolinfo[i].colcount = FNC_TblOpGetColCount(i, ISINPUT);
        icolinfo[i].iCols = (FNC_TblOpColumnDef_t *)FNC_malloc(TblOpSIZECOLDEF(icolinfo[i].colcount));
        TblOpINITCOLDEF(icolinfo[i].iCols, icolinfo[i].colcount);
        FNC_TblOpGetColDef(i, ISINPUT, icolinfo[i].iCols);
        icolinfo[i].Handle = (FNC_TblOpHandle_t *)FNC_TblOpOpen(i, 'r', 0);
        icolinfo[i].dimension = FNC_TblOpIsDimension(i, ISINPUT);
        icolinfo[i].is_eof = 0;
    }

    outHandle = (FNC_TblOpHandle_t *)FNC_TblOpOpen(0, 'w', 0);

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

        if (all_streams_eof) {
            break;
        }

        for (j = 0; j < incount; j++) {
            if (icolinfo[j].is_eof == 0) {
                inHandle = icolinfo[j].Handle;
                iCols = icolinfo[j].iCols;

                /* TODO: Arrow RecordBatch Building and Flight DoPut */

                rows_sent++;
                
                for (i = 0; i < iCols->num_columns; i++) {
                    bytes_sent += iCols->column_types[i].bytesize;
                }
                
                (void)inHandle;
            }
        }
    }

    /* Write output summary row */
    out_amp_id = amp_id;
    out_rows_sent = rows_sent;
    out_bytes_sent = bytes_sent;
    
    status_len = (short)strlen(status);
    memcpy(out_status, &status_len, 2);
    strcpy(out_status + 2, status);

    outHandle->row->columnptr[0] = (void *)&out_amp_id;
    outHandle->row->lengths[0] = sizeof(INTEGER);
    
    outHandle->row->columnptr[1] = (void *)&out_rows_sent;
    outHandle->row->lengths[1] = sizeof(BIGINT);
    
    outHandle->row->columnptr[2] = (void *)&out_bytes_sent;
    outHandle->row->lengths[2] = sizeof(BIGINT);
    
    outHandle->row->columnptr[3] = (void *)out_status;
    outHandle->row->lengths[3] = 2 + status_len;

    memset(outHandle->row->indicators, 0, sizeof(outHandle->row->indicators));

    FNC_TblOpWrite(outHandle);

    for (i = 0; i < incount; i++) {
        FNC_free(icolinfo[i].iCols);
        FNC_TblOpClose(icolinfo[i].Handle);
    }
    FNC_free(icolinfo);
    FNC_TblOpClose(outHandle);
}
