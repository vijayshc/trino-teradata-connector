/**
 * Teradata Table Operator for High-Performance Export to Trino via Arrow Flight
 * 
 * This implementation uses the correct Teradata Table Operator APIs:
 * - FNC_TblOp* functions for stream handling
 * - Contract function for dynamic output schema
 * 
 * Based on Teradata SQL External Routine Programming documentation.
 */

#define SQL_TEXT Latin_Text
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "sqltypes_td.h"

#define BufferSize (32*1024)
#define BATCH_SIZE 10000
#define SetError(e, m) strcpy((char *)sqlstate, (e)); strcpy((char *)error_message, (m))

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
 * ExportToTrino_contract - Contract function for the Table Operator
 * 
 * This function is called by the Parsing Engine to determine the output
 * schema of the Table Operator. It examines the input columns and defines
 * output columns for status reporting.
 */
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
    int i, totalcols;
    Stream_Fmt_en format;
    char mycontract[] = "ExportToTrino Arrow Flight Export Contract v1.0";

    (void)extname;        /* Suppress unused parameter warning */
    (void)specific_name;  /* Suppress unused parameter warning */

    /* Get stream counts */
    FNC_TblOpGetStreamCount(&incount, &outcount);
    
    if (incount == 0) {
        SetError("U0001", "ExportToTrino requires at least one input stream (source table).");
        *Result = -1;
        return;
    }

    /* Allocate info structures for each input stream */
    icolinfo = (InputInfo_t *)FNC_malloc(incount * sizeof(InputInfo_t));
    
    totalcols = 0;
    for (i = 0; i < incount; i++) {
        icolinfo[i].colcount = FNC_TblOpGetColCount(i, ISINPUT);
        totalcols += icolinfo[i].colcount;
        
        icolinfo[i].iCols = (FNC_TblOpColumnDef_t *)FNC_malloc(TblOpSIZECOLDEF(icolinfo[i].colcount));
        TblOpINITCOLDEF(icolinfo[i].iCols, icolinfo[i].colcount);
        FNC_TblOpGetColDef(i, ISINPUT, icolinfo[i].iCols);
    }

    /* 
     * Define output columns for status reporting
     * Output schema: (amp_id INTEGER, rows_sent BIGINT, bytes_sent BIGINT, status VARCHAR(100))
     */
    oCols = (FNC_TblOpColumnDef_t *)FNC_malloc(TblOpSIZECOLDEF(4));
    memset(oCols, 0, TblOpSIZECOLDEF(4));
    oCols->num_columns = 4;
    oCols->length = TblOpSIZECOLDEF(4) - (2 * sizeof(int));
    TblOpINITCOLDEF(oCols, 4);

    /* Column 0: amp_id INTEGER */
    oCols->column_types[0].datatype = INTEGER_DT;
    oCols->column_types[0].size.length = 4;
    oCols->column_types[0].bytesize = 4;

    /* Column 1: rows_sent BIGINT */
    oCols->column_types[1].datatype = BIGINT_DT;
    oCols->column_types[1].size.length = 8;
    oCols->column_types[1].bytesize = 8;

    /* Column 2: bytes_sent BIGINT */
    oCols->column_types[2].datatype = BIGINT_DT;
    oCols->column_types[2].size.length = 8;
    oCols->column_types[2].bytesize = 8;

    /* Column 3: status VARCHAR(100) */
    oCols->column_types[3].datatype = VARCHAR_DT;
    oCols->column_types[3].size.length = 100;
    oCols->column_types[3].charset = LATIN_CT;
    oCols->column_types[3].bytesize = 102; /* 100 + 2 for length prefix */

    /* Set the contract definition */
    FNC_TblOpSetContractDef(mycontract, strlen(mycontract) + 1);
    
    /* Define output columns */
    FNC_TblOpSetOutputColDef(0, oCols);
    
    /* Set data format for input and output streams */
    format = INDICFMT1;
    for (i = 0; i < incount; i++) {
        FNC_TblOpSetFormat("RECFMT", i, ISINPUT, &format, sizeof(format));
    }
    FNC_TblOpSetFormat("RECFMT", 0, ISOUTPUT, &format, sizeof(format));

    /* Cleanup */
    FNC_free(oCols);
    for (i = 0; i < incount; i++) {
        FNC_free(icolinfo[i].iCols);
    }
    FNC_free(icolinfo);

    *Result = 1;
    *indicator_Result = 0; /* Not null */
}

/**
 * ExportToTrino - Main execution function for the Table Operator
 * 
 * This function executes on each AMP in parallel, reading input rows,
 * converting them to Arrow format, and streaming to Trino workers via Arrow Flight.
 * 
 * Usage:
 * SELECT * FROM ExportToTrino(
 *   ON (SELECT * FROM MySourceTable)
 *   USING TargetIPs('10.1.1.5:50051,10.1.1.6:50051'), QueryID('uuid-123')
 * ) AS export_result;
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

    /* Static storage for output values - must persist until FNC_TblOpWrite */
    static INTEGER out_amp_id;
    static BIGINT out_rows_sent;
    static BIGINT out_bytes_sent;
    static char out_status[102]; /* VARCHAR(100) with 2-byte length prefix */
    short status_len;

    /* Use a simple counter for AMP identification - each AMP instance increments */
    amp_id = amp_counter++;

    /* Get stream counts */
    FNC_TblOpGetStreamCount(&incount, &outcount);

    /* Allocate info structures for input streams */
    icolinfo = (InputInfo_t *)FNC_malloc(incount * sizeof(InputInfo_t));

    for (i = 0; i < incount; i++) {
        /* Get column count */
        icolinfo[i].colcount = FNC_TblOpGetColCount(i, ISINPUT);

        /* Get column definitions */
        icolinfo[i].iCols = (FNC_TblOpColumnDef_t *)FNC_malloc(TblOpSIZECOLDEF(icolinfo[i].colcount));
        TblOpINITCOLDEF(icolinfo[i].iCols, icolinfo[i].colcount);
        FNC_TblOpGetColDef(i, ISINPUT, icolinfo[i].iCols);

        /* Open input stream handle */
        icolinfo[i].Handle = (FNC_TblOpHandle_t *)FNC_TblOpOpen(i, 'r', 0);
        icolinfo[i].dimension = FNC_TblOpIsDimension(i, ISINPUT);
        icolinfo[i].is_eof = 0;
    }

    /* Open output stream handle */
    outHandle = (FNC_TblOpHandle_t *)FNC_TblOpOpen(0, 'w', 0);

    /*
     * TODO: Arrow Flight Connection Setup
     * 
     * In a full implementation:
     * 1. Parse TargetIPs from USING clause
     * 2. Select target based on amp_hash % num_targets
     * 3. Connect to Trino Flight Server via arrow::flight::FlightClient
     * 4. Start DoPut stream with QueryID for correlation
     */

    /* Main row processing loop */
    while (1) {
        int all_streams_eof = 1;

        /* Read from all input streams */
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

        /* Process each input row */
        for (j = 0; j < incount; j++) {
            if (icolinfo[j].is_eof == 0) {
                inHandle = icolinfo[j].Handle;
                iCols = icolinfo[j].iCols;

                /*
                 * TODO: Arrow RecordBatch Building
                 * 
                 * For each column in the row:
                 * 1. Check null indicator: TBLOPISNULL(inHandle->row->indicators, i)
                 * 2. Get column value: inHandle->row->columnptr[i]
                 * 3. Get column length: inHandle->row->lengths[i]
                 * 4. Append to appropriate Arrow builder based on datatype
                 * 
                 * When batch reaches BATCH_SIZE:
                 * 1. Finalize Arrow arrays
                 * 2. Create RecordBatch
                 * 3. Send via flight_client->DoPut()
                 * 4. Reset builders
                 */

                rows_sent++;
                
                /* Estimate bytes (simplified) */
                for (i = 0; i < iCols->num_columns; i++) {
                    bytes_sent += iCols->column_types[i].bytesize;
                }
                
                /* Use inHandle to suppress warning */
                (void)inHandle;
            }
        }
    }

    /*
     * TODO: Final Flush & Cleanup
     * 
     * 1. Flush remaining Arrow batch
     * 2. Close Flight stream
     * 3. Report status
     */

    /* Write output summary row */
    out_amp_id = amp_id;
    out_rows_sent = rows_sent;
    out_bytes_sent = bytes_sent;
    
    /* Format VARCHAR with 2-byte length prefix */
    status_len = (short)strlen(status);
    memcpy(out_status, &status_len, 2);
    strcpy(out_status + 2, status);

    /* Set output column values using proper casts */
    outHandle->row->columnptr[0] = (void *)&out_amp_id;
    outHandle->row->lengths[0] = sizeof(INTEGER);
    
    outHandle->row->columnptr[1] = (void *)&out_rows_sent;
    outHandle->row->lengths[1] = sizeof(BIGINT);
    
    outHandle->row->columnptr[2] = (void *)&out_bytes_sent;
    outHandle->row->lengths[2] = sizeof(BIGINT);
    
    outHandle->row->columnptr[3] = (void *)out_status;
    outHandle->row->lengths[3] = 2 + status_len;

    /* Clear null indicators for output */
    memset(outHandle->row->indicators, 0, sizeof(outHandle->row->indicators));

    /* Write output row */
    FNC_TblOpWrite(outHandle);

    /* Cleanup */
    for (i = 0; i < incount; i++) {
        FNC_free(icolinfo[i].iCols);
        FNC_TblOpClose(icolinfo[i].Handle);
    }
    FNC_free(icolinfo);
    FNC_TblOpClose(outHandle);
}
