/**
 * ExportToTrino Contract Function  
 * 
 * This file contains ONLY the contract function for the Table Operator.
 */

#define SQL_TEXT Latin_Text
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "sqltypes_td.h"

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

    (void)extname;
    (void)specific_name;

    FNC_TblOpGetStreamCount(&incount, &outcount);
    
    if (incount == 0) {
        SetError("U0001", "ExportToTrino requires at least one input stream.");
        *Result = -1;
        return;
    }

    icolinfo = (InputInfo_t *)FNC_malloc(incount * sizeof(InputInfo_t));
    
    totalcols = 0;
    for (i = 0; i < incount; i++) {
        icolinfo[i].colcount = FNC_TblOpGetColCount(i, ISINPUT);
        totalcols += icolinfo[i].colcount;
        
        icolinfo[i].iCols = (FNC_TblOpColumnDef_t *)FNC_malloc(TblOpSIZECOLDEF(icolinfo[i].colcount));
        TblOpINITCOLDEF(icolinfo[i].iCols, icolinfo[i].colcount);
        FNC_TblOpGetColDef(i, ISINPUT, icolinfo[i].iCols);
    }

    /* Output schema: (amp_id INTEGER, rows_sent BIGINT, bytes_sent BIGINT, status VARCHAR(100)) */
    oCols = (FNC_TblOpColumnDef_t *)FNC_malloc(TblOpSIZECOLDEF(4));
    memset(oCols, 0, TblOpSIZECOLDEF(4));
    oCols->num_columns = 4;
    oCols->length = TblOpSIZECOLDEF(4) - (2 * sizeof(int));
    TblOpINITCOLDEF(oCols, 4);

    oCols->column_types[0].datatype = INTEGER_DT;
    oCols->column_types[0].size.length = 4;
    oCols->column_types[0].bytesize = 4;

    oCols->column_types[1].datatype = BIGINT_DT;
    oCols->column_types[1].size.length = 8;
    oCols->column_types[1].bytesize = 8;

    oCols->column_types[2].datatype = BIGINT_DT;
    oCols->column_types[2].size.length = 8;
    oCols->column_types[2].bytesize = 8;

    oCols->column_types[3].datatype = VARCHAR_DT;
    oCols->column_types[3].size.length = 100;
    oCols->column_types[3].charset = LATIN_CT;
    oCols->column_types[3].bytesize = 102;

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
