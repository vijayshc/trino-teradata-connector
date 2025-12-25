-- ============================================================
-- Teradata Table Operator Registration: ExportToTrino
-- ============================================================
-- 
-- WORKING REGISTRATION SYNTAX - Verified on Teradata 20.00.22.31
-- 
-- This script registers the ExportToTrino Table Operator.
-- Both contract and execution functions are in the same source file.
--
-- Reference: Teradata SQL External Routine Programming
-- ============================================================

-- Create/Use the TrinoExport database
DATABASE TrinoExport;

-- Grant necessary permissions
GRANT CREATE FUNCTION ON TrinoExport TO dbc;

-- ============================================================
-- Register the Table Operator (Combined Source File)
-- ============================================================
-- The contract function (ExportToTrino_contract) is auto-discovered
-- from the source file via the USING FUNCTION clause.

REPLACE FUNCTION ExportToTrino()
RETURNS TABLE VARYING USING FUNCTION ExportToTrino_contract
SPECIFIC ExportToTrino
LANGUAGE C
NO SQL
NO EXTERNAL DATA
PARAMETER STYLE SQLTable
NOT DETERMINISTIC
CALLED ON NULL INPUT
EXTERNAL NAME 'CS!ExportToTrino!src/teradata/export_to_trino.c!F!ExportToTrino';

-- ============================================================
-- Verification
-- ============================================================
SELECT FunctionName, FunctionType, ParameterStyle 
FROM DBC.FunctionsV 
WHERE FunctionName LIKE 'EXPORT%';

-- ============================================================
-- Usage Example (Working Syntax)
-- ============================================================
-- SELECT * FROM ExportToTrino(
--     ON (SELECT * FROM MyDatabase.LargeTable)
-- ) AS export_result;
--
-- Output columns:
-- | Column_1 (amp_id) | Column_2 (rows_sent) | Column_3 (bytes_sent) | Column_4 (status) |
-- |-------------------|----------------------|-----------------------|-------------------|
-- | 0                 | 125000               | 5000000               | SUCCESS           |
-- | 1                 | 124500               | 4980000               | SUCCESS           |
