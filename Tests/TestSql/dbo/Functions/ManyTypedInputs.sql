-- Takes many parameters of different types in a single command, and returns a bit
-- indicating whether every input arrived with its expected value. This exercises a
-- generated Input record with many fields of mixed SqlDbType, and their ordering.
CREATE FUNCTION [dbo].[ManyTypedInputs]
(
    @theByte    tinyint,
    @theInt     int,
    @theBigInt  bigint,
    @theBit     bit,
    @theDouble  float,
    @theDecimal decimal(18, 4),
    @theGuid    uniqueidentifier,
    @theText    nvarchar(50),
    @theTime    time(7),
    @theStamp   datetime2(7)
)
RETURNS bit
AS
BEGIN
    RETURN CASE WHEN
            @theByte    = 200
        AND @theInt     = 1234567
        AND @theBigInt  = 9876543210
        AND @theBit     = 1
        AND @theDouble  = 3.141592653589793
        AND @theDecimal = 12345.6789
        AND @theGuid    = '12345678-1234-1234-1234-1234567890AB'
        AND @theText    = N'Hello'
        AND @theTime    = '13:45:30.1230000'
        AND @theStamp   = '2024-06-01T13:45:30.1230000'
        THEN 1 ELSE 0 END
END
