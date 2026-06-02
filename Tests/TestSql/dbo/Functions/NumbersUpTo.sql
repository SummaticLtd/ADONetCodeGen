-- A table-valued (non-scalar) UDF: takes a parameter and returns a variable number of
-- rows with several typed columns, including a nullable one. Exercises the TableUDF
-- code path, the generated Output type, and reading a NULL result column as ValueNone.
CREATE FUNCTION [dbo].[NumbersUpTo] (@count int)
RETURNS @result TABLE
(
    [Value]  int          NOT NULL,
    [Square] bigint       NOT NULL,
    [Label]  nvarchar(20) NOT NULL,
    [Maybe]  int          NULL
)
AS
BEGIN
    DECLARE @i int = 1
    WHILE @i <= @count
    BEGIN
        INSERT INTO @result ([Value], [Square], [Label], [Maybe])
        VALUES (@i, CAST(@i AS bigint) * @i, CONCAT(N'n', @i), CASE WHEN @i % 2 = 0 THEN NULL ELSE @i * 10 END)
        SET @i = @i + 1
    END
    RETURN
END
