-- A non-query stored procedure that takes 2 s, for testing command timeouts.
CREATE PROCEDURE [dbo].[WaitTwoSeconds]
AS
BEGIN
    SET NOCOUNT ON
    WAITFOR DELAY '00:00:02'
END
