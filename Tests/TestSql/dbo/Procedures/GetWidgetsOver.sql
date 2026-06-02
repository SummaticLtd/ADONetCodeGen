-- A query stored procedure: returns a result set. The generator discovers the result
-- columns at design time (SchemaOnly) and produces a StoredProcQuery with an Output type.
CREATE PROCEDURE [dbo].[GetWidgetsOver]
    @minPrice decimal(10, 2)
AS
BEGIN
    SET NOCOUNT ON
    SELECT [Id], [Name], [Price]
    FROM [dbo].[Widget]
    WHERE [Price] >= @minPrice
    ORDER BY [Id]
END
