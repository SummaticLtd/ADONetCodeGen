-- A non-query stored procedure: inserts a row. No SELECT, so the generator produces a
-- StoredProcNonQuery whose Execute returns the number of rows affected. NOCOUNT is left
-- off so that count is reported.
CREATE PROCEDURE [dbo].[InsertWidget]
    @name  nvarchar(50),
    @price decimal(10, 2)
AS
BEGIN
    INSERT INTO [dbo].[Widget] ([Name], [Price]) VALUES (@name, @price)
END
