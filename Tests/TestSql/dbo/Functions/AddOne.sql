CREATE FUNCTION [dbo].[AddOne]
(
    @n int
)
RETURNS int
AS
BEGIN
    RETURN @n + 1
END
