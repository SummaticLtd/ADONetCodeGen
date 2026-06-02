-- A nullable scalar UDF: the parameter has a NULL default, so the generator treats
-- the input as an `int voption`. Scalar returns are always nullable, so passing NULL
-- (ValueNone) in and getting NULL (ValueNone) out exercises a nullable input and a
-- nullable output together.
CREATE FUNCTION [dbo].[EchoNullableInt] (@value int = NULL) RETURNS int
AS BEGIN RETURN @value END
