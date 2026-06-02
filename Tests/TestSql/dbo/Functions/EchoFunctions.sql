-- Round-trip ("echo") scalar UDFs: each takes a value of one SQL type and returns
-- it unchanged. Together they exercise every .NET-mapped scalar type the generator
-- supports, as both an input parameter and a scalar return value.

CREATE FUNCTION [dbo].[EchoTinyInt] (@value tinyint) RETURNS tinyint
AS BEGIN RETURN @value END
GO

CREATE FUNCTION [dbo].[EchoSmallInt] (@value smallint) RETURNS smallint
AS BEGIN RETURN @value END
GO

CREATE FUNCTION [dbo].[EchoInt] (@value int) RETURNS int
AS BEGIN RETURN @value END
GO

CREATE FUNCTION [dbo].[EchoBigInt] (@value bigint) RETURNS bigint
AS BEGIN RETURN @value END
GO

CREATE FUNCTION [dbo].[EchoBit] (@value bit) RETURNS bit
AS BEGIN RETURN @value END
GO

CREATE FUNCTION [dbo].[EchoReal] (@value real) RETURNS real
AS BEGIN RETURN @value END
GO

CREATE FUNCTION [dbo].[EchoFloat] (@value float) RETURNS float
AS BEGIN RETURN @value END
GO

CREATE FUNCTION [dbo].[EchoDecimal] (@value decimal(18, 4)) RETURNS decimal(18, 4)
AS BEGIN RETURN @value END
GO

CREATE FUNCTION [dbo].[EchoUniqueIdentifier] (@value uniqueidentifier) RETURNS uniqueidentifier
AS BEGIN RETURN @value END
GO

CREATE FUNCTION [dbo].[EchoNVarChar] (@value nvarchar(100)) RETURNS nvarchar(100)
AS BEGIN RETURN @value END
GO

CREATE FUNCTION [dbo].[EchoVarBinary] (@value varbinary(100)) RETURNS varbinary(100)
AS BEGIN RETURN @value END
GO

CREATE FUNCTION [dbo].[EchoDateTime2] (@value datetime2(7)) RETURNS datetime2(7)
AS BEGIN RETURN @value END
GO

CREATE FUNCTION [dbo].[EchoDateTimeOffset] (@value datetimeoffset(7)) RETURNS datetimeoffset(7)
AS BEGIN RETURN @value END
GO

CREATE FUNCTION [dbo].[EchoTime] (@value time(7)) RETURNS time(7)
AS BEGIN RETURN @value END
GO
