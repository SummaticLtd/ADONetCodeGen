-- A small table so we can test stored procedures (insert/query) and the TableGetter path.
CREATE TABLE [dbo].[Widget]
(
    [Id]    int           IDENTITY(1, 1) NOT NULL CONSTRAINT [PK_Widget] PRIMARY KEY,
    [Name]  nvarchar(50)  NOT NULL,
    [Price] decimal(10, 2) NOT NULL
)
