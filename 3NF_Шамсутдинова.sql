--Task: n tables. One of them breaks 3NF. Bring the database table structure to 3NF.--
use test_3_NF

GO
create procedure statisticsOfWrites
@tableName varchar(max),
@NumberOfWrites int OUTPUT,
@NumberOfReads int OUTPUT
as
	IF OBJECT_ID('tempdb..##tempStatistics') IS NOT NULL drop table ##tempStatistics
	select
		a.name,
		sum(ius.user_scans + ius.user_seeks + ius.user_lookups) as reads,
		sum(ius.user_updates) as writes,
		max(b.last_read) as last_read,
		max(ius.last_user_update) as last_write
	into ##tempStatistics
	from
		sys.dm_db_index_usage_stats ius cross apply
		(select quotename(object_schema_name(ius.object_id, ius.database_id)) + N'.' + quotename(object_name(ius.object_id, ius.database_id))) a(name) cross apply
		(select max(t.v) from (values (ius.last_user_seek), (ius.last_user_scan), (ius.last_user_lookup)) t(v)) as b(last_read)
	where
		a.name = '[dbo].['+@tableName+']'
	group by
		a.name;
	set @NumberOfWrites = (select writes from ##tempStatistics)
	set @NumberOfReads = (select reads from ##tempStatistics)
RETURN

GO
--Create temp table with two columns--
create procedure createGlobalTempTable
@columnNow varchar(max),
@columnNowType varchar(max),
@columnNext varchar(max),
@columnNextType varchar(max),
@tableName varchar(max)
as
declare @sql nvarchar(max)

set @sql = 'select top 0 B.'+@columnNow + ', B.'+@columnNext +' into ##tempColumnsCompare from '+@tableName+' as A left join '
	+@tableName+' as B on 1=0'
EXEC sp_executeSQL @sql
						
GO
--Insert distinct values of first column to temp table--
create procedure insertDistinctValuesIntoGlobalTempTable
@columnNow varchar(max),
@tableName varchar(max)
as
declare @sql nvarchar(max)

set @sql = 'insert into ##tempColumnsCompare ('+@columnNow + ') select distinct('+@columnNow + ') from '+@tableName
EXEC sp_executeSQL @sql

GO
--Drop temp table--
create procedure dropGlobalTempTable
as
declare @sql2 nvarchar(max)

set @sql2 = 'IF OBJECT_ID('+ CHAR(39) +'tempdb..##tempColumnsCompare'+ CHAR(39) +' , '+ 
	CHAR(39) +'U'+ CHAR(39) +') IS NOT NULL drop TABLE ##tempColumnsCompare'
EXEC sp_executeSQL @sql2

GO
--Remove duplicates from temp table--
create procedure removeDuplicates
@columnNow varchar(max),
@columnNext varchar(max)
as
BEGIN
declare @sql4 nvarchar(max),
@sql5 nvarchar(max)
set @sql4= 'ALTER TABLE ##tempColumnsCompare ADD AUTOID INT IDENTITY(1,1) '
EXEC sp_executeSQL @sql4
set @sql5= 'DELETE FROM ##tempColumnsCompare WHERE AUTOID NOT IN (SELECT MIN(AUTOID) _
	FROM ##tempColumnsCompare GROUP BY '+@columnNow+','+@columnNext+') '
EXEC sp_executeSQL @sql5
END

GO
--Create PK--
create procedure addKeys
@newTableName nvarchar(max),
@columnNow varchar(max),
@columnNext varchar(max),
@tableName varchar(max)
as
BEGIN
declare @sql6 nvarchar(max),
@sql7 nvarchar(max),
@sql8 nvarchar(max)

set @sql8 = 'alter table '+@newTableName+' ADD PRIMARY KEY('+@columnNow+')'
EXEC sp_executeSQL @sql8
set @sql7 = 'ALTER TABLE '+@tableName+'
	ADD FOREIGN KEY ('+@columnNow+')
	REFERENCES '+@newTableName+'('+@columnNow+')'
EXEC sp_executeSQL @sql7
END

GO
--Repoint FKs which point to specified column (which will be removed from table)--
create procedure repointKeys
@newTableName nvarchar(max),
@columnNext varchar(max),
@tableName varchar(max)
as
BEGIN
declare @sql6 nvarchar(max),
@sql7 nvarchar(max),
@sql8 nvarchar(max),
@tablePK varchar(max),
@columnPK varchar(max),
@foreignKey varchar(max)
set @sql8 = 'SELECT	f.name AS ForeignKey, OBJECT_NAME (f.referenced_object_id) AS ReferenceTableName,
	COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS ReferenceColumnName
	into ##temp
	FROM	sys.foreign_keys AS f INNER JOIN 
		sys.foreign_key_columns AS fc ON f.OBJECT_ID = fc.constraint_object_id
		where COL_NAME(fc.parent_object_id, fc.parent_column_id) = '+ CHAR(39) +@columnNext+ CHAR(39) +'
		and OBJECT_NAME(f.parent_object_id) ='+ CHAR(39) +@tableName+ CHAR(39)
EXEC sp_executeSQL @sql8
--Loop through #temp--

DECLARE FKs CURSOR LOCAL FOR (select ReferenceTableName, ReferenceColumnName, ForeignKey from ##temp)
declare @sql nvarchar(max),
@sql2 nvarchar(max),
@sql4 nvarchar(max),
@sql3 nvarchar(max)

OPEN FKs
FETCH NEXT FROM FKs into @tablePK, @columnPK, @foreignKey
WHILE @@FETCH_STATUS = 0
BEGIN

	set @sql2 = 'ALTER TABLE '+@newTableName+'
		ADD FOREIGN KEY ('+@columnNext+')
		REFERENCES '+@tablePK+'('+@columnPK+')'

	set @sql3 = 'ALTER TABLE '+@tableName+'
		DROP CONSTRAINT '+@foreignKey

	EXEC sp_executeSQL @sql2
	EXEC sp_executeSQL @sql3

    FETCH NEXT FROM FKs into @tablePK, @columnPK, @foreignKey
END

CLOSE FKs
DEALLOCATE FKs

set @sql6 = 'drop table ##temp'
	EXEC sp_executeSQL @sql6
END

GO

select OBJECT_NAME(t.object_id) as TableName
into #tempTables 
FROM sys.tables t where t.lob_data_space_id<>1

declare 
@tabelNow nvarchar(128),
@Number int,
@columnToDrop nvarchar(max),
@not3NF bit,
@NumberOfWrites int,
@NumberOfReads int

--Loop through each table--
DECLARE TablesCursor CURSOR LOCAL FOR (select TableName from #tempTables)
OPEN TablesCursor
FETCH NEXT FROM TablesCursor into @tabelNow
WHILE @@FETCH_STATUS = 0
BEGIN
	set @not3NF=0
	EXEC statisticsOfWrites @tabelNow, @NumberOfWrites OUTPUT, @NumberOfReads OUTPUT
	IF OBJECT_ID('tempdb..#tempColumns') IS NOT NULL DROP TABLE #tempColumns
	
	SELECT c.name as ColumnName, y.name as ColumnType
	into #tempColumns
	FROM sys.tables t 
	JOIN sys.columns c ON t.Object_ID = c.Object_ID 
	JOIN sys.types y ON y.system_type_id = c.system_type_id
		WHERE	OBJECT_NAME(t.object_id) = @tabelNow  and 
				c.name not in 
				(SELECT COL_NAME(ic.OBJECT_ID,ic.column_id) AS ColumnName
					FROM    sys.indexes AS i 
					INNER JOIN sys.index_columns AS ic ON  i.OBJECT_ID = ic.OBJECT_ID
								AND i.index_id = ic.index_id
								WHERE   i.is_primary_key = 1 and OBJECT_NAME(ic.object_id) = @tabelNow)
		ORDER BY c.name


	if (select COUNT(*) from #tempColumns)>1
		BEGIN
			declare 
			@columnNow varchar(max),
			@columnNowType varchar(max),
			@columnNext varchar(max),
			@columnNextType varchar(max),
			@sql nvarchar(max),
			@sql2 nvarchar(max),
			@sql3 nvarchar(max),
			@sql4 nvarchar(max),
			@newTableName nvarchar(max)

			--Loop through each column--
			DECLARE ColumnsCursor CURSOR LOCAL FOR (select ColumnName, ColumnType from #tempColumns)

			OPEN ColumnsCursor
			FETCH NEXT FROM ColumnsCursor into @columnNow, @columnNowType
			WHILE @@FETCH_STATUS = 0
			BEGIN
	
				if exists (select ColumnName from #tempColumns where ColumnName>@columnNow)
					BEGIN
					--Compare each column with all others--
					DECLARE ColumnsNexts CURSOR LOCAL FOR (select ColumnName, ColumnType from #tempColumns where ColumnName>@columnNow)
					OPEN ColumnsNexts
					FETCH NEXT FROM ColumnsNexts into @columnNext, @columnNextType
					WHILE @@FETCH_STATUS = 0
						BEGIN
						
							EXEC createGlobalTempTable @columnNow, @columnNowType, @columnNext, @columnNextType, @tabelNow
							EXEC insertDistinctValuesIntoGlobalTempTable @columnNow, @tabelNow
							--Amount of distinct values in column--
							set @Number = (select count(*) from ##tempColumnsCompare)
							EXEC dropGlobalTempTable

							set @sql3 = 'select '+@tabelNow+'.'+@columnNow+', '+@tabelNow+'.'+@columnNext+' into ##tempColumnsCompare from '+@tabelNow
							EXEC sp_executeSQL @sql3
							
							EXEC removeDuplicates  @columnNow, @columnNext
							--Compare amount of distinct values with amount of rows without duplicates--
							if (@Number = (select count(*) from ##tempColumnsCompare)
								AND @columnNextType<>'int' AND @columnNowType<>'int'
								AND @NumberOfWrites>@NumberOfReads)
								--AND (select count(*) from ##tempColumnsCompare)/@Number>10)
								BEGIN
								--Decompose to 3NF--
								set @not3NF=1
								set @newTableName = @columnNow+'_'+@columnNext+'_3NF'
								set @sql4 = 'select '+@columnNow+', '+@columnNext+' into '+@newTableName+' from ##tempColumnsCompare'
								EXEC sp_executeSQL @sql4
								EXEC addKeys @newTableName,@columnNow, @columnNext, @tabelNow
								EXEC repointKeys @newTableName, @columnNext, @tabelNow
								set @columnToDrop = @columnNext
								END

							EXEC dropGlobalTempTable
							--Switch columnNow and next--
							EXEC createGlobalTempTable @columnNext, @columnNextType, @columnNow, @columnNowType,  @tabelNow
							EXEC insertDistinctValuesIntoGlobalTempTable @columnNext, @tabelNow
							set @Number = (select count(*) from ##tempColumnsCompare)
							EXEC dropGlobalTempTable
							set @sql3 = 'select '+@tabelNow+'.'+@columnNext+', '+@tabelNow+'.'+@columnNow+' into ##tempColumnsCompare from '+@tabelNow
							EXEC sp_executeSQL @sql3

							EXEC removeDuplicates @columnNext, @columnNow
							if (@Number = (select count(*) from ##tempColumnsCompare)
								AND @columnNextType<>'int' AND @columnNowType<>'int'
								AND @NumberOfWrites>@NumberOfReads)
								--AND (select count(*) from ##tempColumnsCompare)/@Number>10)
								BEGIN
								set @not3NF=1
								set @newTableName = @columnNext+'_'+@columnNow+'_3NF'
								set @sql4 = 'select '+@columnNext+', '+@columnNow+' into '+@newTableName+' from ##tempColumnsCompare'
								EXEC sp_executeSQL @sql4
								EXEC addKeys @newTableName,@columnNext, @columnNow, @tabelNow
								EXEC repointKeys @newTableName, @columnNow, @tabelNow
								set @columnToDrop = @columnNow
								END
							

							EXEC dropGlobalTempTable

						
						FETCH NEXT FROM ColumnsNexts into @columnNext, @columnNextType
						END
					CLOSE ColumnsNexts
					DEALLOCATE ColumnsNexts
					END
			FETCH NEXT FROM ColumnsCursor into @columnNow, @columnNowType
			END
		CLOSE ColumnsCursor
		DEALLOCATE ColumnsCursor
		--Drop column in table--
		drop table #tempColumns
		set @sql = 'alter table '+@tabelNow+' Drop column ' + @columnToDrop  + ';'
		if @not3NF = 1
		EXEC sp_executeSQL @sql
	
	END
	FETCH NEXT FROM TablesCursor into @tabelNow
END
CLOSE TablesCursor
DEALLOCATE TablesCursor

GO
drop procedure dropGlobalTempTable
drop procedure createGlobalTempTable
drop procedure insertDistinctValuesIntoGlobalTempTable
drop procedure removeDuplicates
drop procedure addKeys
drop procedure repointKeys
drop procedure statisticsOfWrites
drop table #tempTables