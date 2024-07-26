/* 
This is what I poked at today. It is a deletion script.
The process in question is to migrating data from legacy systems into new systems, and to not have data collisions occur.
This necessitates:
    receiving data from the production environment, 
    converting data into the corresponding tables where it will sequentially follow the data already existing from production,
    then remove that data which was provided from production,
    then allow other partners to merge the remaining (converted) data into the existing production environment.
The problem: sometimes the provided data from the production environment is a lot! The standard deletion script takes too long.
    How can we speed it up?
    How can we account for orphaned rows that might exist for some reason in some tables?
The solution is below. First, a table of contents:
-----------------------------------------
setup -- right(b1_per_id\d,3) to delete
bare deletes
declare the "latest records to keep"
    Saves B1PERMIT and BDOCUMENT for last
    Cursor to iterate through the table names and primary key columns
    Build dynamic SQL for each table
    Execute the dynamic SQL
site wipe except those declared records
    get population to delete
    remove triggers that do delete tracking     
    Handles deletes on tables that don't directly connect to the permits.
    Tracks some records that cannot be deleted until after the main tables are cleard
    Deletes the regular permit tables, ordered to avoid foreign key issues
    Deletes the records stored earlier
    Clears B1PERMIT and BDOCUMENT
*/

--setup -- right(b1_per_id\d,3) to delete
--if you run this entire script with this setup part using blank values, then only "bare deletes" will do anything.
-- this segment of logic functions as a means to sort of automate the site wipe portion.
;print 'setup -- right(b1_per_id\d,3) to delete';
SELECT
    right3_b1_per_id1
into #preliminaryDeletes__rightB1PerIdsToDelete
from (values 
 ('')
,('HIS')
) t(right3_b1_per_id1)
;


--bare deletes - I do this before other steps, because these two tables can get populated by the conversion tool. The buffer buffs these first, then I remove old rows, then whatever gets converted into this will be new/relevant.
;print 'bare deletes';
delete d from G7CONTACT_AKA_INFO d join g3contact c on c.G1_CONTACT_NBR = d.G1_CONTACT_NBR
delete from G3CONTACT where isnull(audit_mod_by,'') <> 'AASEQ'
delete from G7CONTACT_ADDRESS where isnull(audit_mod_by,'') <> 'AASEQ'

delete from DOCUMENT_REVIEW_HISTORY
delete from DOCUMENT_STATUS_HISTORY
delete from XDOCUMENT_ENTITY
delete from BDOCUMENT
delete from BDOCUMENT_CONTENT
delete from BDOCUMENT_COMMENT
delete from FSHOPPING_CART_ITEM
delete from FSHOPPING_CART
delete from XPUBLIC_USER_PROV_LIC
delete from setdetails
delete from RSTATE_LIC
delete from XG6ACTION
delete from GPROCESS_SPEC_INFO_HISTORY -- where right(b1_per_id1,3) not in (<TARGET_B1_PER_ID1>)
delete from [AGIS_OBJECT_ENT]

go


--declare the "latest records to keep"
/*
This gets all tables, primary keys per table, orders by those keys desc, then saves lastmost b1_per_id row
*/
;print 'declare the "latest records to keep"';

-- Deletes the regular permit tables, ordered to avoid foreign key issues
-- Saves B1PERMIT and BDOCUMENT for last
;print '##preliminaryDeletes__permit_tables_to_clear';
select tab.name, tab.object_id
into #preliminaryDeletes__all_tables
from sys.tables tab
join sys.columns col on (tab.object_id = col.object_id)
where 
    col.name = 'B1_PER_ID1'
    and tab.name not in (  'B1PERMIT', 'BDOCUMENT' )
	
intersect
	
select tab.name, tab.object_id
from sys.tables tab
join sys.columns col on (tab.object_id = col.object_id)
where col.name = 'B1_PER_ID2'
;

    select parent_object_id, referenced_object_id
	into #preliminaryDeletes__all_fk_ref
	from #preliminaryDeletes__all_tables tab
	join sys.foreign_key_columns con on (tab.object_id = con.parent_object_id)
;

    select distinct tab.name, tab.object_id
	into #preliminaryDeletes__first_group
    from #preliminaryDeletes__all_tables tab
    left join #preliminaryDeletes__all_fk_ref con on (tab.object_id = con.referenced_object_id)
    where con.referenced_object_id is null
;

    select distinct tab.name, tab.object_id
	into #preliminaryDeletes__second_group
    from #preliminaryDeletes__all_tables tab
    left join (
        select * 
        from #preliminaryDeletes__all_fk_ref ref
        where parent_object_id not in (select OBJECT_ID from #preliminaryDeletes__first_group)
	) con on (tab.object_id = con.referenced_object_id)
    left join #preliminaryDeletes__first_group f on (tab.object_id = f.object_id)
    where 
        con.referenced_object_id is null
        and f.object_id is null
;

    select distinct tab.name, tab.object_id
	into #preliminaryDeletes__third_group
    from #preliminaryDeletes__all_tables tab
    left join (
        select * 
        from #preliminaryDeletes__all_fk_ref ref
        where parent_object_id not in (
            select OBJECT_ID from #preliminaryDeletes__first_group
            union
            select OBJECT_ID from #preliminaryDeletes__second_group
        )
    ) con on (tab.object_id = con.referenced_object_id)
    left join #preliminaryDeletes__first_group f on (tab.object_id = f.object_id)
    left join #preliminaryDeletes__second_group s on (tab.object_id = s.object_id)
    where 
        f.object_id is null
        and s.object_id is null
;

select distinct 
       tab.name as table_name 
       ,tab.object_id
	   ,case when f.object_id is not null then 1
            when s.object_id is not null then 2
			when t.object_id is not null then 3
			else 9999 end sort_group
into ##preliminaryDeletes__permit_tables_to_clear
from #preliminaryDeletes__all_tables tab
left join #preliminaryDeletes__first_group f on (tab.object_id = f.object_id)
left join #preliminaryDeletes__second_group s on (tab.object_id = s.object_id)
left join #preliminaryDeletes__third_group t on (tab.object_id = t.object_id)
WHERE
    1=1
    and tab.name not like 'aatable%'
order by 3,1
;
GO

;with a as (
	SELECT 
		 KU.table_name as TABLE_NAME
		,column_name as PRIMARYKEYCOLUMN
		,ku.ORDINAL_POSITION
	FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC 
	INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
		ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY'
		AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME
	join ##preliminaryDeletes__permit_tables_to_clear f on f.table_name = ku.TABLE_NAME
	where
		1=1
	--ORDER BY KU.TABLE_NAME,KU.ORDINAL_POSITION
), has_servProvCode as (
    select distinct 
        table_name 
    from a 
    WHERE
        1=1
        and PRIMARYKEYCOLUMN = 'serv_prov_code'
)
select
	a.TABLE_NAME
	,a.PRIMARYKEYCOLUMN
    ,a.ORDINAL_POSITION
into ##_preliminaryDeletes__table_to_primaryKeyColumns
from a 
join has_servProvCode f on f.table_name = a.table_name --filters out nonstandard tables, eg [PERMIT_EDMS_CLOUDSR45612]
where
	1=1
	and PRIMARYKEYCOLUMN not in (
		'serv_prov_code'
		,'B1_PER_ID1'
		,'B1_PER_ID2'
		,'B1_PER_ID3'
	)
ORDER BY TABLE_NAME,ORDINAL_POSITION
;

SELECT 
	table_name
	,STRING_AGG(QUOTENAME(primaryKeyColumn), ' desc, ')+' desc' primary_keys
into ##preliminaryDeletes__table_to_primaryKeyColumns
FROM ##_preliminaryDeletes__table_to_primaryKeyColumns a
group by table_name

create table ##_preliminaryDeletes__permits_to_keep_for_later_delete (
    serv_prov_code varchar(100)
    ,b1_per_id1  varchar(100)
	,b1_per_id2 varchar(100)
	,b1_per_id3 varchar(100)
    ,table_name varchar(100)
)
go




DECLARE @sql NVARCHAR(MAX) = N'';
DECLARE @table NVARCHAR(128);
DECLARE @primaryKeyColumns NVARCHAR(MAX);

-- Cursor to iterate through the table names and primary key columns
DECLARE table_cursor CURSOR FOR
    SELECT 
        table_name
        , primary_keys
    FROM ##preliminaryDeletes__table_to_primaryKeyColumns
    ORDER BY table_name
    ;

OPEN table_cursor;

FETCH NEXT FROM table_cursor INTO @table, @primaryKeyColumns;

-- Build dynamic SQL for each table
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = @sql + '
    insert into ##_preliminaryDeletes__permits_to_keep_for_later_delete
    SELECT TOP 1 serv_prov_code,b1_per_id1, b1_per_id2, b1_per_id3, '''+QUOTENAME(@table)+'''
    FROM ' + QUOTENAME(@table) + '
    ORDER BY ' + @primaryKeyColumns + '
	;
    ';
    FETCH NEXT FROM table_cursor INTO @table, @primaryKeyColumns;
END

CLOSE table_cursor;
DEALLOCATE table_cursor;

---- Print the dynamic SQL for debugging
-- PRINT @sql;
--select @sql;

----- Execute the dynamic SQL
EXEC sp_executesql @sql;

select distinct serv_prov_code,b1_per_id1,b1_per_id2,b1_per_id3 into ##preliminaryDeletes__permits_to_keep_for_later_delete from ##_preliminaryDeletes__permits_to_keep_for_later_delete
go
;



--site wipe except those declared records
--This part might not be strictly necessary. The goal is to ensure nothing gets de-populated which needs some population for some unknown field to progress appropriately.
;print 'site wipe except those declared records';
/*
get population to delete
remove triggers that do delete tracking     
Handles deletes on tables that don't directly connect to the permits.
Tracks some records that cannot be deleted until after the main tables are cleard
Deletes the regular permit tables, ordered to avoid foreign key issues
Deletes the records stored earlier
Clears B1PERMIT and BDOCUMENT
*/
IF OBJECT_ID('tempdb.dbo.##completed_sitewipe', 'U') IS NOT NULL DROP TABLE ##completed_sitewipe
;
create table ##completed_sitewipe (dt datetime, val varchar(1000))
;

-- get population to delete
    --i.e. all Building/Converted/Historical records that are not in the H01 set.
    --put the b1_per_id1 suffix you want to target for deletion.
insert into ##completed_sitewipe values (getdate(),'##permits_to_delete')
;
select 
    a.SERV_PROV_CODE
    , a.B1_PER_ID1
    , a.B1_PER_ID2
    , a.B1_PER_ID3
into ##permits_to_delete
from B1PERMIT a 
join #preliminaryDeletes__rightB1PerIdsToDelete f on f.right3_b1_per_id1 = right(a.b1_per_id1,3)
where 
    1=1
    --and right(b1_per_id1,3) in (
    --''
    --)
    and not exists (
        --this omits the "latest row" I wanted to keep.
        --Do this after buffer, I think.
        select 1
        from ##preliminaryDeletes__permits_to_keep_for_later_delete
        WHERE   
            1=1
            and b1_per_id1 = a.b1_per_id1 
            AND b1_per_id2 = a.b1_per_id2 
            and b1_per_id3 = a.b1_per_id3
    )
;
go

--remove triggers that do delete tracking
;insert into ##completed_sitewipe values (getdate(),'remove deletion triggers');
;print 'remove deletion triggers';
DECLARE @sql NVARCHAR(MAX) = N'';

;WITH bodies AS (
	SELECT
		o.[name]
		,c.[text]
	FROM sys.objects AS o
	INNER JOIN  sys.syscomments AS c ON  o.object_id = c.id
	WHERE   
		1=1
		and o.[type] = 'TR'
), a AS (
	SELECT 
		 sysobjects.name AS trigger_name 
		,USER_NAME(sysobjects.uid) AS trigger_owner 
		,s.name AS table_schema 
		,OBJECT_NAME(parent_obj) AS table_name 
		,OBJECTPROPERTY( id, 'ExecIsUpdateTrigger') AS isupdate 
		,OBJECTPROPERTY( id, 'ExecIsDeleteTrigger') AS isdelete 
		,OBJECTPROPERTY( id, 'ExecIsInsertTrigger') AS isinsert 
		,OBJECTPROPERTY( id, 'ExecIsAfterTrigger') AS isafter 
		,OBJECTPROPERTY( id, 'ExecIsInsteadOfTrigger') AS isinsteadof 
		,OBJECTPROPERTY( id, 'ExecIsTriggerDisabled') AS [disabled] 
	FROM sysobjects 
	INNER JOIN sysusers ON sysobjects.uid = sysusers.uid 
	INNER JOIN sys.tables t ON sysobjects.parent_obj = t.object_id 
	INNER JOIN sys.schemas s ON t.schema_id = s.schema_id 
	WHERE
		1=1
		and sysobjects.type = 'TR' 
)
SELECT @sql += 'DROP TRIGGER ' + QUOTENAME(a.trigger_owner) + '.' + QUOTENAME(a.trigger_name)+char(10)+';'
FROM a
LEFT JOIN bodies b ON b.name = a.trigger_name
WHERE
	1=1
	and trigger_name like '%del%'
;
EXEC sp_executesql @sql;
go

-- Handles deletes on tables that don't directly connect to the permits.
-- They are cleared first to avoid foreign key conflicts.
;insert into ##completed_sitewipe values (getdate(),'GGDSHEET_ITEM_ASITAB_VALUE');
;print 'GGDSHEET_ITEM_ASITAB_VALUE';
delete v
from ##permits_to_delete p
inner join GGUIDESHEET g on p.SERV_PROV_CODE = g.SERV_PROV_CODE and p.B1_PER_ID1 = g.B1_PER_ID1 and p.B1_PER_ID2 = g.B1_PER_ID2 and p.B1_PER_ID3 = g.B1_PER_ID3
inner join GGDSHEET_ITEM_ASITAB_VALUE v on g.GUIDESHEET_SEQ_NBR = v.GUIDESHEET_SEQ_NBR

;insert into ##completed_sitewipe values (getdate(),'GGDSHEET_ITEM_ASITAB');
;print 'GGDSHEET_ITEM_ASITAB';
delete v
from ##permits_to_delete p
inner join GGUIDESHEET g on p.SERV_PROV_CODE = g.SERV_PROV_CODE and p.B1_PER_ID1 = g.B1_PER_ID1 and p.B1_PER_ID2 = g.B1_PER_ID2 and p.B1_PER_ID3 = g.B1_PER_ID3
inner join GGDSHEET_ITEM_ASITAB v on g.GUIDESHEET_SEQ_NBR = v.GUIDESHEET_SEQ_NBR

;insert into ##completed_sitewipe values (getdate(),'GGDSHEET_ITEM_ASI');
;print 'GGDSHEET_ITEM_ASI';
delete v
from ##permits_to_delete p
inner join GGUIDESHEET g on p.SERV_PROV_CODE = g.SERV_PROV_CODE and p.B1_PER_ID1 = g.B1_PER_ID1 and p.B1_PER_ID2 = g.B1_PER_ID2 and p.B1_PER_ID3 = g.B1_PER_ID3
inner join GGDSHEET_ITEM_ASI v on g.GUIDESHEET_SEQ_NBR = v.GUIDESHEET_SEQ_NBR

;insert into ##completed_sitewipe values (getdate(),'GGUIDESHEET_ITEM');
;print 'GGUIDESHEET_ITEM';
delete v
from ##permits_to_delete p
inner join GGUIDESHEET g on p.SERV_PROV_CODE = g.SERV_PROV_CODE and p.B1_PER_ID1 = g.B1_PER_ID1 and p.B1_PER_ID2 = g.B1_PER_ID2 and p.B1_PER_ID3 = g.B1_PER_ID3
inner join GGUIDESHEET_ITEM v on g.GUIDESHEET_SEQ_NBR = v.GUIDESHEET_SEQ_NBR

;insert into ##completed_sitewipe values (getdate(),'G7CONTACT_ADDRESS');
;print 'G7CONTACT_ADDRESS';
delete a
from ##permits_to_delete p
inner join B3CONTACT bc on p.SERV_PROV_CODE = bc.SERV_PROV_CODE and p.B1_PER_ID1 = bc.B1_PER_ID1 and p.B1_PER_ID2 = bc.B1_PER_ID2 and p.B1_PER_ID3 = bc.B1_PER_ID3
inner join G7CONTACT_ADDRESS a on bc.SERV_PROV_CODE = a.SERV_PROV_CODE and bc.B1_CONTACT_NBR = a.G7_ENTITY_ID
where a.G7_ENTITY_TYPE = 'CAP_CONTACT'

;insert into ##completed_sitewipe values (getdate(),'BDOCUMENT_CONTENT');
;print 'BDOCUMENT_CONTENT';
delete dc
from ##permits_to_delete p
inner join BDOCUMENT bd on p.SERV_PROV_CODE = bd.SERV_PROV_CODE and p.B1_PER_ID1 = bd.B1_PER_ID1 and p.B1_PER_ID2 = bd.B1_PER_ID2 and p.B1_PER_ID3 = bd.B1_PER_ID3
inner join BDOCUMENT_CONTENT dc on bd.SERV_PROV_CODE = dc.SERV_PROV_CODE and bd.DOC_SEQ_NBR = dc.DOC_SEQ_NBR

;insert into ##completed_sitewipe values (getdate(),'BDOCUMENT_COMMENT_ELEMENT');
;print 'BDOCUMENT_COMMENT_ELEMENT';
delete dc
from ##permits_to_delete p
inner join BDOCUMENT bd on p.SERV_PROV_CODE = bd.SERV_PROV_CODE and p.B1_PER_ID1 = bd.B1_PER_ID1 and p.B1_PER_ID2 = bd.B1_PER_ID2 and p.B1_PER_ID3 = bd.B1_PER_ID3
inner join BDOCUMENT_COMMENT_ELEMENT dc on bd.SERV_PROV_CODE = dc.SERV_PROV_CODE and bd.DOC_SEQ_NBR = dc.DOC_SEQ_NBR

;insert into ##completed_sitewipe values (getdate(),'BDOCUMENT_COMMENT');
;print 'BDOCUMENT_COMMENT';
delete dc
from ##permits_to_delete p
inner join BDOCUMENT bd on p.SERV_PROV_CODE = bd.SERV_PROV_CODE and p.B1_PER_ID1 = bd.B1_PER_ID1 and p.B1_PER_ID2 = bd.B1_PER_ID2 and p.B1_PER_ID3 = bd.B1_PER_ID3
inner join BDOCUMENT_COMMENT dc on bd.SERV_PROV_CODE = dc.SERV_PROV_CODE and bd.DOC_SEQ_NBR = dc.DOC_SEQ_NBR

;insert into ##completed_sitewipe values (getdate(),'DOCUMENT_REVIEW_HISTORY');
;print 'DOCUMENT_REVIEW_HISTORY';
delete h
from ##permits_to_delete p
inner join BDOCUMENT bd on p.SERV_PROV_CODE = bd.SERV_PROV_CODE and p.B1_PER_ID1 = bd.B1_PER_ID1 and p.B1_PER_ID2 = bd.B1_PER_ID2 and p.B1_PER_ID3 = bd.B1_PER_ID3
inner join DOCUMENT_REVIEW_HISTORY h on bd.SERV_PROV_CODE = h.SERV_PROV_CODE and bd.DOC_SEQ_NBR = h.DOC_SEQ_NBR

;insert into ##completed_sitewipe values (getdate(),'DOCUMENT_STATUS_HISTORY');
;print 'DOCUMENT_STATUS_HISTORY';
delete h
from ##permits_to_delete p
inner join BDOCUMENT bd on p.SERV_PROV_CODE = bd.SERV_PROV_CODE and p.B1_PER_ID1 = bd.B1_PER_ID1 and p.B1_PER_ID2 = bd.B1_PER_ID2 and p.B1_PER_ID3 = bd.B1_PER_ID3
inner join DOCUMENT_STATUS_HISTORY h on bd.SERV_PROV_CODE = h.SERV_PROV_CODE and bd.DOC_SEQ_NBR = h.DOC_SEQ_NBR

;insert into ##completed_sitewipe values (getdate(),'XDOCUMENT_ENTITY');
;print 'XDOCUMENT_ENTITY';
delete de
from ##permits_to_delete p
inner join BDOCUMENT bd on p.SERV_PROV_CODE = bd.SERV_PROV_CODE and p.B1_PER_ID1 = bd.B1_PER_ID1 and p.B1_PER_ID2 = bd.B1_PER_ID2 and p.B1_PER_ID3 = bd.B1_PER_ID3
inner join XDOCUMENT_ENTITY de on bd.SERV_PROV_CODE = de.SERV_PROV_CODE and bd.DOC_SEQ_NBR = de.DOC_SEQ_NBR

;insert into ##completed_sitewipe values (getdate(),'B6CONDIT_HISTORY_DETAIL');
;print 'B6CONDIT_HISTORY_DETAIL';
DELETE d
FROM B6CONDIT_HISTORY_DETAIL d
LEFT JOIN B6CONDIT_HISTORY h ON d.SERV_PROV_CODE = h.SERV_PROV_CODE AND d.B1_CON_HISTORY_NBR = h.B1_CON_HISTORY_NBR
LEFT JOIN B6CONDIT bc ON h.SERV_PROV_CODE = bc.SERV_PROV_CODE AND h.B1_CON_NBR = bc.B1_CON_NBR
LEFT JOIN ##permits_to_delete p ON p.SERV_PROV_CODE = bc.SERV_PROV_CODE AND p.B1_PER_ID1 = bc.B1_PER_ID1 AND p.B1_PER_ID2 = bc.B1_PER_ID2 AND p.B1_PER_ID3 = bc.B1_PER_ID3
WHERE 
    1=1
    and (
        bc.SERV_PROV_CODE IS NULL  --orphans
        OR  
        p.B1_PER_ID1 IS NOT NULL 
    )
;



;insert into ##completed_sitewipe values (getdate(),'B6CONDIT_HISTORY');
;print 'B6CONDIT_HISTORY';
DELETE h
FROM B6CONDIT_HISTORY h
LEFT JOIN B6CONDIT bc ON h.SERV_PROV_CODE = bc.SERV_PROV_CODE AND h.B1_CON_NBR = bc.B1_CON_NBR
LEFT JOIN ##permits_to_delete p ON p.SERV_PROV_CODE = bc.SERV_PROV_CODE AND p.B1_PER_ID1 = bc.B1_PER_ID1 AND p.B1_PER_ID2 = bc.B1_PER_ID2 AND p.B1_PER_ID3 = bc.B1_PER_ID3
WHERE 
    1=1
    and (
        bc.SERV_PROV_CODE IS NULL --orphans
        OR 
        p.b1_per_id1 is not null 
    )
;

;insert into ##completed_sitewipe values (getdate(),'BMODEL_VAR');
;print 'BMODEL_VAR';
delete v
from ##permits_to_delete p
inner join BMODEL m on p.SERV_PROV_CODE = m.SERV_PROV_CODE and p.B1_PER_ID1 = m.B1_PER_ID1 and p.B1_PER_ID2 = m.B1_PER_ID2 and p.B1_PER_ID3 = m.B1_PER_ID3
inner join BMODEL_VAR v on m.SERV_PROV_CODE = v.SERV_PROV_CODE and m.MODEL_ID = v.MODEL_ID


-- Tracks some records that cannot be deleted until after the main tables are cleard
;insert into ##completed_sitewipe values (getdate(),'#invoices_to_delete');
;print '#invoices_to_delete';
select distinct i.*
into #invoices_to_delete
from ##permits_to_delete p
inner join X4FEEITEM_INVOICE xf on p.SERV_PROV_CODE = xf.SERV_PROV_CODE and p.B1_PER_ID1 = xf.B1_PER_ID1 and p.B1_PER_ID2 = xf.B1_PER_ID2 and p.B1_PER_ID3 = xf.B1_PER_ID3
inner join F4INVOICE i on xf.SERV_PROV_CODE = i.SERV_PROV_CODE and xf.INVOICE_NBR = i.INVOICE_NBR

;insert into ##completed_sitewipe values (getdate(),'#receipts_to_delete');
;print '#receipts_to_delete';
select distinct r.*
into #receipts_to_delete
from ##permits_to_delete p
inner join F4PAYMENT fp on p.SERV_PROV_CODE = fp.SERV_PROV_CODE and p.B1_PER_ID1 = fp.B1_PER_ID1 and p.B1_PER_ID2 = fp.B1_PER_ID2 and p.B1_PER_ID3 = fp.B1_PER_ID3
inner join F4RECEIPT r on fp.SERV_PROV_CODE = r.SERV_PROV_CODE and fp.RECEIPT_NBR = r.RECEIPT_NBR

;insert into ##completed_sitewipe values (getdate(),'#payment_invoices_to_delete');
;print '#payment_invoices_to_delete';
select distinct i.*
into #payment_invoices_to_delete
from ##permits_to_delete p
inner join X4FEEITEM_INVOICE xf on p.SERV_PROV_CODE = xf.SERV_PROV_CODE and p.B1_PER_ID1 = xf.B1_PER_ID1 and p.B1_PER_ID2 = xf.B1_PER_ID2 and p.B1_PER_ID3 = xf.B1_PER_ID3
inner join X4PAYMENT_INVOICE i on xf.SERV_PROV_CODE = i.SERV_PROV_CODE and xf.INVOICE_NBR = i.INVOICE_NBR

;insert into ##completed_sitewipe values (getdate(),'#carts_to_delete');
;print '#carts_to_delete';
select distinct sc.*
into #carts_to_delete
from ##permits_to_delete p
inner join FSHOPPING_CART_ITEM i on p.SERV_PROV_CODE = i.SERV_PROV_CODE and p.B1_PER_ID1 = i.B1_PER_ID1 and p.B1_PER_ID2 = i.B1_PER_ID2 and p.B1_PER_ID3 = i.B1_PER_ID3
inner join FSHOPPING_CART sc on i.CART_SEQ_NBR = sc.CART_SEQ_NBR

-- Deletes the regular permit tables, ordered to avoid foreign key issues
-- Saves B1PERMIT and BDOCUMENT for last
;insert into ##completed_sitewipe values (getdate(),'##permit_tables_to_clear');
;print '##permit_tables_to_clear';
select tab.name, tab.object_id
into #all_tables
from sys.tables tab
join sys.columns col on (tab.object_id = col.object_id)
where 
    col.name = 'B1_PER_ID1'
    and tab.name not in (  
        'B1PERMIT' --dont delete from here until all other target data is deleted, cuz it is what I base other deletes on. b1permit is "the top".
        , 'BDOCUMENT' --emptied in preliminary bare deletes.
    )
	
intersect
	
select tab.name, tab.object_id
from sys.tables tab
join sys.columns col on (tab.object_id = col.object_id)
where col.name = 'B1_PER_ID2'
;

    select parent_object_id, referenced_object_id
	into #all_fk_ref
	from #all_tables tab
	join sys.foreign_key_columns con on (tab.object_id = con.parent_object_id)
;

    select distinct tab.name, tab.object_id
	into #first_group
    from #all_tables tab
    left join #all_fk_ref con on (tab.object_id = con.referenced_object_id)
    where con.referenced_object_id is null
;

    select distinct tab.name, tab.object_id
	into #second_group
    from #all_tables tab
    left join (
        select * 
        from #all_fk_ref ref
        where parent_object_id not in (select OBJECT_ID from #first_group)
	) con on (tab.object_id = con.referenced_object_id)
    left join #first_group f on (tab.object_id = f.object_id)
    where 
        con.referenced_object_id is null
        and f.object_id is null
;

    select distinct tab.name, tab.object_id
	into #third_group
    from #all_tables tab
    left join (
        select * 
        from #all_fk_ref ref
        where parent_object_id not in (
            select OBJECT_ID from #first_group
            union
            select OBJECT_ID from #second_group
        )
    ) con on (tab.object_id = con.referenced_object_id)
    left join #first_group f on (tab.object_id = f.object_id)
    left join #second_group s on (tab.object_id = s.object_id)
    where 
        f.object_id is null
        and s.object_id is null
;

select distinct 
       tab.name as table_name 
       ,tab.object_id
	   ,case when f.object_id is not null then 1
            when s.object_id is not null then 2
			when t.object_id is not null then 3
			else 9999 end sort_group
into ##permit_tables_to_clear
from #all_tables tab
left join #first_group f on (tab.object_id = f.object_id)
left join #second_group s on (tab.object_id = s.object_id)
left join #third_group t on (tab.object_id = t.object_id)
WHERE
    1=1
    and tab.name not like 'aatable%'
    and tab.name not like 'jms%'
order by 3,1
;

;insert into ##completed_sitewipe values (getdate(),'generated tables');
;print 'generated tables';

IF OBJECT_ID('tempdb.dbo.##debug_deletion_sql', 'U') IS NOT NULL DROP TABLE ##debug_deletion_sql
go

DECLARE @BatchSize INT = 8500
DECLARE @TableName NVARCHAR(128)
DECLARE @SQL NVARCHAR(MAX) = ''
create table ##debug_deletion_sql (
	sql_to_execute NVARCHAR(MAX)
)
    -- Loop through each table in ##permit_tables_to_clear
DECLARE table_cursor CURSOR FOR
SELECT table_name FROM ##permit_tables_to_clear order by sort_group,table_name
OPEN table_cursor

FETCH NEXT FROM table_cursor INTO @TableName

WHILE @@FETCH_STATUS = 0
BEGIN
    -- Prepare the dynamic SQL for deletion
    SET @SQL = 'IF EXISTS (SELECT TOP 1 * FROM ' + @TableName + ')
BEGIN
    ;insert into ##completed_sitewipe values (getdate(),''' + @TableName + ''');
    ;print ''' + @TableName + ''';
    DECLARE @BatchSize INT = ' + cast(@BatchSize as NVARCHAR) + '
    DECLARE @RowCount INT
    SELECT serv_prov_code, b1_per_id1, b1_per_id2, b1_per_id3 INTO #TempToDelete FROM ##permits_to_delete;


    -- Get the count of rows to delete
    SET @RowCount = (SELECT COUNT(*) FROM #TempToDelete);

    WHILE @RowCount > 0
    BEGIN
        -- Delete rows in batches
        DELETE a
        FROM ' + @TableName + ' a
        INNER JOIN (
            SELECT TOP (' + CAST(@BatchSize AS NVARCHAR) + ') * 
            FROM #TempToDelete 
            ORDER BY serv_prov_code, b1_per_id1, b1_per_id2, b1_per_id3
        ) b 
        ON CONCAT(b.b1_per_id1, b.b1_per_id2, b.b1_per_id3) = CONCAT(a.b1_per_id1, a.b1_per_id2, a.b1_per_id3);

        -- Remove the deleted rows from the temporary table
        DELETE FROM #TempToDelete
        WHERE 
            1=1
            AND CONCAT(serv_prov_code, b1_per_id1, b1_per_id2, b1_per_id3) IN (
                SELECT TOP (' + CAST(@BatchSize AS NVARCHAR) + ') 
                    CONCAT(serv_prov_code, b1_per_id1, b1_per_id2, b1_per_id3)
                FROM #TempToDelete 
                ORDER BY serv_prov_code, b1_per_id1, b1_per_id2, b1_per_id3
            );

        -- Update the row count
        SET @RowCount = (SELECT COUNT(*) FROM #TempToDelete);
    END
    DROP TABLE #TempToDelete
END

'

    -- Execute the dynamic SQL
	insert into ##debug_deletion_sql
	select @sql
	
	EXEC sp_executesql @SQL 

    FETCH NEXT FROM table_cursor INTO @TableName
END

CLOSE table_cursor
DEALLOCATE table_cursor




-- Deletes the records stored earlier
;insert into ##completed_sitewipe values (getdate(),'X4PAYMENT_INVOICE');
;print 'X4PAYMENT_INVOICE';
delete i
from #payment_invoices_to_delete d
inner join X4PAYMENT_INVOICE i on d.SERV_PROV_CODE = i.SERV_PROV_CODE and d.INVOICE_NBR = i.INVOICE_NBR

;insert into ##completed_sitewipe values (getdate(),'F4INVOICE');
;print 'F4INVOICE';
DELETE i
FROM F4INVOICE i
LEFT JOIN X4PAYMENT_INVOICE b ON i.SERV_PROV_CODE = b.SERV_PROV_CODE AND i.INVOICE_NBR = b.INVOICE_NBR
LEFT JOIN X4FEEITEM_INVOICE c ON i.INVOICE_NBR = c.INVOICE_NBR
LEFT JOIN #invoices_to_delete d ON i.SERV_PROV_CODE = d.SERV_PROV_CODE AND i.INVOICE_NBR = d.INVOICE_NBR
WHERE 
    1=1
    and (
        d.SERV_PROV_CODE IS NOT NULL 
        OR (b.INVOICE_NBR IS NULL AND c.INVOICE_NBR IS NULL) --orphans
    )
;

;insert into ##completed_sitewipe values (getdate(),'F4RECEIPT');
;print 'F4RECEIPT';
DELETE r
FROM F4RECEIPT r
LEFT JOIN F4PAYMENT b ON r.SERV_PROV_CODE = b.SERV_PROV_CODE AND r.RECEIPT_NBR = b.RECEIPT_NBR
LEFT JOIN #receipts_to_delete d ON r.SERV_PROV_CODE = d.SERV_PROV_CODE AND r.RECEIPT_NBR = d.RECEIPT_NBR
WHERE 
    1=1
    and (
        d.SERV_PROV_CODE IS NOT NULL 
        OR 
        b.RECEIPT_NBR IS NULL --orphans
    )
;


;insert into ##completed_sitewipe values (getdate(),'FSHOPPING_CART');
;print 'FSHOPPING_CART';
delete sc
from #carts_to_delete d
inner join FSHOPPING_CART sc on d.CART_SEQ_NBR = sc.CART_SEQ_NBR


-- Clears B1PERMIT and BDOCUMENT
;insert into ##completed_sitewipe values (getdate(),'BDOCUMENT');
;print 'BDOCUMENT';
delete bd
from ##permits_to_delete p
inner join BDOCUMENT bd on p.SERV_PROV_CODE = bd.SERV_PROV_CODE and p.B1_PER_ID1 = bd.B1_PER_ID1 and p.B1_PER_ID2 = bd.B1_PER_ID2 and p.B1_PER_ID3 = bd.B1_PER_ID3

;insert into ##completed_sitewipe values (getdate(),'B1PERMIT');
;print 'B1PERMIT';
delete bp
from ##permits_to_delete p
inner join B1PERMIT bp on p.SERV_PROV_CODE = bp.SERV_PROV_CODE and p.B1_PER_ID1 = bp.B1_PER_ID1 and p.B1_PER_ID2 = bp.B1_PER_ID2 and p.B1_PER_ID3 = bp.B1_PER_ID3

