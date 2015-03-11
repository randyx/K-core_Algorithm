USE KCORE
/*
�����ṹLINGSHI,��Ϊͳ��ÿ��ҩ�Ľڵ��ps��ֵ��
ID---ҩ������ VALUE---��Ӧҩ�ĵ�ps��ֵ,
�����ʼ�մ��ڣ������Žڵ�ļ��ٶ����ϱ仯,
�ڵ㱻ɾ����������нڵ���ϢҲ����ɾ��
*/
DECLARE @LINGSHI TABLE( 
					ID VARCHAR(10),
					VALUE INT
					)
/*
�����ṹ Namepage Ϊ�˱��潫Ҫɾ����ҩ�Ľڵ㼯��,
ID---ҩ������
*/
DECLARE @Namepage TABLE(
					ID VARCHAR(10)
					)
/*
�����ṹNamepage1 ����ϱ�ʹ�ã�
�������ϱ��е�Ҫɾ���ڵ�������Ľڵ㣬�Դﵽ��̬�ı�LINGSHI���Ŀ��
ID---ҩ������
*/
DECLARE @Namepage1 TABLE(
					ID VARCHAR(10),
					VALUE INT
					)
/*
����Cores��ʾ��Ҫִ�еĶ���core�㷨,�����Լ��涨
*/
declare @Cores INT
set @Cores=1
/*
��ԭ��������ݸ��Ƹ��µ����ɱ�,����������ò����Ľ��
*/
DECLARE @sql varchar(20) 
set @sql='core'+cast(@Cores as varchar(2)) 
Exec('create table '+@sql+'(lh varchar(10),rh varchar(10),ps smallint)')
Exec('insert into ' +@sql+ ' SELECT * FROM dbo.D23_ZY_lt_ps')


INSERT INTO @Namepage(ID) SELECT DISTINCT rh from dbo.D23_ZY_lt_ps  --rh����Namepage�У���ʱNamepage������һ�����ɵ�����
INSERT INTO @Namepage(ID) SELECT DISTINCT lh from dbo.D23_ZY_lt_ps  --lh����Namepage��

/*
����һ���ǰ�����ҩ����ȡ������װ��LINGSHI���У�����psֵ��ʼ��Ϊ0
*/
declare @name varchar(10)
declare pcurr cursor for select DISTINCT ID from @Namepage
open pcurr
fetch next from pcurr into @name
while (@@fetch_status = 0)
begin
 INSERT INTO @LINGSHI(ID,VALUE) VALUES(@name,0)
  fetch next from pcurr into @name
end
close pcurr
deallocate pcurr

/*
����һ����ͳ��ÿ��ҩ���ܵ�psֵ�����������ݵ�LINGSHI����
�������ݵ�VALUE�ǰ�psֵ�����ۼ�,���Ҫʵ�ּ򵥹�ϵ,��psֵʱ
ֱ���ó���1������psֵ����
*/
declare @lname varchar(10)
declare @rname varchar(10)
declare @iid int
declare pcurr cursor for select lh,rh,ps from dbo.D23_ZY_lt_ps
open pcurr
fetch next from pcurr into @lname,@rname,@iid
while (@@fetch_status = 0)
begin
 UPDATE @LINGSHI SET VALUE= VALUE+@iid WHERE ID = @lname OR ID=@rname
 fetch next from pcurr into @lname,@rname,@iid
end
close pcurr
deallocate pcurr

/*
����Core����ʹ�㷨ѭ��ִ��,��1��ʼ��Cores-1
Cores����1ʱ,while������,ֱ������ѭ��
Cores>1ʱ,����ѭ���ݹ�ִ��
*/
declare @Core INT
set @Core=1
while @Core<=@Cores-1
begin

/*
��Namepageԭ����������գ�Ȼ����װ��Ҫɾ��ҩ�Ľڵ��ID
*/
DELETE FROM @Namepage
DECLARE @Ai INT
SET @Ai=0
WHILE @Ai<=@Core
BEGIN
	INSERT INTO @Namepage(ID) SELECT ID FROM @LINGSHI WHERE VALUE = @Ai
SET @Ai=@Ai+1
END

/*
���Թ۲�ִ��Ч��,����ʡȥ
*/
SELECT ID,VALUE FROM @LINGSHI
SELECT ID FROM @Namepage

/*
�ҵ���Ҫɾ��ҩ�Ľڵ�������Ľڵ㣬�ŵ�Namepage1���У�Ϊ��һ������LINGSHI����׼��
�����ҵ��󣬰�Ҫɾ����ҩ�Ľڵ��LINGSHI���ԭ��D23_ZY_lt_ps��ɾȥ
*/
declare @delname varchar(10)
declare pcurr cursor for select ID from @Namepage
open pcurr
fetch next from pcurr into @delname
while (@@fetch_status = 0)
begin

 INSERT INTO @Namepage1(ID,VALUE) SELECT lh,ps FROM dbo.D23_ZY_lt_ps WHERE rh=@delname
 INSERT INTO @Namepage1(ID,VALUE) SELECT rh,ps FROM dbo.D23_ZY_lt_ps WHERE lh=@delname
 DELETE FROM @LINGSHI WHERE ID=@delname
 --DELETE FROM dbo.D23_ZY_lt_ps WHERE lh=@delname OR rh=@delname
 --����deletesqlɾ�����,����̬sql,ɾ�����ǵ����ɱ�Ľڵ�
 declare @deleteSql nvarchar(100)
 set @deleteSql='DELETE FROM '+@sql+' WHERE lh=@delname OR rh=@delname'
 EXEC sp_executesql @deleteSql,N'@delname varchar(10)',@delname
 fetch next from pcurr into @delname
end
close pcurr
deallocate pcurr

/*
����LINGSHI���е�psֵ
*/
declare @delname1 varchar(10)
DECLARE @VALUE1 INT
declare pcurr cursor for select ID,VALUE from @Namepage1
open pcurr
fetch next from pcurr into @delname1,@VALUE1
while (@@fetch_status = 0)
begin
 UPDATE @LINGSHI SET VALUE= VALUE-@VALUE1 WHERE ID = @delname1
 fetch next from pcurr into @delname1,@VALUE1
end
close pcurr
deallocate pcurr

/*
���ѭ�������ж�ɾ�������ڵ��,psֵΪ0�������������@sql�����ѱ�ɾ����û������ڵ�
��ʵ���ϣ�����ڵ��Ǵ��ڵģ�ֻ�ǹ�����һ����
���α���LINGSHI��value=0ʱ�����������뵽���ɱ�@sql����
*/
declare @ID2 varchar(10)
DECLARE @VALUE2 INT
declare pcurr cursor for select ID,VALUE from @LINGSHI
open pcurr
fetch next from pcurr into @ID2,@VALUE2
while (@@fetch_status = 0)
begin
 IF @VALUE2=0
 BEGIN
 declare @Sql0 nvarchar(100)
 --��������rh=0.��ʾû�нڵ���֮����������һ�������Ľڵ�
 set @Sql0='INSERT INTO '+@sql+' (lh,rh,ps)values(@ID2,0,0)'
 EXEC sp_executesql @Sql0,N'@ID2 varchar(10)',@ID2
 END
 fetch next from pcurr into @ID2,@VALUE2
end
close pcurr
deallocate pcurr


--ʹ����Namepage1���ݺ�����ɾ��,��������һֱ��Namepage1��,����������
DELETE FROM @Namepage1

--��ǰ���ѭ������������ѭ��core�㷨ɾ���ڵ�
SET @Core=@Core+1
END