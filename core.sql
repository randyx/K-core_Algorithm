USE KCORE
/*
定义表结构LINGSHI,作为统计每个药材节点的ps总值，
ID---药材名字 VALUE---对应药材的ps总值,
这个表始终存在，并随着节点的减少而不断变化,
节点被删除，这个表中节点信息也随着删除
*/
DECLARE @LINGSHI TABLE( 
					ID VARCHAR(10),
					VALUE INT
					)
/*
定义表结构 Namepage 为了保存将要删除的药材节点集合,
ID---药材名称
*/
DECLARE @Namepage TABLE(
					ID VARCHAR(10)
					)
/*
定义表结构Namepage1 配合上表使用，
保存与上表中的要删除节点相关联的节点，以达到动态改变LINGSHI表的目的
ID---药材名称
*/
DECLARE @Namepage1 TABLE(
					ID VARCHAR(10),
					VALUE INT
					)
/*
定义Cores表示是要执行的多少core算法,可以自己规定
*/
declare @Cores INT
set @Cores=1
/*
把原表里的数据复制给新的生成表,这个表就是最好产生的结果
*/
DECLARE @sql varchar(20) 
set @sql='core'+cast(@Cores as varchar(2)) 
Exec('create table '+@sql+'(lh varchar(10),rh varchar(10),ps smallint)')
Exec('insert into ' +@sql+ ' SELECT * FROM dbo.D23_ZY_lt_ps')


INSERT INTO @Namepage(ID) SELECT DISTINCT rh from dbo.D23_ZY_lt_ps  --rh插入Namepage中，这时Namepage表做了一个过渡的作用
INSERT INTO @Namepage(ID) SELECT DISTINCT lh from dbo.D23_ZY_lt_ps  --lh插入Namepage中

/*
下面一段是把所有药材提取出来，装在LINGSHI表中，并把ps值初始化为0
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
下面一段是统计每个药材总的ps值，并更新数据到LINGSHI表中
更新数据的VALUE是把ps值进行累加,如果要实现简单关系,无ps值时
直接用常数1来代替ps值即可
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
定义Core作用使算法循环执行,从1开始到Cores-1
Cores等于1时,while不满足,直接跳出循环
Cores>1时,进行循环递归执行
*/
declare @Core INT
set @Core=1
while @Core<=@Cores-1
begin

/*
把Namepage原来的数据清空，然后再装入要删除药材节点的ID
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
测试观察执行效果,可以省去
*/
SELECT ID,VALUE FROM @LINGSHI
SELECT ID FROM @Namepage

/*
找到与要删除药材节点相关联的节点，放到Namepage1表中，为下一步更新LINGSHI表做准备
并在找到后，把要删除的药材节点从LINGSHI表和原表D23_ZY_lt_ps中删去
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
 --定义deletesql删除语句,处理动态sql,删除我们的生成表的节点
 declare @deleteSql nvarchar(100)
 set @deleteSql='DELETE FROM '+@sql+' WHERE lh=@delname OR rh=@delname'
 EXEC sp_executesql @deleteSql,N'@delname varchar(10)',@delname
 fetch next from pcurr into @delname
end
close pcurr
deallocate pcurr

/*
更新LINGSHI表中的ps值
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
这个循环用来判定删除关联节点后,ps值为0的情况，但是在@sql表中已被删除，没有这个节点
可实际上，这个节点是存在的，只是孤立的一个点
依次遍历LINGSHI表，value=0时，把这个点加入到生成表@sql表中
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
 --插入数据rh=0.表示没有节点与之相连，它是一个孤立的节点
 set @Sql0='INSERT INTO '+@sql+' (lh,rh,ps)values(@ID2,0,0)'
 EXEC sp_executesql @Sql0,N'@ID2 varchar(10)',@ID2
 END
 fetch next from pcurr into @ID2,@VALUE2
end
close pcurr
deallocate pcurr


--使用完Namepage1数据后立即删除,避免数据一直在Namepage1中,发生错误结果
DELETE FROM @Namepage1

--最前面的循环条件，进行循环core算法删除节点
SET @Core=@Core+1
END