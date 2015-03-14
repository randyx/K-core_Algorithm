USE GC
/*
--定义表结构NodeSumnum,作为统计每个药材节点的ps总值，
ID---药材名字 VALUE---对应药材的ps总值,
这个表始终存在，并随着节点的减少而不断变化,
节点被删除，这个表中节点信息也随着删除
*/
DECLARE @NodeSumnum TABLE( 
					ID VARCHAR(10),
					VALUE INT
					)
/*
定义表结构 DelNode 为了保存将要删除的药材节点集合,
ID---药材名称
*/
DECLARE @DelNode TABLE(
					ID VARCHAR(10)
					)
/*
定义表结构LinkDelNode 配合上表使用，
保存与上表中的要删除节点有连接的节点，以达到动态改变NodeSumnum表的目的
ID---药材名称
*/
DECLARE @LinkDelNode TABLE(
					ID VARCHAR(10),
					VALUE INT
					)
/*
把原表里的数据复制给新表CoreTemp中，这是个临时表，以后生成的很多Core-n表都要依赖于这个表
*/
DECLARE @sql varchar(20)
SET @sql='CoreTemp'
Exec('CREATE TABLE '+@sql+'(lh varchar(10),rh varchar(10),ps smallint)')
Exec('INSERT INTO ' +@sql+ ' SELECT * FROM D23_ZY_lt_ps')


INSERT INTO @DelNode(ID) SELECT DISTINCT rh from CoreTemp  --rh插入DelNode中，这时DelNode表做了一个过渡的作用
INSERT INTO @DelNode(ID) SELECT DISTINCT lh from CoreTemp  --lh插入DelNode中

/*
下面一段是把所有药材提取出来，装在NodeSumnum表中，并把ps值初始化为0
*/

DECLARE @name varchar(10)
DECLARE pcurr cursor for select DISTINCT ID from @DelNode
OPEN pcurr
FETCH  NEXT FROM pcurr INTO @name
WHILE (@@fetch_status = 0)
BEGIN
	INSERT INTO @NodeSumnum(ID,VALUE) VALUES(@name,0)
	FETCH  NEXT FROM pcurr INTO @name
END
CLOSE pcurr
DEALLOCATE pcurr

/*
下面一段是统计每个药材总的ps值，并更新数据到NodeSumnum表中
更新数据的VALUE是把ps值进行累加,如果要实现简单关系,无ps值时
直接用常数1来代替ps值即可
*/
DECLARE @lname varchar(10)
DECLARE @rname varchar(10)
DECLARE @iid int
DECLARE pcurr cursor for select lh,rh,ps from CoreTemp
OPEN pcurr
FETCH  NEXT FROM pcurr INTO @lname,@rname,@iid
WHILE (@@fetch_status = 0)
BEGIN
	UPDATE @NodeSumnum SET VALUE= VALUE+@iid WHERE ID = @lname OR ID=@rname
	FETCH  NEXT FROM pcurr INTO @lname,@rname,@iid
END
CLOSE pcurr
DEALLOCATE pcurr

/*
定义Core作用使算法循环执行,从1开始到Cores.进行循环递归执行
*/
DECLARE @Core INT
DECLARE @TF AS INT
SET @Core=0
SET @TF=1
--WHILE @Core<=@Cores
WHILE @TF > 0
BEGIN

/*
把DelNode原来的数据清空，然后再装入所有要删除药材节点的ID（度<=Core的）
@Ai初始化为0，因为可能删除，产生孤立的节点，这时我们把它度认为给0，为了下一步删除
*/
	
	DELETE FROM @DelNode
	DECLARE @Ai INT
	SET @Ai=0
	WHILE @Ai<=@Core
	BEGIN
		INSERT INTO @DelNode(ID) SELECT ID FROM @NodeSumnum WHERE VALUE = @Ai
		SET @Ai=@Ai+1
	END

/*
测试观察执行效果,可以省去
*/
	SELECT ID,VALUE FROM @NodeSumnum
	SELECT ID FROM @DelNode

/*
找到与要删除药材节点相关联的节点，放到LinkDelNode表中，为下一步更新NodeSumnum表做准备
并在找到后，把要删除的药材节点从NodeSumnum表和之前创建的@sql生成表中删去
*/
	DECLARE @delname varchar(10)
	DECLARE pcurr cursor for select ID from @DelNode
	OPEN pcurr
	FETCH  NEXT FROM pcurr INTO @delname
	WHILE (@@fetch_status = 0)
	BEGIN
		INSERT INTO @LinkDelNode(ID,VALUE) SELECT lh,ps FROM CoreTemp WHERE rh=@delname
		INSERT INTO @LinkDelNode(ID,VALUE) SELECT rh,ps FROM CoreTemp WHERE lh=@delname
		DELETE FROM @NodeSumnum WHERE ID=@delname
		--定义deletesql删除语句,处理动态sql,删除我们的生成表@sql的节点
		DECLARE @deleteSql nvarchar(100)
		SET @deleteSql='DELETE FROM '+@sql+' WHERE lh=@delname OR rh=@delname'
		EXEC sp_executesql @deleteSql,N'@delname varchar(10)',@delname
		FETCH  NEXT FROM pcurr INTO @delname
	END
	CLOSE pcurr
	DEALLOCATE pcurr

/*
更新NodeSumnum表中的ps值
*/
	DECLARE @delname1 varchar(10)
	DECLARE @VALUE1 INT
	DECLARE pcurr cursor for select ID,VALUE from @LinkDelNode
	OPEN pcurr
	FETCH  NEXT FROM pcurr INTO @delname1,@VALUE1
	WHILE (@@fetch_status = 0)
	BEGIN
		UPDATE @NodeSumnum SET VALUE= VALUE-@VALUE1 WHERE ID = @delname1
		FETCH  NEXT FROM pcurr INTO @delname1,@VALUE1
	END
	CLOSE pcurr
	DEALLOCATE pcurr

/*
这个循环用来判定删除关联节点后,ps值为0的情况，但是在@sql表中已被删除，没有这个节点
可实际上，这个节点是存在的，只是孤立的一个点
依次遍历NodeSumnum表，value=0时，把这个点加入到生成表@sql表中
*/
	DECLARE @ID2 varchar(10)
	DECLARE @VALUE2 INT
	DECLARE pcurr cursor for select ID,VALUE from @NodeSumnum
	OPEN pcurr
	FETCH  NEXT FROM pcurr INTO @ID2,@VALUE2
	WHILE (@@fetch_status = 0)
	BEGIN
		IF @VALUE2=0
		BEGIN
		DECLARE @Sql0 nvarchar(100)
		--插入数据rh=0.表示没有节点与之相连，它是一个孤立的节点
		SET @Sql0='INSERT INTO '+@sql+' (lh,rh,ps)values(@ID2,0,0)'
		EXEC sp_executesql @Sql0,N'@ID2 varchar(10)',@ID2
		END
	FETCH  NEXT FROM pcurr INTO @ID2,@VALUE2
	END
	CLOSE pcurr
	DEALLOCATE pcurr

--使用完LinkDelNode数据后立即删除,避免数据一直在LinkDelNode中,发生错误结果
	DELETE FROM @LinkDelNode

	DECLARE @TableName varchar(20)
	SET @TableName='Core'+cast(@Core+1 as varchar(2)) 
	Exec('CREATE TABLE '+@TableName+'(lh varchar(10),rh varchar(10),ps smallint)')
	Exec('INSERT INTO ' +@TableName+ ' SELECT * FROM ' +@sql)
	
--判断CoreA的是否还有数据，如何被删完全，@tf=0，跳出循环
	SELECT @TF = COUNT(*) FROM CoreTemp
	--IF (@TF = 0)
	--	BREAK;
--最前面的大循环条件，进行下一个循环删除
	SET @Core=@Core+1
END
DROP TABLE CoreTemp