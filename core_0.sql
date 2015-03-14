USE GC
/*
--�����ṹNodeSumnum,��Ϊͳ��ÿ��ҩ�Ľڵ��ps��ֵ��
ID---ҩ������ VALUE---��Ӧҩ�ĵ�ps��ֵ,
�����ʼ�մ��ڣ������Žڵ�ļ��ٶ����ϱ仯,
�ڵ㱻ɾ����������нڵ���ϢҲ����ɾ��
*/
DECLARE @NodeSumnum TABLE( 
					ID VARCHAR(10),
					VALUE INT
					)
/*
�����ṹ DelNode Ϊ�˱��潫Ҫɾ����ҩ�Ľڵ㼯��,
ID---ҩ������
*/
DECLARE @DelNode TABLE(
					ID VARCHAR(10)
					)
/*
�����ṹLinkDelNode ����ϱ�ʹ�ã�
�������ϱ��е�Ҫɾ���ڵ������ӵĽڵ㣬�Դﵽ��̬�ı�NodeSumnum���Ŀ��
ID---ҩ������
*/
DECLARE @LinkDelNode TABLE(
					ID VARCHAR(10),
					VALUE INT
					)
/*
��ԭ��������ݸ��Ƹ��±�CoreTemp�У����Ǹ���ʱ���Ժ����ɵĺܶ�Core-n��Ҫ�����������
*/
DECLARE @sql varchar(20)
SET @sql='CoreTemp'
Exec('CREATE TABLE '+@sql+'(lh varchar(10),rh varchar(10),ps smallint)')
Exec('INSERT INTO ' +@sql+ ' SELECT * FROM D23_ZY_lt_ps')


INSERT INTO @DelNode(ID) SELECT DISTINCT rh from CoreTemp  --rh����DelNode�У���ʱDelNode������һ�����ɵ�����
INSERT INTO @DelNode(ID) SELECT DISTINCT lh from CoreTemp  --lh����DelNode��

/*
����һ���ǰ�����ҩ����ȡ������װ��NodeSumnum���У�����psֵ��ʼ��Ϊ0
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
����һ����ͳ��ÿ��ҩ���ܵ�psֵ�����������ݵ�NodeSumnum����
�������ݵ�VALUE�ǰ�psֵ�����ۼ�,���Ҫʵ�ּ򵥹�ϵ,��psֵʱ
ֱ���ó���1������psֵ����
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
����Core����ʹ�㷨ѭ��ִ��,��1��ʼ��Cores.����ѭ���ݹ�ִ��
*/
DECLARE @Core INT
DECLARE @TF AS INT
SET @Core=0
SET @TF=1
--WHILE @Core<=@Cores
WHILE @TF > 0
BEGIN

/*
��DelNodeԭ����������գ�Ȼ����װ������Ҫɾ��ҩ�Ľڵ��ID����<=Core�ģ�
@Ai��ʼ��Ϊ0����Ϊ����ɾ�������������Ľڵ㣬��ʱ���ǰ�������Ϊ��0��Ϊ����һ��ɾ��
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
���Թ۲�ִ��Ч��,����ʡȥ
*/
	SELECT ID,VALUE FROM @NodeSumnum
	SELECT ID FROM @DelNode

/*
�ҵ���Ҫɾ��ҩ�Ľڵ�������Ľڵ㣬�ŵ�LinkDelNode���У�Ϊ��һ������NodeSumnum����׼��
�����ҵ��󣬰�Ҫɾ����ҩ�Ľڵ��NodeSumnum���֮ǰ������@sql���ɱ���ɾȥ
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
		--����deletesqlɾ�����,����̬sql,ɾ�����ǵ����ɱ�@sql�Ľڵ�
		DECLARE @deleteSql nvarchar(100)
		SET @deleteSql='DELETE FROM '+@sql+' WHERE lh=@delname OR rh=@delname'
		EXEC sp_executesql @deleteSql,N'@delname varchar(10)',@delname
		FETCH  NEXT FROM pcurr INTO @delname
	END
	CLOSE pcurr
	DEALLOCATE pcurr

/*
����NodeSumnum���е�psֵ
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
���ѭ�������ж�ɾ�������ڵ��,psֵΪ0�������������@sql�����ѱ�ɾ����û������ڵ�
��ʵ���ϣ�����ڵ��Ǵ��ڵģ�ֻ�ǹ�����һ����
���α���NodeSumnum��value=0ʱ�����������뵽���ɱ�@sql����
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
		--��������rh=0.��ʾû�нڵ���֮����������һ�������Ľڵ�
		SET @Sql0='INSERT INTO '+@sql+' (lh,rh,ps)values(@ID2,0,0)'
		EXEC sp_executesql @Sql0,N'@ID2 varchar(10)',@ID2
		END
	FETCH  NEXT FROM pcurr INTO @ID2,@VALUE2
	END
	CLOSE pcurr
	DEALLOCATE pcurr

--ʹ����LinkDelNode���ݺ�����ɾ��,��������һֱ��LinkDelNode��,����������
	DELETE FROM @LinkDelNode

	DECLARE @TableName varchar(20)
	SET @TableName='Core'+cast(@Core+1 as varchar(2)) 
	Exec('CREATE TABLE '+@TableName+'(lh varchar(10),rh varchar(10),ps smallint)')
	Exec('INSERT INTO ' +@TableName+ ' SELECT * FROM ' +@sql)
	
--�ж�CoreA���Ƿ������ݣ���α�ɾ��ȫ��@tf=0������ѭ��
	SELECT @TF = COUNT(*) FROM CoreTemp
	--IF (@TF = 0)
	--	BREAK;
--��ǰ��Ĵ�ѭ��������������һ��ѭ��ɾ��
	SET @Core=@Core+1
END
DROP TABLE CoreTemp