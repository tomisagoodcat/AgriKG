#首先实现将关系数据库中不同维度表数据进行图数据库储存，关键在与转换不同实体的层级关系subclass

from pyltp  import SentenceSplitter
from pyltp import Segmentor
import pymssql
import os
from pyltp import Postagger
from pyltp import NamedEntityRecognizer
#连接neo4j
from py2neo import Graph,Node,Relationship
import pandas
#连接图数据库
graph = Graph(
    "http://localhost:11009", 
    username="neo4j", 
    password="tomis1cat")

#连接关系数据库
conn = pymssql.connect(host='TOMISASCAT\TOMISAGOODCAT',user='sa',
                       password='Tomis1cat',database='IASDB4Neo',
                      charset="utf8")
cursor = conn.cursor(as_dict=True)

choice=input("输入转换表 1：dept，2：eventtype，3：location，4：EventView:")
print(choice)
def department2Neo():
##构建deportment节点，并保留其上级部门作为属性
     sql = 'SELECT   * FROM [IASDB4Neo].[dbo].[DEPT] '
     cursor.execute(sql)
     rs = cursor.fetchall()
     for row in rs:
         deptNode=Node("Servicer",name=row['DEPT_NAME'],par_id=row['P_DEPT_ID'],id=row['DEPT_ID'])
         graph.create(deptNode)
    #对应的部门关系建立match (a:Servicer),(b:Servicer) where a.par_id=b.id merge (a)-[:subClass]->(b)
     print("dept2Neo is done")
     return;
 


def eventType2Neo():
    # #构建eventType节点，描述不同事件的类型，以及事件的父类型DEPT_ID'
     sql = 'SELECT   * FROM [IASDB4Neo].[dbo].[EventType]'
     cursor.execute(sql)
     rs = cursor.fetchall()
     for row in rs:
         EventClassNode=Node("EventClass",name=row['appeal_type_name'],par_id=row['p_appeal_type'],id=row['appeal_type'],keywords=row['appeal_keywords'],classify=row['p_classify'])
         graph.create(EventClassNode)
     print("eventtype2Neo is done")
     return;
    
    #对应的部门关系建立match (a:Servicer),(b:Servicer) where a.par_id=b.id merge (a)-[:subClass]->(b)
  #构建eventType节点，描述不同事件的类型，以及事件的父类型

 
def location2Neo():

  #构建Locaiton节点，分为州（state），县市(city),乡镇办事处（village）
  sql='select * from city'
  cursor.execute(sql)
  rs = cursor.fetchall()
  for row in rs:
      CityClass=Node("City",name=row['city_name'],par_id='522700',id=row['area_code'],type="location")
      graph.create(CityClass)
  sql='select * from [IASDB4Neo].[dbo].[DimLoation]'
  cursor.execute(sql)
  rs = cursor.fetchall()
  for row in rs:
    LocationClass=Node("Village",name=row['locationName'],par_id=row['city_code'],id=row['area_code'],type='location',phone=row['phone'])
    graph.create(LocationClass)
 
  print("locaiton to Neo4j is done")
  return;
       
 
def EventView2Neo():
    # 构建事件关联，来自于eventview表 ,首先不进行content的实体识别等操作，而是将content也作为一个节点，在后期对conent中数据进行处理，再进一步构建实体关系
    sql='select * from EventView'
    cursor.execute(sql)
    rs = cursor.fetchall()
    i=1
    for row in rs:
       EventClass=Node("Event",name=row['EventContent'],id=row['workorder_id'],content=row['workorder_content'],category=row['EventType'],
                    area_code=row['area_code'],typeid=row['workorder_field'],state=row['workorder_state_desc'],
                    happenYear=row['happenYear'],
                    happenMonth=row['happenMonth'],
                    happenHour=row['happenHour'],
                    happenMi=row['happenMi'],
                    ClosedYear=row['ClosedYear'],
                    ClosedMonth=row['ClosedMonth'],
                    ClosedHour=row['ClosedHour'],
                    ClosedMi=row['ClosedMi'],
                    
                    
                    
                    happenDatetime=row['HappenDateNeotime'],closedDatetime=row['ClosedDateNeotime'],subDatetime=row['SubDateNeotime']
                    ,SubWorker=row['workorder_sub_man'],ClosedWorker=row['workorder_closed_man'],totoalCosttime=row['TotalCostTime'],subCosttime=row['SubCostTime']

                    
                    
                    
                    )
       graph.create(EventClass);
       print (i)
       i=i+1
print("done")
 
 #构建servicer处理event关系，首先从数据库中读取关系表，读取每一个
def bulid_realtion_event_maincharge():
     sql='SELECT   [plan_id],e.[workorder_id] \
      ,[dept_id] \
      ,[DEPT_NAME] \
      ,[is_master] \
      ,[is_accept] \
      ,[accept_time] \
      ,[dept_sub_time] \
      ,[__timestamp] \
      ,[create_user] \
      ,[create_source] \
      ,[dept_closed] \
      ,[dept_closed_name] \
      ,[dept_closed_time],TotalCostTime,SubCostTime \
  FROM [IASDB4Neo].[dbo].[MainCharge] as m inner join EventView as e \
  on e.workorder_id=m.workorder_id'
     cursor.execute(sql)
     rs = cursor.fetchall()
     i=1
     for row in rs:
         workid=row['workorder_id']
         depid=row['dept_id']
         if row["TotalCostTime"] is not None:
             cypher="MATCH (e:Event),(s:Servicer) where e.id='"+row['workorder_id']+"' and s.id='"+row['dept_id']+"' merge (s)-[ :MainInCharge {costTime:"+str(row["TotalCostTime"])+",Closedstate:1}]->(e)"
         else:
              cypher="MATCH (e:Event),(s:Servicer) where e.id='"+row['workorder_id']+"' and s.id='"+row['dept_id']+"' merge (s)-[ :MainInCharge {Closedstate:0}]->(e)"
         graph.run(cypher)
         i+=1
         print(i)
#bulid_realtion_event_maincharge()
#print("everything is done:)")

 #构建servicer处理event关系，首先从数据库中读取关系表，读取每一个subcharge关系建立各个部门辅助实现业务，同时建立部门与部门之间得Assist关系
def bulid_realtion_event_subcharge():
     sql='SELECT   [plan_id],e.[workorder_id] \
      ,[dept_id] \
      ,[DEPT_NAME] \
      ,[is_master] \
      ,[is_accept] \
      ,[accept_time] \
      ,[dept_sub_time] \
      ,[__timestamp] \
      ,[create_user] \
      ,[create_source] \
      ,[dept_closed] \
      ,[dept_closed_name] \
      ,[dept_closed_time],TotalCostTime,SubCostTime \
  FROM [IASDB4Neo].[dbo].[SubCharge] as m inner join EventView as e \
  on e.workorder_id=m.workorder_id'
     cursor.execute(sql)
     rs = cursor.fetchall()
     i=1
     for row in rs:
         workid=row['workorder_id']
         depid=row['dept_id']
         if row["TotalCostTime"] is not None:
             cypher="MATCH (e:Event),(s:Servicer) where e.id='"+row['workorder_id']+"' and s.id='"+row['dept_id']+"' merge (s)-[ :SubInCharge {costTime:"+str(row["TotalCostTime"])+",Closedstate:1}]->(e)"
         else:
              cypher="MATCH (e:Event),(s:Servicer) where e.id='"+row['workorder_id']+"' and s.id='"+row['dept_id']+"' merge (s)-[ :SubInCharge {Closedstate:0}]->(e)"
         graph.run(cypher)
         i+=1
         print(i)

print("everything is done:)")