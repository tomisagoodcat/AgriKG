from pyltp  import SentenceSplitter
from pyltp import Segmentor
import pymssql
import os
from pyltp import Postagger
from pyltp import NamedEntityRecognizer
#连接neo4j
from py2neo import Graph,Node,Relationship
#连接图数据库
graph = Graph(
    "http://localhost:7474", 
    username="neo4j", 
    password="tomis1cat")
 
for r in graph.run('MATCH (n:DimOrigin) RETURN n LIMIT 25'):
    print(r[0])

#连接关系数据库
conn = pymssql.connect(host='TOMISASCAT\TOMISAGOODCAT',user='sa',
                       password='Tomis1cat',database='IASDB4Neo',
                      charset="utf8")
cursor = conn.cursor(as_dict=True)
 

sql = 'SELECT TOP (1000) [workorder_id] ,[EventContent] FROM [IASDB4Neo].[dbo].[EventView]'
cursor.execute(sql)
 
rs = cursor.fetchall()

#首先实现关系数据库中数据到图数据库数据的转换实验

#建立语义网设计的事件发生于时间，事件由某一工作人员接到，事件发生于某一个城市区域，事件由工作人员分发到对应部门处理，事件由工作人员进行回访的一个语义过程





#由于本身系统存储的是单独句子，故分句意义不是很大。
#for row in rs:
    #print(row['EventContent'])
 #   sents=SentenceSplitter.split(row['EventContent'])
   # print('.\n'.join(sents))
#word='报警人称：在七街发生纠纷，已转入都匀公安110'
#sents=SentenceSplitter.split('报警人称：在七街发生纠纷，已转入都匀公安110')
#print('.\n'.join(sents))
#循环分词s

ldir='C:\\Users\\tomis\\source\\repos\\AgriKG\\AgriKG\\ltp\\cws.model'# 分词模型路径，模型名称为`cws.model`
 
dicdir='C:\\Users\\tomis\\source\\repos\\AgriKG\\dic'#使用外部字典,定义外部词典目录#

for row in rs:
    segmentor = Segmentor() # 初始化实例
    segmentor.load_with_lexicon(ldir,dicdir)  # 加载模型，包括本地词典
    wordOriginal=row['EventContent']
    word=segmentor .segment(row['EventContent'])#读取对应记录事件的列，并讲下分词
    word=list(word)#将分成后的文本转换成为list
    print(type(word))
    #print(word)
    segmentor.release()  # 释放模型
    #词性标注
    pdir='C:\\Users\\tomis\\source\\repos\\AgriKG\\AgriKG\\ltp\\pos.model'
   
    pos=Postagger()
    pos.load(pdir)
    postags=pos.postag(word)#基于分词得到的list讲下词性标注
    postags=list(postags)
    data={"word":word,"tags":postags}
    type(data)
    #print(data["word"],data["tags"])
    pos.release()
  

#wp代表标点符号，d代表副词，p代表介词，ns代表地理名词
 
   ### 命名实体识别
    nermodel='C:\\Users\\tomis\\source\\repos\\AgriKG\\AgriKG\\ltp\\ner.model' 
    reg=NamedEntityRecognizer()# 初始化命名实体实例
    reg.load(nermodel)
    netags=reg.recognize(word,postags)#对分词、词性标注得到的数据进行实体标识
    netags=list(netags)
    data={"”reg":netags,"word":word,"tags":postags}
    print(wordOriginal)
    print(data)
    reg.release()
      #增加循环实现地点名词实体的提取
    i = 0
    place = ''     #地点实体
    flag = 0      #标记变量
    while i<len(postags):
        #print (postags[i], type(postags[i]))
        if flag==0 and postags[i]=='ns':      #从前遍历名词 "文峰家园"为n 故第一个为名词
            place = word[i]
            flag = 1
        elif flag==1 and postags[i]=='ns': #地点实体拼接
            place = place + word[i]
        elif flag==1:                                #当前词性不为地点名词 结束循环
            flag = 0
            break
        #print(u"测试:", place)
        i = i + 1

    #如果不包含ns地点名词, 被标注为n nt等名词, 则需要进行重新判断第一个名词为地点
    if place=="":
        i = 0
        while i<len(word): #固定数字 否则len(postags)会出现 ['接到', '110', '挪', '车', '电话']  地点为 车
            #print (postags[i], type(postags[i]))
            #j  公检法
            #['在', '中兴大厦', '2', '单元', '舞蹈班', '噪音', '扰民']
            #['p', 'j', 'm', 'n', 'n', 'n', 'v']
            if postags[i]=='n' or postags[i]=='nt'  or postags[i]=='j':      #从前遍历名词 "文峰家园"为n 故第一个为名词
                place = word[i]
                break
            #print(u"测试:", place)
            i = i + 1

    #如果地点实体为空，设置为NO
    if place=="":
        place = "NO"
    print (u'地点实体:', place)
        
    
    #增加循环实现事件动词的提取
    k = len(word) - 1
    event = ''  #事件
    flag = 0
    while k>=0:
        if flag==0 and postags[k]=='v':   #从后往前遍历动词即为事件
            event = word[k]
            flag = 1
            break 
        k = k - 1

    #动词+动词 动词+介词构成事件
    #'被', '狗', '咬'   'p', 'n', 'v'
    j = k - 1
    if postags[j]=='v' or  postags[j]=='p':
        event = word[j] + event
    
    #动词+名词构成事件
    j = k + 1
    while j<len(word):
        if postags[j]=='n' or postags[j]=='q' or postags[j]=='v' or postags[j]=='p' or postags[j]=='nl':
            #接到 挪 车 电话 ['v', 'q', 'n', 'n']
            event = event + word[j]
        j = j + 1

    #如果地点实体为空，设置为NO
    if event=="":
        event = "NO"
    print (u'事件:', event)
    
    #SQL语句插入数据库表
    
    
    data = {"word": word, "tags": postags}
    
    #type(data)
    #print(data["word"],data["tags"])
    pos.release()
  
    #wp代表标点符号，d代表副词，p代表介词，ns代表地理名词
 
    # 命名实体识别
    nermodel='AgriKG\\ltp\\ner.model' 
    reg=NamedEntityRecognizer() # 初始化命名实体实例
    reg.load(nermodel)
    netags=reg.recognize(word,postags) #对分词、词性标注得到的数据进行实体标识
    netags=list(netags)
    #实体识别结果
    data={"reg": netags,"word":word,"tags":postags}
    print(data)
    reg.release()

    #最终输出结果
    data={"wordOriginal": wordOriginal,"place": place,"event": event}

    print ('Output', data)
    print(" ")
    a= Node("Location", name=place)
    b = Node("Event", name=event)
    ab = Relationship(b, "Happen_on", a)
    graph.create(a)
    graph.create(b)
    graph.create(ab)
  
 

