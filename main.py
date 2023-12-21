import time
from langchain import PromptTemplate, LLMChain
from langchain.chains.combine_documents.stuff import StuffDocumentsChain
from langchain.text_splitter import CharacterTextSplitter
# from langchain.retrievers import KNNRetriever
# from langchain.embeddings import SentenceTransformerEmbeddings
from flask import Flask, Response, request, stream_with_context, jsonify
import os
import requests
import copy
import openai
import numpy as np
from langchain.schema import Document
from openai import OpenAI
import random
from serpapi import search
from bs4 import BeautifulSoup
from dbutils.pooled_db import PooledDB
from urllib.parse import urlparse
from datetime import datetime, timedelta
import uuid
import pymysql
import tiktoken
import logging
# os.environ["http_proxy"] = "http://localhost:7890"
# os.environ["https_proxy"] = "http://localhost:7890"
from neo4j_ import Neo4jGraphStore
neo4jUsername = os.environ["neo4jUsername"]
neo4jPassword = os.environ["neo4jPassword"]
neo4jLink = os.environ["neo4jLink"]
serpapi_key = os.environ["serpapi_key"]
rootDataApi = os.environ["rootDataApi"]
graph_store = Neo4jGraphStore(neo4jUsername, neo4jPassword, neo4jLink)
username = os.environ["DBusername"]
password = os.environ["DBpassword"]
host = os.environ["DBhostname"]
port = os.environ["DBport"] 
database = os.environ["DBdbname"] 
config = {
    'user': username,
    'password': password,
    'host': host,
    'port': port,
    'database': database
}
# 连接池配置
pool = PooledDB(
    creator=pymysql,  # 使用的数据库模块
    maxconnections=10,  # 连接池最大连接数量
    mincached=2,       # 初始化时，连接池中至少创建的空闲的连接
    maxcached=5,       # 连接池中最多闲置的连接
    maxshared=3,       # 连接池中最多共享的连接数量
    blocking=True,     # 连接池中如果没有可用连接后是否阻塞等待
    host=host,
    port=int(port),
    user=username,
    password=password,
    database=database,
    ssl={"ssl_mode":"VERIFY_IDENTITY",
        "ssl_accept":"strict"
    }
)
client = OpenAI()
app = Flask(__name__)
# openai.api_key = os.environ["OPENAI_API_KEY"]
# 配置日志记录
logging.basicConfig(filename='entity_search.log', level=logging.DEBUG, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
def getRootData(entity):
    # 请求头部信息
    headers = {
        "apikey": rootDataApi,
        "language": "en",
        "Content-Type": "application/json"
    }
    getidUrl = "https://api.rootdata.com/open/ser_inv"
    getidQuery = {"query":entity}
    response = requests.post(getidUrl, headers=headers, json=getidQuery)
    project_id = None
    org_id = None
    if response.status_code==200:
        res_json = response.json()
        if res_json['result']==200:
            if res_json['data'][0]['type']==1:
                project_id = res_json['data'][0]['id']
            else:
                org_id = res_json['data'][0]['id']
        else:
            return False,2
    else:
        return False,2
    if project_id:
        # 请求体参数
        data_project = {
            "project_id": project_id,
            "include_team": True,
            "include_investors": True
        }
        url = "https://api.rootdata.com/open/get_item"
        response = requests.post(url, headers=headers, json=data_project)
        res = {}
        if response.status_code==200:
            res_json = response.json()
            if res_json['result']==200:
                res = res_json['data']
            else:
                return False,1
        else:
            return False,1
        return res,1
    elif org_id:
        # 请求体参数
        data_project = {
            "org_id": org_id,
            "include_team": True,
            "include_investments": True
        }
        url = "https://api.rootdata.com/open/get_org"
        response = requests.post(url, headers=headers, json=data_project)
        res = {}
        if response.status_code==200:
            res_json = response.json()
            if res_json['result']==200:
                res = res_json['data']
            else:
                return False,2
        else:
            return False,2
        return res,2


def check_url_exists(url, data=None):
    conn = pool.connection()
    cursor = conn.cursor()
        # 当前时间
    current_time = datetime.now()
    # 30天前的时间
    thirty_days_ago = current_time - timedelta(days=30)
    # 转换为MySQL格式的字符串
    thirty_days_ago_str = thirty_days_ago.strftime('%Y-%m-%d %H:%M:%S')

    query = "SELECT summary FROM rootData_summary WHERE url = %s AND created_at > %s"
    cursor.execute(query, (url, thirty_days_ago_str))
    res =  cursor.fetchone()
    cursor.close()
    conn.close()
    if data and res!=None:
        logging.info(res)
        return res[0]
    elif res!=None:
        return True
    return False


# 数据库连接
def insert_or_update_data(url, summary):
    conn = pool.connection()
    cursor = conn.cursor()
    current_time = datetime.now()
    create_at = current_time.strftime('%Y-%m-%d %H:%M:%S')
    generated_uuid = str(uuid.uuid4())
    if check_url_exists(url):
        # URL存在，更新数据
        update_query = "UPDATE rootData_summary SET summary = %s, created_at = %s WHERE url = %s"
        cursor.execute(update_query, (summary, create_at, url))
        print(f"Data {url} updated 0.")
    else:
        # URL不存在，插入新数据
        insert_query = "INSERT INTO rootData_summary (id, url, summary, created_at, is_source_answer) VALUES (%s, %s, %s,%s,%s)"
        cursor.execute(insert_query, (generated_uuid,url, summary, create_at,0))
        print(f"Data {url} inserted 0.")
    # sql = """
    #     INSERT INTO rootData_summary (id, url, summary, created_at, is_source_answer)
    #     VALUES (%s, %s, %s, %s, %s)
    #     ON DUPLICATE KEY UPDATE
    #         summary = VALUES(summary),
    #         created_at = VALUES(created_at),
    #         is_source_answer = VALUES(is_source_answer)
    # """
    # # sql = """
    # #     INSERT INTO rootData_summary (id, url, summary, created_at, is_source_answer)
    # #     VALUES (%s, %s, %s, %s, %s)
    # # """
    # cursor.execute(sql, (generated_uuid, url, summary, create_at, 0))
    conn.commit()
    cursor.close()
    conn.close()

def insert_data(entity, rootdata_summary, kg_summary=None, socialMedia_summary=None, socialMedia_url=None):
    conn = pool.connection()
    cursor = conn.cursor()
    id = str(uuid.uuid4())
    cursor.execute("INSERT INTO entity_summary (id, entity, rootdata_summary, kg_summary, socialMedia_summary, socialMedia_url) VALUES (%s, %s, %s, %s, %s, %s)", 
              (id, entity, rootdata_summary, kg_summary, socialMedia_summary, socialMedia_url))
    conn.commit()
    conn.close()

def update_data(entity, kg_summary=None, socialMedia_summary=None, socialMedia_url=None):
    conn = pool.connection()
    cursor = conn.cursor()
    if kg_summary:
        cursor.execute("UPDATE entity_summary SET kg_summary=%s WHERE entity=%s", 
                (kg_summary, entity))
    if socialMedia_summary:
        cursor.execute("UPDATE entity_summary SET socialMedia_summary=%s WHERE entity=%s", 
                (socialMedia_summary, entity))
    if socialMedia_url:
        list_as_string = ','.join(socialMedia_url)
        cursor.execute("UPDATE entity_summary SET socialMedia_url=%s WHERE entity=%s", 
                (list_as_string, entity))
    conn.commit()
    conn.close()

def query_data(entity,kg_summary=None, socialMedia_summary=None, socialMedia_url=None):
    conn = pool.connection()
    cursor = conn.cursor()
    cursor.execute("SELECT entity, rootdata_summary, kg_summary, socialMedia_summary, socialMedia_url FROM entity_summary WHERE entity = %s", (entity,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()
    return row

def getUrlSummary(urls):
    res = []
    logging.info("all need fetch url:",urls)
    my_headers = [
    "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:30.0) Gecko/20100101 Firefox/30.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14",
    "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Win64; x64; Trident/6.0)",
    'Mozilla/5.0 (Windows; U; Windows NT 5.1; it; rv:1.8.1.11) Gecko/20071127 Firefox/2.0.0.11',
    'Opera/9.25 (Windows NT 5.1; U; en)',
    'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)',
    'Mozilla/5.0 (compatible; Konqueror/3.5; Linux) KHTML/3.5.5 (like Gecko) (Kubuntu)',
    'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.0.12) Gecko/20070731 Ubuntu/dapper-security Firefox/1.5.0.12',
    'Lynx/2.8.5rel.1 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/1.2.9',
    "Mozilla/5.0 (X11; Linux i686) AppleWebKit/535.7 (KHTML, like Gecko) Ubuntu/11.04 Chromium/16.0.912.77 Chrome/16.0.912.77 Safari/535.7",
    "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:10.0) Gecko/20100101 Firefox/10.0 ",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Version/118.0.0 Safari/537.36",
    ]
    for url in urls:
        databaseGet = check_url_exists(url,data=True)
        if databaseGet:
            res.append(databaseGet)
            continue
        headers={'User-Agent': random.choice(my_headers)}
        try:
            response = requests.request("GET", url,headers=headers)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, "html.parser")
                main_text = ' '.join(p.get_text() for p in soup.find_all('p'))
                if soup.find("title"):
                    logging.info("Ok url:", url)
                    # res.append(main_text)
                    summary_url = getBriefSummary(main_text)
                    res.append(summary_url)
                    insert_or_update_data(url,summary_url)
                    continue
                else:
                    logging.info(f"{url} fail fetch content.")
            else:
                logging.info(f"First fetch url error:{url}, code:{response.status_code}")
                s=request.Session()
                s.trust_env=False
                response_ = requests.request("GET", url,headers=headers)
                s.trust_env=True
                if response_.status_code == 200:
                    soup = BeautifulSoup(response_.content, "html.parser")
                    main_text = ' '.join(p.get_text() for p in soup.find_all('p'))
                    if soup.find("title"):
                        logging.info("Ok url:", url)
                        # res.append(main_text)
                        summary_url = getBriefSummary(main_text)
                        res.append(summary_url)
                        insert_or_update_data(url,summary_url)
                        continue
                    else:
                        logging.info(f"{url} fail fetch content.")
                else:
                    logging.info(f"Status error url:{url}, code:{response_.status_code}")

        except:
            logging.info("Fail url:",url)
            pass
    return res


# the summary format is: intro+urls_summary
def getBriefSummary(input_text):
    try:
        PROMPT = """
        I want you to act as a summarizer. I will give you the text, and you should provide a summary. Your summary should be factual and segmented, covering the most important aspects of the text. Avoid statements like 'Based on the context, ...' or "
        'The context information ...' or anything along
        'those lines.'
        """
        content = f"The text is: {input_text}\n\n"
        completion = client.chat.completions.create(
        model="gpt-3.5-turbo-16k",                                          # 模型选择GPT 3.5 Turbo
        messages=[{"role": "system","content":PROMPT},
                {"role": "user", "content":content}],
        max_tokens = 2048
        )
        res = completion.choices[0].message.content
        return res
    except:
        return ""

# get sim
def _get_relevant_documents(texts, query, k, relevancy_threshold=None):
    index_embeds = openai.embeddings.create(model="text-embedding-ada-002", input=texts)
    index_embeds = np.array([item.embedding for item in index_embeds.data])
    query_embeds = openai.embeddings.create(model="text-embedding-ada-002", input=query)
    query_embeds = np.array([item.embedding for item in query_embeds.data])
    # calc L2 norm
    index_embeds = index_embeds / np.sqrt((index_embeds**2).sum(1, keepdims=True))
    query_embeds = query_embeds / np.sqrt((query_embeds**2).sum())

    similarities = index_embeds.dot(query_embeds.squeeze())
    sorted_ix = np.argsort(-similarities)

    denominator = np.max(similarities) - np.min(similarities) + 1e-6
    normalized_similarities = (similarities - np.min(similarities)) / denominator

    top_k_results = [
        Document(page_content=texts[row])
        for row in sorted_ix[0 : k]
        if (
            relevancy_threshold is None
            or normalized_similarities[row] >= relevancy_threshold
        )
    ]
    return top_k_results

def getSimUrl(pre_summary,texts, k):
    text = [i[0] for i in texts]
    # retriever = KNNRetriever.from_texts(text, SentenceTransformerEmbeddings(),k=k)
    # result = retriever.get_relevant_documents(pre_summary)
    result = _get_relevant_documents(text, pre_summary, k)
    text_json = {i[0]:i[1] for i in texts}
    simUrls = [text_json[r.page_content] for r in result]
    return result,simUrls

# find in google:
def googleSearch(entity, type = 1):
    # import serpapi.SerpApiClient
    Templates_project = [f"What's new about {entity}.", f"The news about {entity}."]
    Templates_org = [f"What's new about {entity}.", f"The news about {entity}."]

    res = []
    if type==1:
        for t in Templates_project:
            search_ = search({
                "q": t, 
                "engine": "google",
                "api_key":serpapi_key,
                "hl": "en",
                "gl": "us",
                "tbs": "qdr:y",
                })

    else:
        for t in Templates_org:
            search_ = search({
                "q": t, 
                "engine": "google",
                "api_key":serpapi_key,
                "hl": "en",
                "gl": "us",
                "tbs": "qdr:y",
                })
    data = search_
    snip = parse_snippets(data,3)
    if snip:
        res.extend(snip)
    return res


# google data parse
def parse_snippets(results, k):
    result_key_for_type = {
    "news": "news",
    "places": "places",
    "images": "images",
    "search": "organic_results",
    }
    type: Literal["news", "search", "places", "images"] = "search"

    snippets = []

    if results.get("knowledge_graph"):
        kg = results.get("knowledge_graph", {})
        title = kg.get("title")
        entity_type = kg.get("type")
        if entity_type:
            snippets.append(f"{title}: {entity_type}.")
        description = kg.get("description")
        if description:
            snippets.append(description)
        for attribute, value in kg.get("attributes", {}).items():
            snippets.append(f"{title} {attribute}: {value}.")

    for result in results[result_key_for_type[type]][: k]:
        if "snippet" in result:
            snippets.append([result["snippet"],result["link"]])
        # for attribute, value in result.get("attributes", {}).items():
        #     snippets.append(f"{attribute}: {value}.")

    if len(snippets) == 0:
        return False
    return snippets


    
def refineSummary(entity,pre_summary,input_text,socialMedia_summary,kg_subgraph=None,exist_kg=None):
    yield pre_summary
    yield "\n"
    if exist_kg:
        yield kg_subgraph
        yield "\n"
    elif kg_subgraph:
        stream_data = ""
        PROMPT = f"I need you to be a text sequence generation engineer. I will provide you with knowledge graph triples, which may involve multiple jumps of knowledge, and you will need to generate sequence text based on the triples I provide. This text is required to be as fluent as possible and contain enough information in the knowledge graph. Avoid statements like 'Based on the context, ...' or 'The context information ...' or anything along 'those lines.'"
        content = f"The knowledge graph triples are: {kg_subgraph}\n\n"
        completion = client.chat.completions.create(
            model="gpt-3.5-turbo-16k",                                          # 模型选择GPT 3.5 Turbo
            messages=[{"role": "system","content":PROMPT},
                    {"role": "user", "content":content}],
            max_tokens = 2048,
            stream=True
        )
        logging.info("yield kgSummary")
        for chunk in completion:
            data = chunk.choices[0].delta.content
            if data!=None:
                stream_data += data
                yield data
        update_data(entity=entity,kg_summary=stream_data)
    if len(input_text)>0:
        if socialMedia_summary:
            input_text.extend(socialMedia_summary)
        PROMPT = f"I want you to act as a summarizer. I will give you a preSummary and a text, and you should refine a summary to replenishment the preSummary based on the text given. Your summary should be factual and segmented, covering the most important aspects of the text. Your new summary should be the addictions to preSummary, and not same to it.\n"
        content = f"The preSummary is: {pre_summary} \n\n The text is: {input_text}\n\n"
        completion = client.chat.completions.create(
            model="gpt-3.5-turbo-16k",                                          # 模型选择GPT 3.5 Turbo
            messages=[{"role": "system","content":PROMPT},
                    {"role": "user", "content":content}],
            max_tokens = 2048,
            stream=True
        )

        for chunk in completion:
            data = chunk.choices[0].delta.content
            if data!=None:
                yield data
        
        # data = chunk.choices[0]['message']['content']
    # logging.info(stream_data) 
    # return stream_data

@app.route('/api/entitySearch',methods=['POST','GET'])
def entitySearch():
    entity = "ETH"
    k = 3
    socialUrlSummary = False
    exist_kg = None
    if request.method =="POST":
        entity = request.get_json().get("entity")
        k = request.get_json().get("k")
        socialUrlSummary = request.get_json().get("socialUrlSummary")
        logging.info(f"Post k is:{k}")
        logging.info(f"Post Entity is:{entity}")
        logging.info(f"Post socialUrlSummary is:{socialUrlSummary}")

    res,type_ = getRootData(entity)
    if res == False:
        return jsonify(data="Cannot get the entity from rootdata."), 500
    socialMedia_summary = ""
    if type_==1: # project entity
        # res:data is data; result is res code
        data = res
        intro = data['description']
        social_media_url = [value for key,value in data['social_media'].items() if value!="" and key!="X"]
        similar_project = [project["project_name"] for project in data['similar_project']]
        project_name = data["project_name"]
        investors = data["investors"]
        # check exist in mysql
        mysql_res = query_data(project_name)
        if mysql_res:
            entity, rootdata_summary, kg_summary, socialMedia_summary, socialMedia_url = mysql_res
            if kg_summary:
                exist_kg = True
                kg_subgraph = kg_summary
            if socialUrlSummary and socialMedia_summary==None:
                socialMedia_summary = getUrlSummary(social_media_url)
                update_data(entity=project_name,socialMedia_summary=socialMedia_summary)
        else:
            insert_data(entity=project_name,rootdata_summary=intro,socialMedia_url=social_media_url)
            if socialUrlSummary:
                socialMedia_summary = getUrlSummary(social_media_url)
                update_data(entity=project_name,socialMedia_summary=socialMedia_summary)
        if exist_kg==None:
            kg_subgraph = graph_store.get_rel_map([project_name])
            kg_subgraph[project_name].append([[project_name,"similar_project", sim] for sim in similar_project])

        # get the socialmedia Url summary
        googleData = googleSearch(project_name)
        simText,simUrl = getSimUrl(intro, googleData, k)
        simUrlSummary = getUrlSummary(simUrl)
        logging.info(f"kg_subgraph:{kg_subgraph}")
        logging.info(f"simUrlSummary:{simUrlSummary}")
        return Response(refineSummary(project_name,intro,simUrlSummary,socialMedia_summary,kg_subgraph,exist_kg), mimetype="text/event-stream")

    elif type_==2: # investor entity
        data = res
        description = data['description']
        investments = data['investments']
        establishment_date = data['establishment_date']
        org_name = data['org_name']
        team_members = data['team_members']
        social_media_url = [value for key,value in data['social_media'].items() if value!="" and key!="X"]

        # check exist in mysql
        mysql_res = query_data(org_name)
        if mysql_res:
            entity, rootdata_summary, kg_summary, socialMedia_summary, socialMedia_url = mysql_res
            if kg_summary:
                exist_kg = True
                kg_subgraph = kg_summary
            if socialUrlSummary and socialMedia_summary==None:
                socialMedia_summary = getUrlSummary(social_media_url)
                update_data(entity=org_name,socialMedia_summary=socialMedia_summary)
        else:
            insert_data(entity=org_name,rootdata_summary=description,socialMedia_url=social_media_url)
            if socialUrlSummary:
                socialMedia_summary = getUrlSummary(social_media_url)
                update_data(entity=org_name,socialMedia_summary=socialMedia_summary)
        if exist_kg == None:
            kg_subgraph = graph_store.get_rel_map([org_name])
            kg_subgraph[org_name].append([[org_name,"team_members", sim["name"]] for sim in team_members])
            kg_subgraph[org_name].append([[org_name,"INVESTED", sim["name"]] for sim in investments])

        # get the socialmedia Url summary
        googleData = googleSearch(project_name,type=2)
        simText,simUrl = getSimUrl(description, googleData, k)
        simUrlSummary = getUrlSummary(simUrl)
        logging.info(f"kg_subgraph:{kg_subgraph}")
        logging.info(f"simUrlSummary:{simUrlSummary}")
        return Response(refineSummary(org_name,description,simUrlSummary,socialMedia_summary,kg_subgraph,exist_kg), mimetype="text/event-stream")

    return jsonify(data="Error!"), 500

@app.route('/api/stream')
def progress():
    def generate():
        for ratio in range(10):
            yield "data:" + str(ratio) + "\n\n"
            logging.info("ratio:", ratio)
            time.sleep(1)
    return Response(stream_with_context(generate()),mimetype="text/event-stream")


app.run(host='0.0.0.0', port=8999, debug=True)

