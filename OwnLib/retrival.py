import requests
import pandas as pd
import json
import os

import datetime as dt
Heute=dt.datetime.now().strftime('%Y-%m-%d')
OA_Nano_ConceptID='c171250308'


def from_folder(folder):
    image_path = []
    for path in os.listdir(folder):
        image_path.append(os.path.join(folder,path))
    return image_path # Returning list of images

def load_data(file):
    with open (file, "r", encoding="utf-8") as f:
        data = json.load(f) 
    return (data)

def write_data(file, data):
    with open (file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

def flattenList(l):
    flat_list = [item for sublist in l for item in sublist]
    return flat_list
        
def Imgdownload(url,filename="",targetfolder="downloads/nano/"):
    
    if filename==None: ###THIS is only for ChemCom Journal so far...
        urlToFilename=url.split("=")[-1]+".gif"
        filename=targetfolder+urlToFilename
        
    try:
        print(url, filename)
        headers = requests.utils.default_headers()
        headers.update({'User-Agent': 'My User Agent 1.0'})
        r = requests.get(url, allow_redirects=True, headers=headers)
        with open (filename, "wb") as f:
            f.write(r.content)
    except:
        print("error with: ", url)
        filename=None
       
        
        
        
def Altmetrics(doi):

    
    SubDict={   'abstract':None,
                'score':None,
                'cited_by_msm_count':None, 
                'cited_by_posts_count':None, 
                'cited_by_tweeters_count':None,
                'cited_by_patents_count':None, 
                'cited_by_accounts_count':None,
                'readers_count':None,
                'readers':None}
    
    doi=doi.replace("https://doi.org/","")
    url=f'http://api.altmetric.com/v1/doi/{doi}'
    print(url)
    x=requests.get(url)
    
    if x.status_code == 200:
        response=x.json()
        for key in SubDict.keys():
            try:
                #print(key)
                SubDict[key]=response[key]
            except: 
                continue

    
    return pd.Series(SubDict)
        
        
def BestWorst(df,by="cited_by_count",Targetfolder="Bestlists",n=100):
    ### Splits Files into best / worst n and copies files to subfolders 
    import shutil
    
    import pathlib
    Bestlists={}
    Bestlists["best"]=list(df.sort_values(by, ascending=False)[:n]["GA_filename"])
    Bestlists["worst"]=list(df.sort_values(by, ascending=True)[:n]["GA_filename"])
    
    
    sourceFolder=Bestlists["best"][-1].rsplit("/",1)[0]
    
    print(sourceFolder)
    for classes in ["best","worst"]:
        folderpath=f"{sourceFolder}/{Targetfolder}/{by}/{classes}/"
        print(folderpath)
        pathlib.Path(folderpath).mkdir(parents=True, exist_ok=True)
        
        for src in Bestlists[classes]:
            shutil.copy(src, folderpath)
            
    folders=folderpath.rsplit("/",2)[0]
    print(folders)
    return folders

TuppleList=[]

Query={
    #'author.id':Auth_ID,
    #'abstract.search':"nano*",
    #'title.search':"nano",
    'fulltext.search':"nano*",
    "concepts.ID":"OA_conceptID",
    "host_venue.issn":"ISSN",    
    "from_publication_date":"1994-01-01",
    "mailto":"mr@techphil.de",
    "page": 1,
    "per_page":"200",
    "cursor" : '*'
    }
Query


#######################################
### Generate DATA Frame From Results

def authorGen(work):
    if type(work)!=list:
        work=work["authorships"]
    authors=[author["author"]['display_name'] for author in work]
    return authors


def authorInstGen(work,flatten=True):
    ## generates authorsList from authorship_instutions: .apply(authorGen)
    if type(work)!=list:
        work=work["authorships"]
    institutionDictList=[author['institutions'] for author in work]
    authorInstitutions=[ [i["display_name"] for i in inst if i] for inst in institutionDictList if len(institutionDictList[0])>0]
    
    if flatten==True:
        authorInstitutions=flattenList(authorInstitutions)
    return authorInstitutions


def authorCountryGen(work):
    ## generates authorsList from authorship_instutions: .apply(authorGen)
    if type(work)!=list:
        work=work["authorships"]
    authorCountryList=[author['institutions'] for author in work]
    
    authorCountry=[ [i['country_code'] for i in inst if i] for inst in authorCountryList if len(authorCountryList[0])>0]
    authorCountry=flattenList(authorCountry)
    return authorCountry


def PropperDF(results):
    df=pd.DataFrame(results)
    df["authors"]=df.authorships.apply(authorGen)
    df["authors_Institutions"]=df.authorships.apply(authorInstGen,flatten=True)
    df["authors_Countries"]=df.authorships.apply(authorCountryGen)

    df=df.sort_values("cited_by_count",ascending=False)
    df["Concepts"]=df.concepts.apply(lambda x: [i['display_name'] for i in x])
    return df


def BasicAuthorInfos(author):
    AuthorDict={"name": author['display_name'],
                "ID": author['id'],
                'works_count': author['works_count'],
                'cited_by_count': author['cited_by_count']}
    try:
        AuthorDict2={
            #"Country": author['last_known_institution']['country_code'],
            "last_institution":author['last_known_institution']["display_name"],
            "active time": (author['counts_by_year'][-1]['year'], author['counts_by_year'][0]['year']),
            "Concepts": ", ".join([i['display_name'] for i in author['x_concepts']][:10])
        }
    except:
        AuthorDict2={}
    AuthorDict=AuthorDict | AuthorDict2      
    return AuthorDict

#######################################################
#######################################################


def RequestGetBasicAuthorInfo(SearchTerm,n=0):
    # AuthorName & N_authorData
    print(SearchTerm)
    authors = requests.get(
        f'https://api.openalex.org/authors?filter=display_name.search:{SearchTerm}').json()['results']
    
    AuthorList=[]
    print(len(authors))
    if n==0:
        n=len(authors)
    for author in authors[:n]:
        basicAuthorInfo=BasicAuthorInfos(author)
        AuthorList+=[basicAuthorInfo]
       # print(basicAuthorInfo)
       # print("\n")
    return AuthorList


###############################################################
#######  WorkDicts ############################################


def Basic_workInfo(work):
    # makes a dict of basic work infos
    work_dict={}
    for k in ["id","doi","title",'publication_date','cited_by_count']:
        work_dict[k]=work[k]

    for k in ['display_name',"issn_l"]:
        try:
            work_dict[f"Host_venue_{k}"]=work['host_venue'][k] 
        except:
            work_dict[f"Host_venue_{k}"]="none"
            print("no Host venue")
        
    work_dict["authors"]=[author["author"]['display_name'] for author in work["authorships"]]

    work_dict["author_institutions"]=authorInstGen(work)
    work_dict["author_countries"]=authorCountryGen(work)
    
    return work_dict

def Work_dicts(Citing_Works):
    work_dicts={}
    #TuppleList=[]
    for work in Citing_Works:
        work_dict=Basic_workInfo(work)
        OAid=work_dict["id"].split("/")[-1]
        work_dicts[OAid]=work_dict
        #TuppleList.append((seed,OAid))
        
    return work_dicts

###############################################################
###############################################################


def RQ_Auth_ID(Query):
    ## For Author Search
    RequestCode=(f"https://api.openalex.org/works"
        f"?filter=author.id:{Query['Auth_ID']},"
        f"is_paratext:false,from_publication_date:{Query['from_Pub_Date']}"
        f"&page={Query['page']}&per_page={Query['per_page']}&cursor={Query['cursor']}&"
        f"mailto={Query['eMail']}")
    
    response = requests.get(RequestCode).json()
    print(response["meta"])
    Query["cursor"]=response["meta"]["next_cursor"]
    print(Query)
    results=response["results"]
    return results

def generateRQ(Query):
    RQString="https://api.openalex.org/works?filter="
    
    Query_Items=["concepts.id","title.search","fulltext.search","concepts.ID","abstract.search",\
                 "host_venue.issn","from_publication_date","is_retracted"]
    
    for key,item in Query.items():
        
        if key in Query_Items:
            RQString=RQString+f"{key}:{item},"
                
        if key in ["mailto","page","per_page","cursor"]:
        
            RQString=RQString.rstrip(",")+f"&{key}={item}" ## rstrip removes the ","
    return RQString


def RQ_Concept_ID(Query):
    ## For Author Search
    RequestCode=generateRQ(Query)
    
    response = requests.get(RequestCode).json()
    print(response["meta"])
    Query["cursor"]=response["meta"]["next_cursor"]
    print(Query)
    results=response["results"]
    return results

def RQ_cites(Query):
    ## For Citation Recursor
    RequestCode=(f"https://api.openalex.org/works?filter=cites:{Query['OA_ID_cites']}&page={Query['page']}&per_page={Query['per_page']}&cursor={Query['cursor']}&mailto={Query['eMail']}")
    
    response = requests.get(RequestCode).json()
    print(response["meta"])
    Query["cursor"]=response["meta"]["next_cursor"]
    print(Query)
    results=response["results"]
    return results


def Cursor_RQ_cites(Query,pages=0):
    results=[]
    page=0
    if pages==0:
        while Query["cursor"]!=None:
            results+=RQ_cites(Query)
            print(len(results))
    else:
        while Query["cursor"]!=None and page<=pages:
            print(page)
            results+=RQ_cites(Query)
            print(len(results))
            page+=1
            
    return results

def Cursor_RQ_Auth_ID(Query):
    results=[]
    while Query["cursor"]!=None:
        results+=RQ_Auth_ID(Query)
        print(len(results))
    return results

def Cursor_RQ_Concept_ID(Query,pages=0):
    results=[]
    
    if pages==0:
        while Query["cursor"]!=None:
            results+=RQ_Concept_ID(Query)
            print(len(results))
    else:
        for i in range(pages):
            print(i)
            results+=RQ_Concept_ID(Query)
            print(len(results))
    
    return results




def recurser(OA_ID,work_dicts,START_ID,counter=0,cmax=1,pages=0,MinimumCitationsForRecall=5):
    print(f"Count: {counter}, ID: {OA_ID}, List-Len: {len(TuppleList)}")
    
    print(OA_ID, START_ID)
    
    if OA_ID == START_ID:
        work = requests.get("https://api.openalex.org/"+OA_ID).json()
        newEntriesSeed0=Basic_workInfo(work)
        newEntriesSeed0["Parents"]=["START"]
        newEntriesSeed0["Level"]=0
        work_dicts[OA_ID]=newEntriesSeed0
    
    if counter<cmax:
        counter+=1
        print(counter)
        Query={"OA_ID_cites":OA_ID,
        "eMail":"mr@techphil.de",
        "page": 1,
        "per_page":"200",
        "cursor" : '*'
        }
        
        ### Get Information from OA:
        Citing_Works=Cursor_RQ_cites(Query,pages=pages)
        newEntries=Work_dicts(Citing_Works)
        
        ### Process Information: Update Dictionary
        for k,v in newEntries.items():
            OA_ID=OA_ID.split("/")[-1]
            v.update({"Parents":[OA_ID],"Level":counter})
            
            if k not in work_dicts:
                print("update",OA_ID, end=". ")
                work_dicts[k]=v
            else:
                print("update, existing ",OA_ID, end=". ")
                #work_dicts[k]["Level"]=counter
                print("alt", work_dicts[k]['cited_by_count'], end=". ")
                print("neu", v['cited_by_count'], end=". ")
                work_dicts[k]["Parents"].append(OA_ID)
                
              
        ### ReCall Function with new OA_ID from newEntries:
        for Newkey in newEntries.keys():
            OA_ID=OA_ID.split("/")[-1]
            
            TuppleList.append((OA_ID,Newkey))
            OA_ID=Newkey
            print("re-call function.")
        
            if work_dicts[Newkey]['cited_by_count']>MinimumCitationsForRecall:
                print(work_dicts[Newkey]['title'],work_dicts[Newkey]['cited_by_count'])
            ### New Call of this function:
                recurser(OA_ID,work_dicts,START_ID,counter,cmax)
    else:
        print("done", end=". ")
        
    return work_dicts
        

def MakeTuppleList(wd):
    TuppleList=[]
    for k,v in wd.items():
        for parent in v['Parents']:
            TuppleList.append((parent.split("/")[-1],k))   
    return TuppleList


def FlattenList(l):
    
    flatList=[item for sublist in l for item in sublist if sublist]
    return flatList

def nxDataFrameFromWD(wd):
    import pandas as pd
    
    df=pd.DataFrame(wd).fillna("-") ### necessary for networkx (crashes with None value)
    df2=df.T
    df2.id=df2.id.apply(lambda x: x.split("/")[-1])
    df2.index=df2.id
    df2.authors=df2.authors.apply(lambda x:", ".join(x))
    df2.author_institutions=df2.author_institutions.apply(lambda l: [i.split(", ")[0] for i in l])
    #df2.author_institutions=df2.author_institutions.apply(lambda l:", ".join(FlattenList(l)))

    df=df2.T.fillna("-")
    
    return df

def MakeGraphFromWD(wd,writefile=True,filename="citations"):
    
    TuppleList=MakeTuppleList(wd)
    
    df=nxDataFrameFromWD(wd)
    
    import networkx as nx
    G=nx.DiGraph()
    colorDict={""}
    for tuppel in TuppleList:
        #starting at 1 because the oldest has the lowest OAID and is first in the dict.
        A=tuppel[0].split("/")[-1]
        B=tuppel[1].split("/")[-1]

        print(A,B)
        Host_venue=str(df[B]['Host_venue_display_name'])
        date=df[B]['publication_date'].split("-")[0]
        Level=df[B]['Level']
        Journal=str(df[B]['Host_venue_display_name'])
        Citations=str(df[B]['cited_by_count'])

        G.add_edge(A,B,Host_venue=Host_venue,date=date,Level=Level,Journal=Journal)


    nx.set_node_attributes(G, df.iloc[:7,:])
    
    if writefile==True:
        nx.write_gexf(G,Heute+"_"+filename+".gexf")
        
    return G

def agrfkt(x):
    aggr=list(set(x))
    if type(x[0])==str:
        aggr=" , ".join(aggr)
    if type(x[0])!=str:
        aggr=sum(x)
    return aggr 



def GenerateAuthorTuppelsFromWD(work_dict_full):
    df=pd.DataFrame.from_dict(work_dict_full, orient='index').fillna("-")
    df.id=df.id.apply(lambda x: x.split("/")[-1])
    AuthorsDict=df["authors"].to_dict() ## Dict of Authors and publication
    AuthorsDict["START"]="START"
    df2=df.explode("authors")
    df3=df2.explode("Parents")
    df3["Parents_Authors"]=df3.Parents.apply(lambda x: AuthorsDict[x])
    df3[df3["Parents"]=="START"]["title"]
    df3=df3.explode("Parents_Authors")
    AuthorTuppelList=list(zip(df3.authors, df3.Parents_Authors))
    
    df4=df2.groupby("authors").agg(lambda x: agrfkt(x))
    df4['publication_date']=df4['publication_date'].apply(lambda x: x.split("-")[0]+" ")
    df4['cited_by_count']=df4['cited_by_count'].apply(lambda x: str(x))
    return AuthorTuppelList,df4



def PyVisGraph(G):
    from pyvis.network import Network
    net=Network(notebook=True)
    net.from_nx(G)
    return net.show("network.html")
    