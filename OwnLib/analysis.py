from datetime import datetime
Today=datetime.now().strftime('%Y-%m-%d')

def make_wordcloud(flat_list,filename,Mostcommon=100,bg="white",font_size=(4,10),startBy=0,removeWords=[],max_words=100):
    from collections import Counter
    from wordcloud import WordCloud
    import matplotlib.pyplot as plt

    flat_list=[i for i in flat_list if i not in removeWords]
    a_counter = Counter(flat_list)
    most_common = a_counter.most_common(Mostcommon)[startBy:]
    wordcloud=WordCloud(min_font_size=font_size[0],max_font_size=font_size[1],background_color=bg, width=1200, height=1200,max_words=max_words).generate_from_frequencies(dict(most_common))
    
    plt.figure()
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    
    
    
    if filename!="":
        plt.savefig(Today+"_"+filename+".pdf",dpi=300)
        
    plt.show()


def Word_NetworkGen(df,n=5,column='authors_Institutions'):
    
    

    TweetWords=df[column].dropna().to_list()
    from itertools import combinations
    Tupples=[combinations(words, 2) for words in TweetWords if len(TweetWords)>1] 
    flatTuppleList=[y for x in Tupples for y in x]
    
    from collections import Counter
    a_counter = Counter(flatTuppleList)
    c = a_counter.most_common(n)
    
    print(c[:5])
    
    #c=[i[0] for i in c]
    
    import networkx as nx
    import re
    G=nx.Graph()
    
    for i in c:
        source=i[0][0]
        target=i[0][1]
        G.add_edge(source,target,count=str(i[1]))
    
    return G




def AlphaNumNodes(G):
    import networkx as nx
    import re
    nx.relabel_nodes(G, lambda x: re.sub('[^A-Z a-z 0-9-]+','', x))
    return G

def PyVisGraph(G):
    G=AlphaNumNodes(G)
    from pyvis.network import Network
    net=Network(notebook=True,directed =True)
    net.from_nx(G,edge_scaling=True, default_edge_weight=0.5)
    return net.show("network.html")

def CleanGraph(G,removeIsolates=True,minDegree=20,only_largest_component=True):
    print(f"Cleaning Graph to minimum Degree {minDegree}.")
    import networkx as nx
    remove = [node for node,degree in dict(G.degree()).items() if degree <minDegree]
    G.remove_nodes_from(remove)
    if removeIsolates==True:
        G.remove_nodes_from(list(nx.isolates(G)))
        
    if only_largest_component==True:
        G=G.to_undirected()
        components = nx.connected_components(G)
        largest_component = max(components, key=len)
        G = G.subgraph(largest_component)
        
    print("Nodes count: ", len(G.nodes))
    print("Edges count: ", len(G.edges))
    return G


def writeNetworkHTML(G,filename="The_network.html",view=True):
    
    ## This function was necessary as the pyvis thing did not work in Colab
    
    
    ###
    #These two lines are necessary to run the script in colab:
    
    #folder=directory.split("content/")[1]
    #filename=folder+"html_output/"+filename
    
    ###
    
    
    EdgesText=", ".join(["{ from: '"+str(i[0]) + "', to: '" + str(i[1]) +"' }" for i in G.edges])
    NodesText=", ".join(["{ id: '"+str(i)+ "', label: '"+str(i)+ "'}" for i in G.nodes])


    HTMLframe="""<!DOCTYPE html>
    <html lang="en">
      <head>
        <title>Network</title>
        <style type="text/css">
          #mynetwork_1 {
            width: 1200px;
            height: 800px;
            border: 1px solid lightgray;
          }
        </style>
      </head>
      <body>
        <div id="mynetwork_1"></div>
        <script type="text/javascript" src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
        <script type="text/javascript">

          // create an array with nodes
          var nodes = new vis.DataSet([""" + NodesText +  """
          ]);

          // create an array with edges
          var edges = new vis.DataSet(["""  + EdgesText + """
          ]);
          // create a network
          var container = document.getElementById("mynetwork_1");
          var data = {
            nodes: nodes,
            edges: edges,
          };
          var options = {};
          var network = new vis.Network(container, data, options);

        </script>
      </body>
    </html>"""
    
    
    with open(filename,"w") as f:
        f.write(HTMLframe)
        
    if view==True:    
        from IPython.display import HTML, display
        display(HTML(filename=filename))
