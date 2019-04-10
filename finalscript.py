#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import re
import time
import os
from mpi4py import MPI


comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

start = time.time() 
phasestart = time.time()  

melbGrid = 'melbGrid.json'
path = 'bigTwitter.json'

class process:
    def __init__(output, grid, chunk, path):
        output.num_post = {}
        output.num_hashtag = {}   
        limit = chunk[rank]
        rownum = 0
        linesread=0
        processed=0
        readerror=0
        with open(path, 'rb') as json_file:
            # read file starting from the start offset assigned to the core
            current_pos = limit[0]            
            json_file.seek(current_pos, os.SEEK_SET)
            index = 0
            for line in json_file:  
                linesread += 1
                if line.find(b'id') == 2:
                    json_raw = line.decode()
                    
                    # take all records except the last one ended with comma and enter
                    try:
                        json_load = json.loads(json_raw[0:-3])
                        
                    # the last record ended by enter                    
                    except:
                        json_load = json.loads(json_raw[0:-2])

                    # read line while in range of the end offset     
                    processed += 1
                    if json_file.tell() <= limit[1] :
                        rownum = rownum + 1
                        try:
                            x = float(json_load['doc']['coordinates']['coordinates'][0])
                            y = float(json_load['doc']['coordinates']['coordinates'][1])  
                            grid_status = False
                            g = 0
                            while not grid_status :   
                                if x >= grid[g][1] and x <= grid[g][2] and y >= grid[g][3] and y <= grid[g][4]:
                                    gridid = grid[g][0]
                                    if gridid in output.num_post.keys():                                    
                                        output.num_post[gridid] = output.num_post[gridid] + 1     
                                        output.num_hashtag[gridid] = process.count_hashtag(output, json_load, gridid)
                                   
                                    else:
                                        output.num_post[gridid] = 1
                                        output.num_hashtag[gridid] = {}
                                        output.num_hashtag[gridid] = process.count_hashtag(output, json_load, gridid)
 
                                    index = index + 1
                                    grid_status = True
                                g = g + 1
                            current_pos = json_file.tell()                                 
                        except:
                            readerror += 1
                            continue                        
                    else:
                        json_file.close()
                        break   
            
        print("Rank ",rank,", lines read=",linesread,", process attempted=",processed,", rownum=",rownum,", readerror=",readerror,"successread=",rownum-readerror)


    def count_hashtag(output, json_load, gridid):
        text = json_load['doc']['text']
        list_hashtag = re.findall(r"\s#(\S+)", text)  
        for hashtag in list_hashtag:
            hashtag = hashtag.lower()
            if hashtag in output.num_hashtag[gridid].keys():
                output.num_hashtag[gridid][hashtag] = output.num_hashtag[gridid][hashtag] + 1
            else:
                output.num_hashtag[gridid][hashtag] = 1 
        return output.num_hashtag[gridid]

if rank == 0:
    
    # grid stores the coordinate boundaries for each melbourne grid
    
    with open(melbGrid) as json_file:  
        gload = json.load(json_file)       
    grid = []    
    for i in range(len(gload["features"])):
        xmin = float(gload["features"][i]["properties"]["xmin"])
        xmax = float(gload["features"][i]["properties"]["xmax"])
        ymin = float(gload["features"][i]["properties"]["ymin"])
        ymax = float(gload["features"][i]["properties"]["ymax"])
        gridid = gload["features"][i]["properties"]["id"] 
        grid.append([gridid, xmin, xmax, ymin, ymax])
    
    # get the file size and divide by core size to get the offset
    # chunks stores the start and end offset for each core
    
    chunk = []
    n = os.path.getsize(path)
    n_chunk = n // size
    i = 0
    limit = n_chunk
    chunk.append([i,limit])
    while (n - limit) >= n_chunk :
        i = limit + 1
        limit = limit + n_chunk
        chunk.append([i,limit])
    if limit < n:
        chunk[len(chunk)-1][1] = n   
    
    #Adjust the chunk boundries to make sure each chunk begins at new line
    tmpfile=open(path,'rb')
    #print("filesize= ",n,"bytes file byte chunk per rank:")
    for i in range(len(chunk)):
        tmpfile.seek(chunk[i][1],os.SEEK_SET)
        s = tmpfile.readline() #read string till line end
        if(i > 0):
            chunk[i][0] = (chunk[i-1][1]) + 1
        chunk[i][1] = chunk[i][1]+len(s) - 1 #update final byte to line end
        print("Rank",i,"byte range",chunk[i])
    tmpfile.close()
    
    #till now operation only done by master
    end = time.time()
    runtime = end - phasestart
    print("Phase: First Master Work, Current phase running time : ", runtime)
    phasestart = time.time()

else:
    grid = None
    chunk = None
    path = None
    phasestart = time.time()
    

comm.Barrier()

# broadcast the grid, chunk, and the twitter file path to every core

grid = comm.bcast(grid, root = 0)
chunk = comm.bcast(chunk, root = 0)
path = comm.bcast(path, root = 0)

# get every core processes the same tasks for different chunk of data

#=operation done by all
end = time.time()
runtime = end - phasestart
print("Phase: Rank ", rank , " Ready to start, Running time : ", runtime)
phasestart = time.time()

result = process(grid, chunk, path)
num_post = result.num_post
num_hashtag = result.num_hashtag

# gather all the results from all cores

#=operation done by all
end = time.time()
runtime = end - phasestart
print("Phase: Rank ", rank , " Job Completed, Running time : ", runtime)
phasestart = time.time()

all_post = comm.gather(num_post, root = 0)
all_hashtag = comm.gather(num_hashtag, root = 0) 

# aggregate the results in master core

#=operation done by all
end = time.time()
runtime = end - phasestart
print("Phase: All Core Work, Rank ", rank , ", Data Gather Completed, Running time : ", runtime)
phasestart = time.time()

if rank == 0:
    num_post = all_post[0]
    num_hashtag = all_hashtag[0]
    if size > 0:
        for r in range(1,size):
            post = all_post[r]
            hashtag = all_hashtag[r]
            for i, key in enumerate(post):  
                if key in num_post.keys():       
                    num_post[key] = num_post[key] + post[key]              
                else:
                    num_post[key] = post[key]         
                
                if key in num_hashtag.keys():
                    list_hashtag = num_hashtag[key].keys()
                    for ht in hashtag[key].keys():  
                        if ht not in list_hashtag:                       
                            num_hashtag[key][ht] = hashtag[key][ht] 
                        else:
                            num_hashtag[key][ht] = num_hashtag[key][ht] + hashtag[key][ht] 
                else:
                    num_hashtag[key] = hashtag[key]
                    
        num_post = sorted(num_post.items(), key=lambda x: x[1], reverse = True)
    
    # agg_hashtag stores the top 5 hashtags based on hashtag frequency   
    
    agg_hashtag = {}
    for key in num_hashtag.keys():
        grid_list = {}
        grid_hash = num_hashtag[key]
        grid_top = list(set(grid_hash.values()))
        grid_top.sort()
        if len(grid_top) >= 5:
            limit = grid_top[-5:]
        else:
            limit = grid_top[-len(grid_top):]   
  
        grid_list = sorted(grid_hash.items(), key=lambda x: x[1], reverse = True)
        
        selected = []
        for i in range(len(grid_list)):
            if grid_list[i][1] in limit:
                selected.append(grid_list[i])

        agg_hashtag[key] = selected    
          
    for k in range(len(num_post)):
        area = num_post[k][0]
        print(area , ": ", num_post[k][1] , " posts", sep = "")           
    for k in range(len(agg_hashtag)):
        area = num_post[k][0]
        print(area , ": " , agg_hashtag[area], sep = "")
        
    end = time.time()
    runtime = end - phasestart
    print("Phase: Last Master work Completed, Running time : ", runtime)
   
    end = time.time()
    time = end - start
    print("running time : ", time)
