#!/usr/bin/env python
"""subset.py
Module for subsetting hyfeatures based ngen hydrofabric geopackages.
@author Nels Frazier
@email nfrazier@lynker.com
@version 0.1
"""

from pathlib import Path
import geopandas as gpd
import pandas as pd
import fiona
from queue import Queue

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import List

import networkx as nx

def make_x_walk(hydrofabric):
    """
    Borrowed from https://github.com/NOAA-OWP/ngen/pull/464
    Args:
        hydrofabric (_type_): _description_
    """
    attributes = gpd.read_file(hydrofabric, layer="flowpath_attributes").set_index("id")
    x_walk = pd.Series(attributes[~attributes["rl_gages"].isna()]["rl_gages"])

    data = {}
    for wb, gage in x_walk.items():
        data[wb] = {"Gage_no": [gage]}
    import json

    with open("crosswalk.json", "w") as fp:
        json.dump(data, fp, indent=2)


def make_geojson(hydrofabric: Path):
    """Create the various required geojson/json files from the geopkg
    Borrowed from https://github.com/NOAA-OWP/ngen/pull/464
    Args:
        hydrofabric (Path): path to hydrofabric geopkg
    """
    try:
        loader = LoadGDB(hydrofabric)
        catchments = loader.read_gdb_layer(layer="divides")
        nexuses = loader.read_gdb_layer(layer="nexus")
        flowpaths = loader.read_gdb_layer(layer="flowpaths")
        edge_list = pd.DataFrame(
            loader.read_gdb_layer(layer="flowpath_edge_list").drop(
                columns="geometry"
            )
        )
        make_x_walk(hydrofabric)
        catchments.to_file("catchments.geojson")
        nexuses.to_file("nexus.geojson")
        flowpaths.to_file("flowpaths.geojson")
        edge_list.to_json("flowpath_edge_list.json", orient="records", indent=2)
    except Exception as e:
        print(f"Unable to use hydrofabric file {hydrofabric}")
        print(str(e))
        raise e


def get_upstream_ids(divides, nexus, catchment_id):
    """Get the ids of the elements upstream from catchment_id
    Derived from https://github.com/NOAA-OWP/DMOD/blob/3a6da86cac3061116b9a1e2ccdd4a3d01222f0d3/python/lib/modeldata/dmod/modeldata/subset/subset_handler.py#L212
    Args:
        divides (_type_): _description_
        nexus (_type_): _description_
        catchment_id (_type_): _description_
    Returns:
        _type_: _description_
    """
    # print(divides)
    nex_index = nexus[["id", "toid"]].set_index(
        "id"
    )  # pd.Series(nexus['toid'], index = nexus['id'])#
    nex_index["toid"] = nex_index["toid"].str.replace("wb", "cat")
    cat_index = divides[["id", "toid"]].set_index("id")
    link_limit = None
    catchment_ids = [catchment_id]
    graph_nodes = Queue()

    # print(cat_index)
    # debug = cat_index.reset_index()['id'].str.replace('cat-', '').astype(int).sort_values()
    # print(debug)
    # print(debug[ (debug > 113050) & (debug < 113070) ])
    # import os
    # os._exit(1)
    for cid in catchment_ids:
        graph_nodes.put((catchment_id, 0, True))
        graph_nodes.put((cat_index.loc[cid].item(), 0, False))

    cat_ids = set()
    nex_ids = set()

    while graph_nodes.qsize() > 0:
        item, link_dist, is_catchment = graph_nodes.get()
        if item is None:
            continue
        if is_catchment and item not in cat_ids:
            cat_ids.add(item)
            if link_limit is None or link_dist < link_limit:
                new_dist = link_dist + 1
                # find the nexus linked to the upstream of this catchment
                inflow = nex_index[nex_index["toid"] == item].index.unique()
                if len(inflow) == 1:
                    graph_nodes.put((inflow[0], new_dist, False))
                elif len(inflow) > 1:
                    print("WARNING: Catchment network is not dendridict")
                # If it is 0, we found a headwater, which is fine...
        elif not is_catchment and item not in nex_ids:
            nex_ids.add(item)
            if link_limit is None or link_dist < link_limit:
                new_dist = link_dist + 1
                for c in cat_index[cat_index["toid"] == item].index:
                    graph_nodes.put((c, new_dist, True))

    return cat_ids, nex_ids


# (tony) adding support for s3
# begin -------------------------------
class LoadGDB():
    
    def __init__(self, path):
        import boto3
        import fsspec
        self.isS3 = False
        self.path = path
        if path[0:3] == 's3:':
            self.isS3 = True
            self.s3 = boto3.resource('s3')
            self.s3 = fsspec.filesystem('s3', anon=True)    

    def read_gdb_layer(self, layer):
        if self.isS3:
            return self._get_layer_from_s3(layer)
        return gpd.read_file(self.path, layer=layer)

    def list_gdb_layers(self):
        if self.isS3:
            return self._list_layers_from_s3()
        return fiona.listlayers(self.path)
            
    def __s3_open(self):
        return self.s3.open(self.path)
    
    def _list_layers_from_s3(self):
        return fiona.listlayers(self.__s3_open())
        
    def _get_layer_from_s3(self, layer):
        return gpd.read_file(self.__s3_open(), layer=layer)
# end -------------------------------

def subset_upstream(hydrofabric: Path, ids: "List") -> None:
    """
    Args:
        hydrofabric (_type_): _description_
        ids (List): _description_
    """
    

    # (tony) adding support for s3
    # begin -------------------------------
    loader = LoadGDB(hydrofabric)
    layers  = loader.list_gdb_layers()
    divides = loader.read_gdb_layer(layer='divides')
    nexus   = loader.read_gdb_layer(layer='nexus')
    # end -------------------------------
        
    cat_ids, nex_ids = get_upstream_ids(divides, nexus, ids)
    # As long as these remain 1-1 this works, but that may not always be the case
    # FIXME in fact this isn't true at all, there can be catchments with no FP, and FP with no catchment
    wb_ids = list(map(lambda x: x.replace("cat", "wb"), cat_ids))

    # To use as pandas indicies, it really wants list, not set
    cat_ids = list(cat_ids)
    nex_ids = list(nex_ids)
    # Now have the index keys to subset the entire hydrofabric
    # print("Subset ids:")
    # print(cat_ids)
    # print(nex_ids)
    # print(wb_ids)
    # Useful for looking at the name of each layer and which id index is needed to subset it
    for layer in layers:
        #     df = gpd.read_file(hydrofabric, layer=layer)
        print(layer)
    #     print(df.head())

    flowpaths = (
        loader.read_gdb_layer(layer="flowpaths")         # (tony) adding support for s3
        .set_index("id")
        .loc[wb_ids]
        .reset_index()
    )
    divides = divides.set_index("id").loc[cat_ids].reset_index()
    nexus = nexus.set_index("id").loc[nex_ids].reset_index()
    # lookup_table = gpd.read_file(hydrofabric, layer='lookup_table').set_index('id').loc[wb_ids].reset_index() v1.0???

       
    crosswalk = (
        loader.read_gdb_layer(layer="network_lookup")  # (tony) adding support for s3
        .set_index("id")
        .loc[wb_ids]
        .reset_index()
    )  # v1.2
    flowpath_edge_list = (
       loader.read_gdb_layer(layer="network") # (tony) adding support for s3
        .set_index("id")
        .loc[nex_ids + wb_ids]
        .reset_index()
    )
    flowpath_attributes = (
        loader.read_gdb_layer(layer="flowpath_attributes") # (tony) adding support for s3
        .set_index("id")
        .loc[wb_ids]
        .reset_index()
    )
    model_attributes = (
        loader.read_gdb_layer(layer="cfe_noahowp_attributes") # (tony) adding support for s3
        .set_index("id")
        .loc[cat_ids]
        .reset_index()
    )
    # forcing_attributes = gpd.read_file(hydrofabric, layer='forcing_attributes').set_index('id').loc[cat_ids].reset_index() v1.0
    forcing_meta = (
        loader.read_gdb_layer(layer="forcing_metadata") # (tony) adding support for s3
        .set_index("id")
        .loc[cat_ids]
        .reset_index()
    )
    name = f"{ids}_upstream_subset.gpkg"

    flowpaths.to_file(name, layer="flowpaths")
    divides.to_file(name, layer="divides")
    nexus.to_file(name, layer="nexus")
    # lookup_table.to_file(name, layer="lookup_table")
    crosswalk.to_file(name, layer="crosswalk")
    flowpath_edge_list.to_file(name, layer="flowpath_edge_list")
    flowpath_attributes.to_file(name, layer="flowpath_attributes")
    model_attributes.to_file(name, layer="cfe_noahowp_attributes")
    forcing_meta.to_file(name, layer="forcing_metadata")

    make_geojson(name)

def get_upstream_ids_prerelease(nexus, flow, catchment_id):

    # clean and merge nexus and flowline data, keep all records
    nexus = nexus[['id', 'toid']]
    nexus = nexus.rename(columns={'id': 'from-nexus', 'toid': 'wb-id'})
    flow = flow[['id', 'toid', 'divide_id']]
    flow = flow.rename(columns={'id': 'wb-id', 'toid': 'to-nexus'})
    merged = nexus.merge(flow, on=['wb-id', 'wb-id'], how='outer')
    
    # create directional graph of these data, build graph in reverse order
    print('Building Graph Network')
    G = nx.DiGraph()
    for idx, row in merged.iterrows():
        if not pd.isnull(row['from-nexus']):
            G.add_edge(row['to-nexus'], row['from-nexus'])
        G.add_edge(row['to-nexus'], row['wb-id'], divide_id=row['divide_id'])

    # get starting node
    start_nexus = merged.loc[merged['wb-id'] == catchment_id]['to-nexus'].item()

    # perform depth first search
    subG = nx.dfs_tree(G, start_nexus)

    # separate wb- and nex- elements into lists
    wbs = []
    nex = []
    for i in list(subG.nodes()):
        if i[0:2] == 'wb':
            wbs.append(i)
        else:
            nex.append(i)
    
    print('Identified:')
    print(f'  - {len(wbs)} waterbody locations')
    print(f'  - {len(nex)} nexus locations ')
    
    return wbs, nex
    
def subset_upstream_prerelease(hydrofabric: Path, ids: "List") -> None:
    """
    Function to peform hydrofabric subsetting on the "pre-release" dataset.
    Args:
        hydrofabric (_type_): _description_
        ids (List): _description_
    """
    

    # (tony) adding support for s3
    # begin -------------------------------
    loader = LoadGDB(hydrofabric)
    layers  = loader.list_gdb_layers()
    divides = loader.read_gdb_layer(layer='divides')
    nexus   = loader.read_gdb_layer(layer='nexus')
    flow = loader.read_gdb_layer(layer='flowpaths')
    # end -------------------------------

    # trace upstream
    wb_ids, nex_ids = get_upstream_ids_prerelease(nexus, flow, ids)
    

    for layer in layers:
        print(layer)
    
    print('Subsetting Flowpaths')
    flowpaths = (
        loader.read_gdb_layer(layer="flowpaths")         
        .set_index("id")
        .loc[wb_ids]
        .reset_index()
    )
    print('Subsetting Divides')
    divides = divides.set_index("id").loc[wb_ids].reset_index()
    
    print('Subsetting Nexus')
    nexus = nexus.set_index("id").loc[nex_ids].reset_index()
 
    print('Subsetting Crosswalk')
    crosswalk = (
        loader.read_gdb_layer(layer="network_lookup")  
        .set_index("id")
        .loc[wb_ids]
        .reset_index()
    ) 
    
    print('Subsetting Edge List')
    flowpath_edge_list = (
       loader.read_gdb_layer(layer="network") 
        .set_index("id")
        .loc[nex_ids + wb_ids]
        .reset_index()
    )

    print('Subsetting Flowpath Attributes')
    flowpath_attributes = (
        loader.read_gdb_layer(layer="flowpath_attributes") 
        .set_index("id")
        .loc[wb_ids]
        .reset_index()
    )
    
    print('Subsetting Model Attributes')
    cat_ids = list(map(lambda x: x.replace("wb", "cat"), wb_ids))
    model_attributes = (
        loader.read_gdb_layer(layer="cfe_noahowp_attributes") 
        .set_index("divide_id")
        .loc[cat_ids]
        .reset_index()
    )
    
#     print('Subsetting Hydro Locations')
#     hydro_locations = (
#         loader.read_gdb_layer(layer="hydrolocations") 
#         .set_index("id")
#         .loc[nex_ids]
#         .reset_index()
#     )

#     print('Subsetting Hydro Lookup')
#     hydro_lookup = (
#         loader.read_gdb_layer(layer="hydrolocations_lookup") 
#         .set_index("id")
#         .loc[wb_ids]
#         .reset_index()
#     )   
    
#     print('Subsetting Lake Attributes')
#     lake_attributes = (
#         loader.read_gdb_layer(layer="lake_attributes") 
#         .set_index("id")
#         .loc[wb_ids]
#         .reset_index()
#     )
    
    # save outputs 
    print('Saving Subsets to GeoPackage')
    name = f"{ids}_upstream_subset.gpkg"
    flowpaths.to_file(name, layer="flowpaths")
    divides.to_file(name, layer="divides")
    nexus.to_file(name, layer="nexus")
    crosswalk.to_file(name, layer="crosswalk")
    flowpath_edge_list.to_file(name, layer="flowpath_edge_list")
    flowpath_attributes.to_file(name, layer="flowpath_attributes")
    model_attributes.to_file(name, layer="cfe_noahowp_attributes")
    
    # make geojsons
    print('Saving Geo JSON')
    make_geojson(name)
    
if __name__ == "__main__":
    import argparse

    # get the command line parser
    parser = argparse.ArgumentParser(description="Subset provided hydrofabric")
    parser.add_argument(
        "hydrofabric", type=Path, help="Path or link to hydrofabric geopkg to"
    )
    # TODO make this a group, pick the type of subset to do...
    # TODO allow multiple inputs for upstream?
    # TODO custom validate type to ensure it is a valid identifier?
    parser.add_argument("upstream", type=str, help="id to subset upstream from")
    

    args = parser.parse_args()
    subset_upstream(args.hydrofabric, args.upstream)
