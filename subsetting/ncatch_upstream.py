import geopandas as gpd
import argparse
from subset import get_upstream_ids    

def main():
    #setup the argument parser
    parser = argparse.ArgumentParser()
    parser.add_argument("-i",  dest="infile", type=str, required=True, help="A gpkg file containing divides and nexus layers")
    parser.add_argument("-o",  dest="outfile", type=str, required=True, help="A text file containing the number of upstream catchments for each catchment")
    args = parser.parse_args()

    infile    = args.infile
    outfile   = args.outfile

    print("Reading catchment data...")
    df_cat = gpd.read_file(str(infile), layer="divides")

    print("Reading nexus data...")
    df_nex = gpd.read_file(str(infile), layer="nexus")

    df_cat_org = df_cat.copy()
    df_nex_org = df_nex.copy()

    df_cat.set_index('id', inplace=True)

    print("Finding upstream catchments...")
    upstream = nupstream(df_cat_org, df_nex_org,df_cat.index)

    with open(outfile,'w') as fp:
        for jcatch in upstream:
            fp.write(f'{jcatch} : {upstream[jcatch]}\n')

    print(f'Done!  - >  {outfile}')            

def nupstream(divides,nexus,cat_list):
    """
    Find the number of upstream catchments for each catchment
    """
    upstream = {}
    for j in range(len(cat_list)):
        jcat_id = cat_list[j]
        cat_up_ids, nexus_up_ids = get_upstream_ids(divides, nexus, jcat_id)
        jnupstream = len(cat_up_ids)
        upstream[jcat_id] = jnupstream

    upstream = dict(sorted(upstream.items(), key=lambda x:x[1], reverse=True))

    return upstream

if __name__ == "__main__":
    main()