import os

import dotenv
import supabase
import geopandas as gpd

dotenv.load_dotenv('..')


BATCH_SIZE = 100

def get_supabase_client():
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_KEY')
    print(supabase_url, supabase_key)
    return supabase.create_client(supabase_url, supabase_key)


def upload_nature_report_data(
        report_name,
        report_data,
        category_scores,
        category_column,
        area_column,
        geometry_column='geometry'
    ):
    gdf = report_data.copy()
    print(gdf.head())
    gdf = gdf.explode(index_parts=False, ignore_index=True)
    gdf['geom'] = gdf[geometry_column].apply(lambda x: x.wkt)
    gdf['area_ha'] = gdf[area_column]
    gdf = gdf.drop(columns=[geometry_column, area_column])

    supabase_client = get_supabase_client()

    for category in category_scores.keys():
        print(category)
        print({
            'report': report_name,
            'category': category,
            'score_biodiversity': category_scores[category]['biodiversity'],
            'score_climate': category_scores[category]['climate'],
            'score_nitrogen': category_scores[category]['nitrogen'],
            'score_recreation': category_scores[category]['recreation']
        })

        response = supabase_client.table('nature_report_category').insert({
            'report': report_name,
            'category': category,
            'score_biodiversity': category_scores[category]['biodiversity'],
            'score_climate': category_scores[category]['climate'],
            'score_nitrogen': category_scores[category]['nitrogen'],
            'score_recreation': category_scores[category]['recreation']
        }).execute()

        # print(response)

        slice = gdf[gdf[category_column] == category][['geom', 'area_ha']]
        slice['nature_report_category'] = response.data[0]['id']
        data_to_upload = slice.to_dict(orient='records')

        for i in range(0, len(data_to_upload), BATCH_SIZE):
            batch = data_to_upload[i:i+BATCH_SIZE]
            response = supabase_client.table('nature_report_area').insert(batch).execute()
            # print(response)

