import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node album
album_node1705352429274 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://spotify-project-aws-datapipeline/staging/albums.csv"],
        "recurse": True,
    },
    transformation_ctx="album_node1705352429274",
)

# Script generated for node artist
artist_node1705352455903 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://spotify-project-aws-datapipeline/staging/artists.csv"],
        "recurse": True,
    },
    transformation_ctx="artist_node1705352455903",
)

# Script generated for node tracks
tracks_node1705352609335 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://spotify-project-aws-datapipeline/staging/track.csv"],
        "recurse": True,
    },
    transformation_ctx="tracks_node1705352609335",
)

# Script generated for node Join artist & album
Joinartistalbum_node1705352482024 = Join.apply(
    frame1=artist_node1705352455903,
    frame2=album_node1705352429274,
    keys1=["id"],
    keys2=["artist_id"],
    transformation_ctx="Joinartistalbum_node1705352482024",
)

# Script generated for node Join with tracks
Joinwithtracks_node1705352671439 = Join.apply(
    frame1=tracks_node1705352609335,
    frame2=Joinartistalbum_node1705352482024,
    keys1=["track_id"],
    keys2=["track_id"],
    transformation_ctx="Joinwithtracks_node1705352671439",
)

# Script generated for node Drop Fields
DropFields_node1705352831645 = DropFields.apply(
    frame=Joinwithtracks_node1705352671439,
    paths=["`.track_id`", "id"],
    transformation_ctx="DropFields_node1705352831645",
)

# Script generated for node Destination
Destination_node1705352884062 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1705352831645,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://spotify-project-aws-datapipeline/datalake/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="Destination_node1705352884062",
)

job.commit()
