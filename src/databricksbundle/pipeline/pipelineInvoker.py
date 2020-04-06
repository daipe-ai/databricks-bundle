from typing import Dict
from pyspark.sql.dataframe import DataFrame
from databricksbundle.pipeline.Pipeline import Pipeline

def invokePipeline(pipeline: Pipeline) -> Dict[str, DataFrame]:
    dataFramesByName = dict()

    for dataFrameLoaderName, dataFrameLoader in pipeline.getDataFrameLoaders().items():
        dataFramesByName[dataFrameLoaderName + '_df'] = dataFrameLoader()

    for transformationName, transformation in pipeline.getTransformations().items():
        def transformSource(source: callable):
            return dataFramesByName[source.__name__ + '_df']

        dataframesToUse = list(map(transformSource, pipeline.getSource(transformationName)))

        df = transformation(*dataframesToUse)
        dataFramesByName[transformationName + '_df'] = df

    return dataFramesByName
