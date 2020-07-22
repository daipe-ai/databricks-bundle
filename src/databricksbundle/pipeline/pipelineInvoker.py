from typing import Dict
from pyspark.sql.dataframe import DataFrame
from databricksbundle.pipeline.Pipeline import Pipeline

def invokePipeline(pipeline: Pipeline) -> Dict[str, DataFrame]:
    dataFramesByName: Dict[str, DataFrame] = dict()

    for dataFrameLoaderName, dataFrameLoader in pipeline.getDataFrameLoaders().items():
        services = pipeline.getServices(dataFrameLoaderName)
        dataFramesByName[dataFrameLoaderName + '_df'] = dataFrameLoader(*services)

    for transformationName, transformation in pipeline.getTransformations().items():
        def transformSource(source: callable):
            return dataFramesByName[source.__name__ + '_df']

        dataframesToUse = tuple(map(transformSource, pipeline.getSource(transformationName)))
        services = pipeline.getServices(transformationName)

        df = transformation(*(dataframesToUse + services))
        dataFramesByName[transformationName + '_df'] = df

    return dataFramesByName
