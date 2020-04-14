from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext
import pyspark.sql.functions as func
import sys
import csv


def main(sc):
    spark = SparkSession(sc)
    sqlContext = SQLContext(sc)

    def parseCSV(idx, part):
        if idx==0:
            next(part)
        for p in csv.reader(part):
            yield (p[1].lower(), p[7].lower(), int(p[0][:4]))

    rows = sc.textFile(sys.argv[1]).mapPartitionsWithIndex(parseCSV)
    df = sqlContext.createDataFrame(rows, ('product', 'company', 'date'))
    dfComplaintsYearly = df.groupby(['date', 'product']).count().sort('product')
    dfComplaintsYearly = dfComplaintsYearly.withColumnRenamed("count",
                                                              "num_complaints")

    dfCompaniesCount = df.groupby(['date', 'product', 'company']).count()
    dfCompaniesYearly = dfCompaniesCount
                        .groupby(['date', 'product'])
                        .count()
                        .sort('product')
    dfCompaniesYearly = dfCompaniesYearly
                        .withColumnRenamed("count", "num_companies")

    dfMax = dfCompaniesCount.groupBy(['date', 'product']).max('count')
    dfTotal = dfCompaniesCount.groupBy(['date', 'product']).sum('count')
    dfRatio = dfMax.join(dfTotal, ['date', 'product'], how='inner')
    dfRatio = dfRatio.select('date', 'product', func.round(dfRatio[2]/dfRatio[3]*100)
                     .cast('integer')
                     .alias('percentage'))

    dfFinal = dfComplaintsYearly
              .join(dfCompaniesYearly.join(dfRatio, ['date', 'product'], how='inner'),
                    ['date', 'product'],
                    how='inner')
              .sort('product', 'date')

    dfFinal.saveAsTextFile(sys.argv[2])

if __name__=="__main__":
    sc = SparkContext()
    main(sc)
