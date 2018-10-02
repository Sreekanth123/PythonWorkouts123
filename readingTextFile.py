import sys
sys.path.insert(0, '.')
from pyspark import SparkContext, SparkConf
from commons.Utils import Utils

def splitComma(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    return "{}, {}".format(splits[1], splits[6])

if __name__ == "__main__":
    conf = SparkConf().setAppName("readTextFileData").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    readTextFileData = sc.textFile("in/readTextFileData.text")
    readTextFileDataInUSA = readTextFileData.filter(lambda line: float(Utils.COMMA_DELIMITER.split(line)[6]) > 40)
    readTextFileDataNameAndCityNames = readTextFileDataInUSA.map(splitComma)
    readTextFileDataNameAndCityNames.saveAsTextFile("out/readTextFileData_by_latitude.text")
