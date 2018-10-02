import sys
sys.path.insert(0, '.')
from pyspark import SparkContext, SparkConf
from commons.Utils import Utils

def splitComma(line: str):
    splits = Utils.COMMA_DELIMITER.split(line)
    return "{}, {}".format(splits[1], splits[6])

if __name__ == "__main__":
    
    #Creating the application with setting configuration
    conf = SparkConf().setAppName("readTextFileData").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    
    #Reading the text file with spark context
    readTextFileData = sc.textFile("in/readTextFileData.text")
    
    #Filtering each line with value greater then 40
    readTextFileDataInUSA = readTextFileData.filter(lambda line: float(Utils.COMMA_DELIMITER.split(line)[6]) > 40)
    
    #In each line getting only specific columns from file
    readTextFileDataNameAndCityNames = readTextFileDataInUSA.map(splitComma)
    
    #Writing output of the program into file
    readTextFileDataNameAndCityNames.saveAsTextFile("out/readTextFileData_by_latitude.text")
