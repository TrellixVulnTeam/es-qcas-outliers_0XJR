import pandas as pd


class maker:

    def __init__(self,filename="test_aggregation_inputData.csv"):
        self.file = filename


    def createJson(self):
        fileDF = pd.read_csv(self.file)
        #file_second = pd.read_csv('means2.csv')
        #mergeDF = pd.merge(fileDF,file_second,on=['region','strata'],how='left')
        fileJSON = fileDF.to_json(orient='records')
        with open("agg_input.json","w+") as files:
            files.write(fileJSON)



if __name__ == '__main__':
    make = maker()
    make.createJson()