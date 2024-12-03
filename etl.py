from json import loads, dumps
import pandas as pd 
from pymongo import MongoClient

# Criando Dataframe Pandas
def load_csv(file_url):
    df = pd.read_csv(file_url)
    df["contact"] = df["contact"].apply(lambda x: "celular" if x == "cellular" else x)
    return df

# Transformando para Json
def csv_to_json(df):
    result = df.to_json(orient="records")
    parsed = loads(result)
    data = dumps(parsed, indent=4)  
    return data

def mongo_access(mongo_path, data):
    try: 
        # Instancia do Client
        myclient = MongoClient(mongo_path)

        # Create database
        db = myclient["Impacta"]

        # Create or Switched to collection
        collection = db["Bank"]

        # Inserting the entire list in the collection
        collection.insert_many(data)

        return "Inserido com sucesso"
    
    except Exception as e: 
        return f"Deu erro por que: {e}"
    

if __name__ == "__main__":
    file_url = "https://www.ime.usp.br/~keiji/bank.csv"
    df = load_csv(file_url)
    json_file = csv_to_json(df)

    # f = open("./data/.json", "w")
    # f.write(json_file)
    # f.close()

    mongo_path = "mongodb://localhost:27017/"
    # print(mongo_access(mongo_path, data=[json_file]))
    print(mongo_access(mongo_path, data=json_file))
