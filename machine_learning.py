
# Individual work by Ruochen Miao rm5327
import pandas as pd
import statsmodels.api as sm
from manage_db import query

def train():

    sql_command = """
        SELECT * FROM CUSTOMER
    """
    query_result = query(sql_command)
    columns = ['age', 'sex', 'bmi', 'children', 'smoker', 'region', 'charges']
    health_data = pd.DataFrame([i[1:] for i in query_result], columns=columns)

    # health_data = pd.read_csv("health.csv") # unzipped after downloaded from www.kaggle.com/datasets/teertha/ushealthinsurancedataset/download?datasetVersionNumber=1
    health_data = pd.get_dummies(health_data, columns=["sex", "smoker", "region"], drop_first=True)

    X = health_data.drop("charges", axis=1)
    y = health_data["charges"]

    model = sm.OLS(y, X).fit()
    model.save("trained.pickle")

    return model


def predict(model):

    age = 25
    bmi = float(25)
    children = 0
    sex = "male",
    smoker = "yes",
    region = "northwest"

    new_data = pd.DataFrame(
        [(age, bmi, children, (1 if sex == "male" else 0), (1 if smoker == "yes" else 0),
        (1 if region == "northwest" else 0), (1 if region == "southeast" else 0), (1 if region == "southwest" else 0))],
        columns=['age', 'bmi', 'children', 'sex_m', 'smoker_True', 'region_northwest', 'region_southeast', 'region_southwest']
    )
    print(new_data)

    return model.predict(new_data)


if __name__ == "__main__":
    try:
        model = sm.load("trained.pickle")
    except FileNotFoundError:
        model = train()
    while(1):
        print(predict(model))
        break
