
# Individual work by Ruochen Miao rm5327
import pyodbc
from credentials import Credentials
import pandas as pd
from tqdm import tqdm

CREDENTIAL = Credentials()

def connect(func):
    def inner(*args, **kwargs):
        with pyodbc.connect('DRIVER=' + CREDENTIAL.driver +
                            ';SERVER=' + CREDENTIAL.server +
                            ';PORT=1433;DATABASE=' + CREDENTIAL.database +
                            ';UID=' + CREDENTIAL.username +
                            ';PWD=' + CREDENTIAL.password) as conn:
            cursor = conn.cursor()
            result = func(cursor, *args, **kwargs)
            cursor.close()
        return result
    return inner

@connect
def test(cursor):
    cursor.execute("SELECT TOP 3 name, collation_name FROM sys.databases")
    row = cursor.fetchone()
    while row:
        print (str(row[0]) + " " + str(row[1]))
        row = cursor.fetchone()

@connect
def create_db(cursor):
    cursor.execute(
        """
        CREATE TABLE CUSTOMER (
            id INT NOT NULL IDENTITY(1, 1),
            age INT,
            sex CHAR(1),
            BMI FLOAT,
            num_children INT,
            smoker BIT,
            region VARCHAR(100),
            medical_charges FLOAT,
            PRIMARY KEY (id)
        );

        CREATE TABLE POLICY (
            Plan_Id VARCHAR(100),
            StateCode CHAR(2),
            tobacco BIT,
            RateEffectiveDate DATE,
            RateExpirationDate DATE,
            PRIMARY kEY (plan_id, RateEffectiveDate, RateExpirationDate)
        );

        CREATE TABLE RATE (
            Plan_Id VARCHAR(100),
            RateEffectiveDate DATE,
            RateExpirationDate DATE,
            Age VARCHAR(20),
            FamilyOption Bit,
            IndividualRate FLOAT,
            IndividualTobaccoRate FLOAT,
            Couple FLOAT,
            PrimarySubscriberAndOneDependent FLOAT,
            PrimarySubscriberAndTwoDependents FLOAT,
            PrimarySubscriberAndThreeOrMoreDependents FLOAT,
            CoupleAndOneDependent FLOAT,
            CoupleAndTwoDependents FLOAT,
            CoupleAndThreeOrMoreDependents FLOAT,
            PRIMARY KEY (Plan_Id, Age, RateEffectiveDate, RateExpirationDate),
            FOREIGN KEY (Plan_Id, RateEffectiveDate, RateExpirationDate)
            REFERENCES POLICY(Plan_Id, RateEffectiveDate, RateExpirationDate)
        );
        """
    )

@connect
def drop_all(cursor):
    cursor.execute(
        """
        DROP TABLE CUSTOMER;
        DROP TABLE RATE;
        DROP TABLE POLICY;
        """
    )

@connect
def insert_CUSTOMER(cursor, data):
    for _, row in tqdm(data.iterrows(), desc="Inserting CUSTOMER", total=data.shape[0]):
        cursor.execute(
            f"""
            INSERT INTO CUSTOMER (age, sex, BMI, num_children, smoker, region, medical_charges)
            VALUES ({row.age}, '{row.sex[0]}', {row.bmi}, {row.children},
            {1 if row.smoker == 'yes' else 0}, '{row.region}', {row.charges});
            """
        )

@connect
def insert_POLICY(cursor, data):
    for _, row in tqdm(data.iterrows(), desc="Inserting POLICY", total=data.shape[0]):
        cursor.execute(
            f"""
            INSERT INTO POLICY (Plan_Id, StateCode, Tobacco, RateEffectiveDate, RateExpirationDate)
            VALUES ('{row.PlanId}', '{row.StateCode}', {1 if row.Tobacco == 'Tobacco User/Non-Tobacco User' else 0},
            '{row.RateEffectiveDate}', '{row.RateExpirationDate}')
            """
        )

@connect
def insert_RATE(cursor, data):
    for _, row in tqdm(data.iterrows(), desc="Inserting RATE", total=data.shape[0]):
        cursor.execute(
            f"""
            INSERT INTO RATE (Plan_Id, RateEffectiveDate, RateExpirationDate, Age, FamilyOption, IndividualRate,
            IndividualTobaccoRate, Couple, PrimarySubscriberAndOneDependent, PrimarySubscriberAndTwoDependents,
            PrimarySubscriberAndThreeOrMoreDependents, CoupleAndOneDependent, CoupleAndTwoDependents, CoupleAndThreeOrMoreDependents)
            VALUES ('{row.PlanId}', '{row.RateEffectiveDate}', '{row.RateExpirationDate}','{row.Age if row.Age != "NULL" else "N/A"}', {1 if row.Age == "Family Option" else 0},
            {row.IndividualRate}, {row.Couple}, {row.PrimarySubscriberAndOneDependent}, {row.PrimarySubscriberAndTwoDependents},
            {row.IndividualTobaccoRate}, {row.PrimarySubscriberAndThreeOrMoreDependents}, {row.CoupleAndOneDependent}, {row.CoupleAndTwoDependents},
            {row.CoupleAndThreeOrMoreDependents});
            """
        )

@connect
def query(cursor, sql):
    cursor.execute(sql)
    return cursor.fetchall()

if __name__ == "__main__":
    test() # can be used to start the db server, as it's auto-paused if thera are no activities for more than 1 hour
    drop_all()
    create_db()

    health_data = pd.read_csv("health.csv") # unzipped after downloaded from www.kaggle.com/datasets/teertha/ushealthinsurancedataset/download?datasetVersionNumber=1
    insert_CUSTOMER(health_data)

    insurance_data = pd.read_csv("Rate_PUF.csv") # unzipped after downloaded from download.cms.gov/marketplace-puf/2024/rate-puf.zip

    insurance_data = insurance_data[["StateCode", "RateEffectiveDate", "RateExpirationDate", "PlanId", "Tobacco", "Age",
                                    "IndividualRate", "IndividualTobaccoRate", "Couple", "PrimarySubscriberAndOneDependent",
                                    "PrimarySubscriberAndTwoDependents", "PrimarySubscriberAndThreeOrMoreDependents",
                                    "CoupleAndOneDependent", "CoupleAndTwoDependents", "CoupleAndThreeOrMoreDependents"]]
    policy_data = insurance_data[["StateCode", "RateEffectiveDate", "RateExpirationDate", "PlanId", "Tobacco"]].drop_duplicates()
    insert_POLICY(policy_data)

    rate_data = insurance_data[["PlanId", "RateEffectiveDate", "RateExpirationDate", "Age",
                                "IndividualRate", "IndividualTobaccoRate", "Couple", "PrimarySubscriberAndOneDependent",
                                "PrimarySubscriberAndTwoDependents", "PrimarySubscriberAndThreeOrMoreDependents",
                                "CoupleAndOneDependent", "CoupleAndTwoDependents", "CoupleAndThreeOrMoreDependents"]]
    rate_data = rate_data.groupby(["PlanId", "RateEffectiveDate", "RateExpirationDate", "Age"]).mean().reset_index().fillna("NULL")

    insert_RATE(rate_data)
