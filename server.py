
# Individual work by Ruochen Miao rm5327
import pandas as pd
from socket import *
import statsmodels.api as sm
import threading

from machine_learning import train
from manage_db import query

region_to_state = {
    "northeast": ("CT", "DE", "ME", "MD", "MA", "NH", "NJ", "NY", "PA", "RI", "VT"),
    "northwest": ("AK", "ID", "MT", "OR", "WA", "WY"),
    "southeast": ("AL", "AR", "FL", "GA", "KY", "LA", "MS", "NC", "SC", "TN", "VA", "WV"),
    "southwest": ("AZ", "CA", "CO", "HI", "NV", "NM", "OK", "TX", "UT")
}

class products:
    def __init__(self, age, sex, bmi, children, smoker, region):
        self.age = age
        self.sex = sex
        self.bmi = bmi
        self.children = children
        self.smoker = 1 if smoker == "yes" else 0
        self.region = region

    def exec_query(self):
        if self.age <= 14:
            sql_age = "'0-14'"
        elif self.age >= 64:
            sql_age = "'64 and over'"
        else:
            sql_age = "\'" + str(self.age) + "\'"
        sql_command = f"""
        SELECT *
        FROM POLICY INNER JOIN RATE
        ON POLICY.Plan_id = RATE.Plan_id
            AND POLICY.RateEffectiveDate = RATE.RateEffectiveDate
            AND POLICY.RateExpirationDate = RATE.RateExpirationDate
        WHERE POLICY.StateCode IN {region_to_state[self.region]}
            AND POLICY.tobacco = {self.smoker}
            AND RATE.Age = {sql_age}
        """
        return query(sql_command)

def handler(client_socket, OLSmodel):
    _, message = client_socket.recv(4096).decode().split("\r\n\r\n", 1)
    conditions = message.split(",")
    age = int(conditions[0].strip("\'\"() "))
    sex = conditions[1].strip("\'\"() ")
    bmi = float(conditions[2].strip("\'\"() "))
    children = int(conditions[3].strip("\'\"() "))
    smoker = conditions[4].strip("\'\"() ")
    region = conditions[5].strip("\'\"() ")

    new_data = pd.DataFrame(
        [(age, bmi, children, (1 if sex == "male" else 0), (1 if smoker == "yes" else 0),
        (1 if region == "northwest" else 0), (1 if region == "southeast" else 0), (1 if region == "southwest" else 0))],
        columns=['age', 'bmi', 'children', 'sex_m', 'smoker_True', 'region_northwest', 'region_southeast', 'region_southwest']
    )
    prediction = float([OLSmodel.predict(new_data)][0])

    prod = products(age, sex, bmi, children, smoker, region)
    all_prods = prod.exec_query()

    best_fit = None
    holder = 0
    for i in all_prods:
        if abs(i[10] - prediction) < abs(holder - prediction):
            holder = i[10]
            best_fit = i

    client_socket.send(f"HTTP/1.1 200 OK\r\n\r\n{str(best_fit) + 'SEP' + str(all_prods)}".encode())
    client_socket.close()


def main():
    with socket(AF_INET, SOCK_STREAM) as serverSocket:
        serverSocket.bind(("", 5327))
        serverSocket.listen()
        try:
            OLSmodel = sm.load("trained.pickle")
        except FileNotFoundError:
            train()
            OLSmodel = sm.load("trained.pickle")
        print("Server running...")
        while True:
            client_socket, _ = serverSocket.accept()
            threading.Thread(target=handler, args=(client_socket, OLSmodel), daemon=True).start()

if __name__ == "__main__":
    main()
