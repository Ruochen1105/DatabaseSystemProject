
# Individual work by Ruochen Miao rm5327
import datetime
import random
from socket import *

def print_product(id, start, end, charge):
    print(f"Plan ID: {id}")
    print(f"Start date: {str(start.month) + '/' + str(start.day) + '/' + str(start.year)}")
    print(f"End date: {str(end.month) + '/' + str(end.day) + '/' + str(end.year)}")
    print(f"Charge: {charge}")

def main():

    age = input("Input age: ")
    sex = input("Input sex (female / male): ")
    bmi = input("Input bmi: ")
    children = input("Input number of children: ")
    smoker = input("Input if is a smoker (yes / no): ")
    region = input("Input the region (southwest / northwest / northeast / southeast): ")

    with socket(AF_INET, SOCK_STREAM) as clientSocket:
        clientSocket.connect(("127.0.0.1", 5327))
        clientSocket.send(f"GET / HTTP/1.1\r\nHost: {'127.0.0.1'}\r\n\r\n{age, sex, bmi, children, smoker, region}".encode())
        response = b''
        while True:
            data = clientSocket.recv(1024)
            if not data:
                break
            response += data

    _, body = response.decode().split("\r\n\r\n", 1)

    best, other = body.split("SEP", 1)
    best = eval(best)
    other = eval(other)

    print("--------------------------------------------------")
    print("--------------------------------------------------")

    print("Your best-fit product is:")
    print_product(best[5], best[6], best[7], best[10])
    print("\r\r\r\r\r")
    print("")
    print("Here are some other options:")
    for p in random.sample(other, 3):
        print_product(p[5], p[6], p[7], p[10])
        print("")

if __name__ == "__main__":
    main()
