import pandas as pd
import sqlite3
import csv
import os

# Connect to sqlite3 db

conn = sqlite3.connect("db.sqlite3")
cur = conn.cursor()

# Create class and instructor tables

cur.execute("""DROP TABLE IF EXISTS class""")

cur.execute("""CREATE TABLE IF NOT EXISTS class (
                id INTEGER PRIMARY KEY,
                term TEXT,
                year INT,
                subject TEXT,
                course_num INT,
                course_name TEXT
            );""")

cur.execute("""DROP TABLE IF EXISTS instructor""")

cur.execute("""CREATE TABLE IF NOT EXISTS instructor (
                id INTEGER PRIMARY KEY,
                class_id INT,
                instructor TEXT,
                Ap INT,
                A INT,
                Am INT,
                Bp INT,
                B INT,
                Bm INT,
                Cp INT,
                C INT,
                Cm INT,
                Dp INT,
                D INT,
                Dm INT,
                F INT
           );""")

term_mapping = {
        "sp": "Spring",
        "su": "Summer",
        "fa": "Fall",
        "wi": "Winter"
}

class_id = 1

# Loop through the .csv files in the assets directory 

for asset in os.listdir("assets"):

    # Skip non .csv files

    if not asset.endswith(".csv"):
        continue

    term, year = asset[:2], int(asset[2:6])

    filename = f"assets/{asset}"
    with open(filename, "r", encoding="utf-8-sig") as file:

        csv_file = csv.DictReader(file)

        rows = list(csv_file)
        num_rows = len(rows)

        instructors = dict()

        for i in range(0, num_rows - 1):

            row = rows[i]
            row_next = rows[i + 1]

            subject = row["Subject"]
            course_num = row["Course"]

            subject_next = row_next["Subject"]
            course_num_next= row_next["Course"]

            course_name = row["Course Title"]
            
            instructor = row["Primary Instructor"]
            
            Ap = int(row["A+"])
            A = int(row["A"])
            Am = int(row["A-"])
            Bp = int(row["B+"])
            B = int(row["B"])
            Bm = int(row["B-"])
            Cp = int(row["C+"])
            C = int(row["C"])
            Cm = int(row["C-"])
            Dp = int(row["D+"])
            D = int(row["D"])
            Dm = int(row["D-"])
            F = int(row["F"])

            if instructor not in instructors:

                instructors[instructor] = {
                    "Ap": Ap,
                    "A": A,
                    "Am": Am,
                    "Bp": Bp,
                    "B": B,
                    "Bm": Bm,
                    "Cp": Cp,
                    "C": C,
                    "Cm": Cm,
                    "Dp": Dp,
                    "D": D,
                    "Dm": Dm,
                    "F": F
                }

            else:

                instructors[instructor]["Ap"] += Ap 
                instructors[instructor]["A"] += A
                instructors[instructor]["Am"] += Am 
                instructors[instructor]["Bp"] += Bp 
                instructors[instructor]["B"] += B
                instructors[instructor]["Bm"] += Bm 
                instructors[instructor]["Cp"] += Cp 
                instructors[instructor]["C"] += C
                instructors[instructor]["Cm"] += Cm 
                instructors[instructor]["Dp"] += Dp 
                instructors[instructor]["D"] += D
                instructors[instructor]["Dm"] += Dm 
                instructors[instructor]["F"] += F

            if subject == subject_next and course_num == course_num_next:

                continue

            for instructor, grade in instructors.items():

                cur.execute("""INSERT INTO instructor (class_id, instructor, Ap, A, Am, Bp, B, Bm, Cp, C, Cm, Dp, D, Dm, F)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (class_id, instructor, grade["Ap"], 
                               grade["A"], grade["Am"], grade["Bp"], grade["B"], grade["Bm"], grade["Cp"], grade["C"], grade["Cm"],
                               grade["Dp"], grade["D"], grade["Dm"], grade["F"]))

            instructors = dict()

            cur.execute("""INSERT INTO class (term, year, subject, course_num, course_name)
                           VALUES (?, ?, ?, ?, ?)""", (term, year, subject, course_num, course_name))

            class_id += 1

# Commit changes and close connection

conn.commit()

cur.close()
conn.close()
