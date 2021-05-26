from pymongo import MongoClient
import csv
import datetime

url = input("Введіть MongoDB URL: ")
url = url if url else "mongodb://localhost:27017"
client = MongoClient(url)


db = client.ZNO
db.ZNO.drop()

def read_file(year, file_name, log, max_block_size = 1000):
    start_time = datetime.datetime.now()
    print(f"Зчитування файлу {file_name}...")
    log.write(str(start_time) + ", " + file_name + ' - Відкрито файл\n')
    rec = db.inserted_docs.find_one({"year": year})
    if rec is None:
        skip_size = 0
    else:
        skip_size = rec["num_docs"]
        print(f"Пропускаємо {skip_size} документів...")
    readed_block = []
    block_counts = skip_size // max_block_size
    block_size = skip_size % max_block_size
    with open(file_name, "r", encoding="cp1251") as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=';')
        for row in csv_reader:
            if skip_size > 0:
                skip_size -= 1
                continue
            temp = row
            temp['year'] = year
            readed_block.append(temp)
            block_size += 1
            if block_size == max_block_size:
                db.collection_zno_data.insert_many(readed_block)
                block_counts += 1
                block_size = 0
                readed_block = []
                if block_counts == 1:
                    db.inserted_docs.insert_one({"num_docs": max_block_size, "year": year})
                else:
                    db.inserted_docs.update_one({
                        "year": year, "num_docs": (block_counts - 1) * max_block_size},
                        {"$inc": {
                            "num_docs": max_block_size
                        }  })
        if block_counts != 0 and readed_block:
            db.inserted_docs.update_one({
                "year": year, "num_docs": block_counts * max_block_size},
                {"$inc": {
                    "num_docs": block_size
                }  })
            db.collection_zno_data.insert_many(readed_block)
    end_time = datetime.datetime.now()
    log.write(str(end_time) + ", " + file_name + ' - Файл зчитаний\n')
    log.write('Витрачений час - ' + str(end_time - start_time) + '\n\n')



def write_file(file_name, result):
    with open(file_name, 'w', encoding="cp1251") as new_csv_file:
        csv_writer = csv.writer(new_csv_file)
        csv_writer.writerow(['Область', 'Рік', 'Найгірший бал з Історії України'])
        for row in result:
            year = row["_id"]["year"]
            regname = row["_id"]["regname"]
            max_score = row["max_score"]
            csv_writer.writerow([regname, year, max_score])


with open('logs.txt', 'w') as logs:
    read_file(2019, "Odata2019File.csv", logs)
    read_file(2020, "Odata2020File.csv", logs)


result = db.collection_zno_data.aggregate([

    {"$match": {"histTestStatus": "Зараховано"}},

    {"$group": {
        "_id": {
            "year": "$year",
            "regname": "$REGNAME"
        },
        "max_score": {
            "$min": "$histBall100"
        }
    }},

    {"$sort": {"_id": 1} }
])

write_file('result.csv', result)
