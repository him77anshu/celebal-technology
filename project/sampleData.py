import pandas as pd
import faker

fake = faker.Faker()

data = []
for i in range(500):
    data.append({
        "CustomerID": i + 1,
        "FirstName": fake.first_name(),
        "LastName": fake.last_name(),
        "Email": fake.email(),
        "Phone": fake.phone_number(),
        "City": fake.city(),
        "Country": fake.country(),
        "Age": fake.random_int(min=18, max=90)
    })

df = pd.DataFrame(data)
df.to_csv("customers.csv", index=False)
