import pandas as pd
import numpy as np
import os

def generate_sample_customers(n=200, outfile="data/samples/sample_customers.csv"):
    os.makedirs("data/samples", exist_ok=True)

    ids = [f"C{str(i+1).zfill(5)}" for i in range(n)]
    first = np.random.choice(["Liam","Olivia","Noah","Emma","Oliver","Ava","Elijah","Sophia"], size=n)
    last  = np.random.choice(["Smith","Jones","Brown","Miller","Davis","Garcia"], size=n)
    age   = np.random.randint(18, 90, size=n)
    country = np.random.choice(["USA","CAN","UK","IND"], size=n)
    state = [np.random.choice(["NY","CA","TX","FL"]) if c=="USA" else "" for c in country]
    email = [f"{f[0].lower()}.{l.lower()}@example.com" for f,l in zip(first,last)]

    df = pd.DataFrame({
        "customer_id": ids,
        "first_name": first,
        "last_name": last,
        "age": age,
        "country": country,
        "state": state,
        "email": email
    })
    df.to_csv(outfile, index=False)
    print("Sample customers saved to:", outfile)


def generate_sample_orders(n=500, customers_file="data/samples/sample_customers.csv",
                           outfile="data/samples/sample_orders.csv"):
    os.makedirs("data/samples", exist_ok=True)

    df_cust = pd.read_csv(customers_file)
    customer_ids = df_cust["customer_id"].tolist()

    order_ids = [f"O{str(i+1).zfill(6)}" for i in range(n)]

    valid_count = int(n * 0.9)
    invalid_count = n - valid_count

    valid_customers = np.random.choice(customer_ids, size=valid_count)
    invalid_customers = [f"C9999X{i}" for i in range(invalid_count)]

    cust_list = np.concatenate([valid_customers, invalid_customers])
    np.random.shuffle(cust_list)

    statuses = ["PENDING","COMPLETE","CANCELLED"]
    amounts = [round(float(np.random.uniform(5,500)),2) for _ in range(n)]
    dates = pd.date_range(start="2024-01-01", periods=n).strftime("%Y-%m-%d")

    df = pd.DataFrame({
        "order_id": order_ids,
        "customer_id": cust_list,
        "total_amount": amounts,
        "status": np.random.choice(statuses, size=n),
        "order_date": dates
    })

    df.to_csv(outfile, index=False)
    print("Sample orders saved to:", outfile)
if __name__ == "__main__":
    generate_sample_customers()
    generate_sample_orders()
