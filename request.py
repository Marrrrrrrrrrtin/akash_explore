import requests
import json
import csv
from multiprocessing.dummy import Pool as ThreadPool

def get_data(index):
    try:
        response = requests.get('https://api.cloudmos.io/v1/blocks/' + str(index))
        
        # Ensure the request was successful
        if response.status_code == 200:
            # Decode the bytes object to a string and then parse it as JSON
            data = json.loads(response.content.decode('utf-8'))

            # Extract the transactions part
            transactions = data.get('transactions', [])

            if transactions:
                # Initialize a list to store the formatted transaction details
                transactions_details = []

                # Iterate over each transaction and format the details
                for tx in transactions:
                    tx_hash = tx.get("hash", "N/A")
                    tx_success = tx.get("isSuccess", "N/A")
                    tx_fee = tx.get("fee", "N/A")
                    tx_datetime = tx.get("datetime", "N/A")

                    # Extract messages
                    messages = tx.get("messages", [])
                    for msg in messages:
                        id = msg.get('id', 'N/A')
                        typ = msg.get('type', 'N/A')
                        amount = msg.get('amount', 'N/A')

                        # Append the transaction details to the list
                        transactions_details.append([tx_hash, tx_success, tx_fee, tx_datetime, id, typ, amount])

                # Return the list of transaction details
                print(f"Sucessful {index}.")
                return transactions_details

            else:
                #print(f"No transactions {index}.")
                return []

        else:
            print(f"Failed {index}, code: {response.status_code}")
            return []

    except Exception as e:
        print(f"Exception occurred for index {index}: {str(e)}")
        return []

#13504274
# Main part to write into CSV
def func(index):
    with open(str(index) + '.csv', 'a', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        for i in range(15530000 + index * 100, 15530000 + index * 100 + 100):
            data = get_data(i)
            if data:
                for detail in data:
                    writer.writerow(detail)


#run with several threads with small pieces
for i in range(20, 50):
    pool = ThreadPool(500)
    pool.map(func, range(i*100, i*100 + 100))
    pool.close()
    print(f'We have already finished the round {i}')

