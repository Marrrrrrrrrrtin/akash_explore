import csv


with open('merge.csv', 'w', encoding = 'utf-8', newline = '') as w:
    writer = csv.writer(w)
    cur = ''
    #change the number depending the number of files
    for i in range(1, 10428):
        with open(str(i) + '.csv', 'r', encoding = 'utf-8', newline = '') as r:
            reader = csv.reader(r)
            for row in reader:
                #remove the duplicate
                if row[0] != cur:
                    #let the message more readable
                    row[5] = row[5].split('.')[-1][3:]
                    writer.writerow(row)
                    cur = row[0]



