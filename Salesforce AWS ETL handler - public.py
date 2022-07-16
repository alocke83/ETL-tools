#Salesforce ETL Processor
#Adam Locke July 1 2022
#checks the customer verbatim field for each entry in the Salesforce exctract, then replaces each newline character \n with a space " ", moreover, SOQL doesn't have a clear way that we could find to choose the order in which data is presented in the CSV, so this script reorganizes columns to meet the needs of the existing ingestion process.
#adam.locke@bmwfs.com, alocke1983@gmail.com


import csv

#files
file=open('SFOutput.csv')
#this script does require the user to manually change the csv filename exported by Salesforce to ensure proper handling
output=open('SFOutputNoNewline.csv','w', newline='')
#reader handles input extracted from Salesforce, writer handles the output csv
reader=csv.reader(file)
writer=csv.writer(output, quoting=csv.QUOTE_ALL)
#loop reader to get the contents to the ammendment process
for rows in reader:
    print("\n   NEW RECORD \n",len(rows),"\n")
    handler=rows[-1]
    handler=rows[-1].replace("\n"," ")
    rows.pop()
    rows.append(handler)
    n0=str(rows[0])
    n1=str(rows[1])
    n2=str(rows[2])
    n3=str(rows[3])
    n4=str(rows[4])
    n5=str(rows[5])
    n6=str(rows[6])
    n7=str(rows[7])
    n8=str(rows[8])
    n9=str(rows[9])
    print(n8)
    n8_check=n8.find('lation')
    print(n8_check)
    if n8_check != -1:
        n8='IQS Campaign'
        print('replaced escalation form; customer PII scrubbed from dataset')
    n10=str(rows[10])
    n11=str(rows[11])
    n12=str(rows[12])
    n13=str(rows[13])
    n14=str(rows[14])
    n15=str(rows[15])
    n16=str(rows[16])
    n17=str(rows[17])
    n18=str(rows[18])
    n19=str(rows[19])
    n20=str(rows[20])
    n21=str(rows[21])
    n22=str(rows[22])
    n23=str(rows[23])
    n24=str(rows[24])
    new_row=[n0,n1,n2,n3,n4,n5,n6,n7,n8,n9,n10,n11,n12,n13,n14,n15,n16,n17,n18,n19,n20,n21,n22,n23,n24]
    #print('\n EDITED RECORD \n',new_row)
    writer.writerow(new_row)
    new_row.clear()


#close files to finish the task
file.close()
output.close()
print('\n\n\nElimination of newline characters in verbatim complete, data can now be passed to internal partners for next stage of ingestion.')
input()
