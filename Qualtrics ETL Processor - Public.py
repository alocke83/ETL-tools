#Qualtrics Ingestion Processor
#Adam Locke June 15 2022
#inserts blank columns and values into rows of a CSV being used as interim data access for the NJ-based BMW Qualtrics team.  The script is adding in dummy values to legacy columns so that the old ingestion system will accept the CSV produced by Salesforce. This is an interim solution while a formal Salesforce to Qualtrics ingestion solution is under development.
#adam.locke@bmwfs.com, alocke1983@gmail.com


import csv

file=''
#SFOutput.csv is the target filename for the results file
processing_list=[]
export_list=[]

#file open
file=open('SFOutput.csv')
#this script does require the user to manually rename the source file
output=open('QualtricsInput.csv', 'w', newline='')
#reader handles input extracted from Salesforce, writer handles the output csv
reader=csv.reader(file)
writer=csv.writer(output)
#loop reader to get the contents to the ammendment process
#for rows in reader:
    #print(rows)
for rows in reader:
    print(len(rows))
    #some of the commented out sectionsof this area are edits that were applied in collaboration with the NJ team in order to adapt their ingestion to the new data, others are troubleshooting lines added to confirm function
    #
    #print(rows[15])
    #print(len(rows))
    #print(rows)
    #singularly append references in rows to the processing list, appending the blanks at the appropriate index points.
    #append first seven entried, then add Hall dummy
    processing_list.append(rows[0])
    #print(processing_list)
    processing_list.append(rows[1])
    #print(processing_list)
    processing_list.append(rows[2])
    #print(processing_list)
    processing_list.append(rows[3])
    processing_list.append(rows[4])
    processing_list.append(rows[5])
    processing_list.append(rows[6])
    processing_list.append(rows[7])
    processing_list.append('Old Hall')
    #append next fault_code_type, then add Old KIFA
    processing_list.append(rows[8])
    processing_list.append('Old KIFA')
    #append next 4 indecies, then add Old ID (maybe can code this later)
    processing_list.append(rows[9])
    processing_list.append(rows[10])
    processing_list.append(rows[11])
    processing_list.append(rows[12])
    processing_list.append('Old ID')
    #Append state dummy, consider writing code to slice the state from the customer address
    processing_list.append('Old State')
    #next append vehicle mileage from the entry, then add IQS4, IQS5, and DMU
    #now append the level 2 and level 3 codes in a single string
    concat_code=rows[14]+' - '+rows[13]
    if concat_code=='Case_Code__r.Fault_Code__r.Name - Case_Code__r.Level_3__c':
        concat_code='concatenated_L2L3'
    #print(concat_code)
    processing_list.append(concat_code)
    processing_list.append('Old IQS4')
    processing_list.append('Old IQS5')
    processing_list.append('Old DMU')
    #append the final entry review relevant
    processing_list.append(rows[14])
    processing_list.append(rows[15])
    processing_list.append(rows[16])
    print(rows[14],rows[15],rows[16])
    #print(processing_list)
    #now convert the values from the processing list into variables appropriate for the writer to use for the output csv
    brand=processing_list[0]
    model_year=processing_list[1]
    model=processing_list[2]
    platform=processing_list[3]
    VIN=processing_list[4]
    prod_date=processing_list[5]
    call_date=processing_list[6]
    plant=processing_list[7]
    hall=processing_list[8]
    feedback_type=processing_list[9]
    kifa_cat=processing_list[10]
    kifa_code=processing_list[11]
    cust_feedback=processing_list[12]
    selling_dealer=processing_list[13]
    calling_agent=processing_list[14]
    old_ID=processing_list[15]
    state=processing_list[16]
    reported_mileage=processing_list[17]
    iqs4_code=processing_list[18]
    iqs5_code=processing_list[19]
    dmu=processing_list[20]
    ccrp=processing_list[21]
    cust_verbatim=processing_list[23]
    #now write these values to the output
    new_row=[brand,model_year,model,platform,VIN,prod_date,call_date,plant,hall,feedback_type,kifa_cat,kifa_code,cust_feedback,selling_dealer,calling_agent,old_ID,state,reported_mileage,iqs4_code,iqs5_code,dmu,ccrp,cust_verbatim]
    #print(new_row)
    writer.writerow(new_row)
    #clear the processing list and preapre for the next row
    processing_list.clear()
    new_row.clear()

#close files to finish the task
file.close()
output.close()
print('Insertion of legacy fields complete, data can now be passed to NJ colleagues for Qualtrics ingestion')
input()
