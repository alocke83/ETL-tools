#2022 May Adam Locke
#Script processing CSV data from blitzz
#alocke1983@gmail.com, adam.locke@bmwfs.com

#!!important
# the CSV you download from blitzz MUST be named 'blitzdl.csv' or the script will not find it.  It will also overwrite the output file so if you want to keep records move it when you are done.
# the script requires that you make one adjustment before processing: in the csv, use MS excel and format the Start Time to a date, eliminating the time portion of the datetime stamp; if there's time in Q3 I will release an improved script to handle this on its own.


#import statements
import csv

#download the blitzz csv before running the script
blitzz_download= open('blitzdl.csv')
scores = {}


#function to open the blitzz csv
def parse_csv():
    check=csv.reader(blitzz_download,dialect='excel')
    header = next(check)
    data=[row for row in check]
    blitzz_download.close()
    
    agent_list=['test entry']
    for rows in data:
        if rows[1] not in agent_list:
            agent_list.append(rows[1])
    date_list=[]
    for rows in data:
        if rows[4] not in date_list:
            date_list.append(rows[4])
    #these commented rows are for troubleshooting purposes
    #print(agent_list)
    #step one is agent list creation
    #step two is rejection of same-ident, same-date records
    reference_check=[]
    name_check=[]
    incident_count = {}
    #print(incident_count)
    #print(data)
    #now i have a dictionary in which to count the incidents, so I must count the incidents on the rule one cusotmer per agent per day can give one point
    scored_entries=[]
    for dates in date_list:
        for rows in data:
            if rows[2] not in reference_check and rows[1] not in name_check and rows[4] == dates:
                reference_check.append(rows[2])
                name_check.append(rows[1])
                scored_entries.append(rows)
            elif rows[2] in reference_check and rows[1] not in name_check and rows[4] == dates:
                name_check.append(rows[4])
                scored_entries.append(rows)
            elif rows[2] not in reference_check and rows[4] == dates:
                reference_check.append(rows[2])
                scored_entries.append(rows)
        name_check.clear()
        reference_check.clear()
    
    #now I have a list of references and dates, and I have added to a new list of scored entries all the records that are first reference ID and first appearance by date
    #print(scored_entries)
        #I want to verify visually that the logic is being output as desired, so I will print a broader collection of the data to a check csv for manual verification
    prove=open('check.csv', 'w')        
    proof=csv.writer(prove)
    for lines in scored_entries:
        entry=[lines]
        proof.writerow(entry)
    prove.close()
    for rows in scored_entries:
        print(rows[1],'  ',rows[2],'   ',rows[4])
    count_names_list=[]
    for rows in scored_entries:
        count_names_list.append(rows[1])
    counter=0
    for names in agent_list:
        counter=count_names_list.count(names)
        incident_count[names]=counter
        print(names,'     ',counter)
        #counter=0

    #print(incident_count)
    rvgout=open('blitz_output.csv', 'w', newline="")
    writer=csv.writer(rvgout)
    for keys in incident_count:
        row=[keys,incident_count[keys]]
        #print(row)
        writer.writerow(row)
    rvgout.close()
                    

#mainline processing
parse_csv()
