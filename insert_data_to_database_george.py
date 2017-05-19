import os
import datetime
import psycopg2 as psy
import psycopg2.extras as ex
import csv

################################################################################
# 'path' parameter should be modified to different computers                   #
# 'hour' & 'minu' parameter control the numbers of data for update             #
################################################################################
datebase = "spectrum"
user = "spectrum"
pwd = "1qaz2wsx"
host = "140.112.21.120"
port = "9527"

print 'start....'

# YOTO database APIs
def read():
    conn = psy.connect(database=datebase, user=user, password=pwd, host=host, port=port)
    cur = conn.cursor(cursor_factory = ex.RealDictCursor)

    cur.execute("SELECT * FROM local_incumbents WHERE name = 'BLROOF';")
    rows = cur.fetchone()
    for key,element in enumerate(rows) :
        print('column name:',element,'value:',rows[element])

    cur.close()
    conn.close()
def write(center_freq, avg_power, record_time):
    # zero means dummy
    lis = []
    lis.append("'BLROOF'")
    lis.append("'bn7884s@gmail.com'")
    lis.append(3)
    lis.append("'BL530'")
    lis.append(0)
    lis.append(center_freq)
    lis.append(121.5422962)
    lis.append(25.0192838)
    lis.append(avg_power)
    lis.append(0)
    lis.append(0)
    lis.append(0)
    lis.append(0)
    lis.append(0)
    lis.append(0)
    lis.append(200)    #protected_distance
    lis.append("'%s'"%(record_time))
    lis.append("'%s'"%(record_time))
    lis.append("'%s'"%(record_time))
    lis.append("'2019-01-01 00:00:00'")    #dummy
    conn = psy.connect(database=datebase, user=user, password=pwd, host=host, port=port)
    cur = conn.cursor()
    cur.execute("INSERT INTO local_incumbents ( name, contact_name, owner_id, addr, device_type, band_id, lng, lat, bs_pwr, ms_pwr, bs_ant_gain, accuracy, bs_sens, ms_sens, haat, protected_distance, start_time, updated_at, created_at , end_time )\
                                      VALUES (%s,%s,%d,%s,%d,%d,%f,%f,%d,%d,%d,%d,%d,%d,%d,%d,%s,%s,%s,%s) "%(lis[0],lis[1],lis[2],lis[3],lis[4],lis[5],lis[6],lis[7],lis[8],lis[9],lis[10],lis[11],lis[12],lis[13],lis[14],lis[15],lis[16],lis[17],lis[18],lis[19]))
    conn.commit()
    cur.close()
    conn.close()

def delete():
    conn = psy.connect(database=datebase, user=user, password=pwd, host=host, port=port)
    cur = conn.cursor()
    id = "23"
    delstatmt = "DELETE FROM local_incumbents WHERE name = 'BLROOF';"
    cur.execute(delstatmt)
    conn.commit()
    cur.close()
    conn.close()
################################################################################


path = "C:/Users/george_lu/Desktop/gest/"
freq_uploaded = [2302.5 + 5*i for i in range(20)]
freq_uploaded = str(freq_uploaded)
num_prev_index = 1
while 1:
    folder = []
    data_set = []
    recorded_date = []
    date = datetime.datetime.now()
    year = date.strftime("%Y")
    month = date.strftime("%m")
    day = date.strftime("%d")
    f = open(path + "processed_data.txt","a+")
    f.close()

    if month[0] == '0':
        month = month[1]
    if day[0] == '0':
        day = day[1]

    datenow = str(year) + '.' + str(month) + '.' + str(day)
    for root, dirs, files in os.walk(path):
        folder.append(dirs)
    try:  #wait for today's folder to be created
        today_index = folder[0].index(datenow)
        if today_index == 0:    #not update today's info
            print "no folder before today"
        else:                  # update previous info

            if today_index - num_prev_index <= -1:
                print "all of the folders updated"
            else:
                uploadfolder = folder[0][today_index - num_prev_index]

                f = open(path + "processed_data.txt","r")
                line = f.read()
                recorded_date = line.split('\n')
                recorded_date.remove('')
                f.close()

                if uploadfolder in recorded_date:
                    num_prev_index = num_prev_index + 1  #the file is already updated and try previous folder
                else:
                    for root1, dirs1, files1 in os.walk(path + uploadfolder):
                        data_set = files1
                    if len(data_set) == 0:
                        f = open(path + "processed_data.txt","a+")
                        f.write("%s\n"%(uploadfolder))    # no data and record also
                        f.close()
                        print str(uploadfolder) + " is empty"
                    else:
                        print "currently process " + str(uploadfolder)

                        for i in range(len(data_set)):
                            try:
                                start_freq = data_set[i][-19] + data_set[i][-18] + data_set[i][-17] + data_set[i][-16]
                                start_freq = int(start_freq)
                                if start_freq < 2400 and start_freq >= 2300:
                                    if data_set[i][-1] == 'v':
                                        hour = data_set[i][-33] + data_set[i][-32]
                                        minu = data_set[i][-30] + data_set[i][-29]
                                        sec  = data_set[i][-27] + data_set[i][-26]

                                        if int(hour) == 11 and int(minu) <= 30:    #only update 11 am
                                            data_csv = open(path + '/'+ uploadfolder + '/' + data_set[i],"r")
                                            for row in csv.DictReader(data_csv):
                                                center_freq = row['Frequency [MHz]']
                                                center_freq = center_freq[:7]
                                                if center_freq[-1] == '0':
                                                    center_freq = center_freq[:6]
                                                if center_freq in freq_uploaded:
                                                    record_time = uploadfolder + ' ' + hour + ':' + minu + ':' + sec
                                                    avg_power = row['Power Avg [dBm]']
                                                    avg_power = float(avg_power)
                                                    center_freq = int((float(center_freq) - 2302.5)/5 + 40) + 1   #convert to channel number
                                                    print "updating...."
                                                    write(center_freq, avg_power, record_time)

                            except:
                                pass
                        f = open(path + "processed_data.txt","a+")
                        f.write("%s\n"%(uploadfolder))
                        f.close()
                        print "finish updating " + str(uploadfolder)
    except:
        pass
