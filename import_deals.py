import os
import subprocess
import sys,getopt
import traceback
import  logging
import shlex
import csv
import time
from datetime import  datetime
class Helpers(object):
    @staticmethod
    def run_cmd(cmd):
        process = subprocess.Popen(shlex.split(cmd),
                                   stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        #logging.info("stdout: " + str(stdout))
        result = None
        err = None
        if len(str(stdout).replace("b''", "")) > 0:
            result = str(stdout).split(",")
        if len(str(stderr).replace("b''", "")) > 0:
            logging.error("Error: " + str(stderr))
        return result, err

    @staticmethod
    def set_log(log_file='import_deals.log'):
        LOGGING_LEVEL = logging.INFO
        format = "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"
        logFormatter = logging.Formatter(format)
        logging.basicConfig(level=LOGGING_LEVEL, format=format,
                            filename=log_file, filemode='w')
        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(logFormatter)
        logging.getLogger().addHandler(consoleHandler)

class ImportDeals(object):
    def __init__(self,csv_file,folder_path,delay):
        self.csv_file =csv_file
        self.folder_path = folder_path
        self.delay=delay

    def run_import(self):
        if os.path.exists(self.csv_file):
            with open(self.csv_file) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                line_count=0
                for row in csv_reader:
                    if line_count ==0:
                        print(", ".join(row))
                        line_count+=1
                    else:
                        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        print(now + " ### Import number " + str(line_count))

                        try:

                            file_with_path = os.path.join(self.folder_path,row[0])
                            deal_cid = row[1]
                            cmd = "lotus-miner storage-deals import-data " + deal_cid + " " + file_with_path
                            print(cmd)
                            Helpers.run_cmd(cmd)
                            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            print(now + " import " + str(line_count) + " done! :)")
                        except:
                            print("err in importing " + str(traceback.print_exc()))
                        line_count += 1
                        if self.delay>0:
                            try:
                                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                print(now + ": Sleep for " + str(self.delay) + " minutes until next import ")
                                time.sleep(int(self.delay)*60)
                            except:
                                print("Unable to push in delay")

        else:
            print(self.csv_file + " does not exist.")



def main(argv):
    csv_file = ""
    folder_path=""
    wallet = ""
    delay=0
    try:
        opts, args = getopt.getopt(argv, "h:i:f:d:", ["help=","csv_file=", "folder=", "delay="])
    except getopt.GetoptError:
        print('python import_deals.py -i <full_path_to_csv_file> -f <full_path_to_folder_path> -d <delay_between_import>')
        sys.exit(2)
    for opt, arg in opts:
        if opt in ["-h","--help"]:
            print('python import_deals.py -i <csv_file> -f <folder_path>')
            sys.exit()
        elif opt in ["-i","--csv_file"]:
            csv_file = arg

        elif opt in ["-f","--folder"]:
            folder_path = arg
        elif opt in ["-d", "--delay"]:
            try:
                delay =  int(arg)
                print("Delay between is  " + str(delay) + " minutes" )
            except:
                delay=0
                print("Unable to parse delay, use default 0")


    im_deal = ImportDeals(csv_file=csv_file,folder_path=folder_path,delay=delay)
    im_deal.run_import()


if __name__ == '__main__':
    if (len(sys.argv))==1:
        print('python import_deals.py -i <full_path_to_csv_file> -f <full_path_to_CAR_folder_path> -d <delay_in_minutes_between_import>')
        sys.exit()
    Helpers.set_log("import_deals.log")
    main(sys.argv[1:])

