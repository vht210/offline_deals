import subprocess
import os
import subprocess
import sys, getopt
from collections import OrderedDict
# SRC_FILE = "/home/tung/test_deals/src"
# DST_FILE = "/homt/tung/test_deal/dst"


# https://docs.filecoin.io/store/lotus/very-large-files/#maximizing-storage-per-sector

# lotus client commP <inputCarFilePath> <minerAddress>

# lotus client generate-car <inputPath> <outputPath>

# lotus client deal --manual-piece-cid=CID --manual-piece-size=datasize <Data CID> <miner> <price> <duration>

# lotus-miner deals import-data <dealCid> <filePath>

import traceback
from pathlib import Path
import time

try:
    from .config import *
except:
    from config import *
import datetime
import logging
import shlex
import math
import csv
from datetime import datetime, timedelta

START_EPOCH = 1598306400
# START_EPOCH=1598281200








class Helpers(object):
    @staticmethod
    def run_cmd(cmd):
        process = subprocess.Popen(shlex.split(cmd),
                                   stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        # logging.info("stdout: " + str(stdout))
        result = None
        err = None
        if len(str(stdout).replace("b''", "")) > 0:
            result = str(stdout).split(",")

        if len(str(stderr).replace("b''", "")) > 0:
            logging.error("Error: " + str(stderr))
        return result, err

    @staticmethod
    def epoch_to_utc(epoch_number):
        if epoch_number > 0:
            unix_timestamp = START_EPOCH + epoch_number * 30
            return datetime.utcfromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M')
        else:
            return -1

    @staticmethod
    def get_epoch_from_date(offset_date=5):
        start_date = datetime.utcnow() + timedelta(days=offset_date)
        start_date_ts = datetime.timestamp(start_date)
        epoch =(int)( (start_date_ts - START_EPOCH)/30)
        print("Epoch " + str(epoch))
        return epoch

class Deals(object):
    def __init__(self, piece_cid=None, piece_size=None, data_cid=None, miner=None, price=None, price_mul=None,
                 wallet=None, duration=1100000):
        self.piece_cid = piece_cid
        self.piece_size = piece_size
        self.price_mul = price_mul
        self.data_cid = data_cid
        self.miner = miner
        self.price = price
        self.duration = duration
        self.wallet = wallet

    def set_miner(self, miner):
        self.miner = miner

    def set_price(self, price):
        self.price = price

    def set_piece(self, piece_cid, piece_size):
        self.piece_cid = piece_cid
        self.piece_size = piece_size

    def set_data_cid(self, data_cid):
        self.data_cid = data_cid

    def set_wallet(self, wallet):
        self.wallet = wallet


class OfflineDeals(object):
    def __init__(self, folder_path, miner=None, price=None, wallet=None,csv_data=None,duration=1070000):
        self.deals = OrderedDict()
        self.miner = miner
        self.price = price
        self.folder_path = folder_path
        self.wallet = wallet
        self.csv_data = csv_data
        self.duration = duration

    def create_data_cid(self):
        files = self.get_file_from_folder(self.folder_path)
        folder_name = self.folder_path.split("/")[-1]
        if self.folder_path[-1] == '/':  #/home/user/test/
            folder_name = self.folder_path("/")[-2]
        is_create_deal = False
        csv_file =None
        try:
            #if self.miner and self.price and self.wallet and not self.csv_data:
             #   is_create_deal = True
           # else:
            #csv_filename = 'carfile_' + folder_name + ".csv"
            csv_filename = self.csv_data
            csv_file = open(csv_filename, mode='w')
            fields_name = ['file_name', 'data_cid','piece_cid','piece_size','price_mul'] #price_mul: 4gb -> 4*costpergb
            csv_writer = csv.DictWriter(csv_file, fieldnames=fields_name)
            csv_writer.writeheader()



            for file in files:

                file_car = file + ".car"
                if os.path.exists(file_car):
                    print(file_car + " exists, skip")
                    continue
                # logging.info("Create data CID for " + str(file))
                filename = file.split("/")[-1]
                # lotus client import
                cmd = "lotus client import " + file
                logging.info(cmd)
                result, err = Helpers.run_cmd(cmd)
                import_id = result[0].replace("b'Import ", "")
                # logging.info("Import id  " + str(import_id))
                data_cid = str(result[-1]).replace("Root ", "").replace("\\n'", "")
                # logging.info("Data cid " + str(data_cid))
                car_file = self.generate_car(file)
                piece_cid, piece_size, price_mul = self.generate_comm(car_file)
                if False:
                    deal = Deals(piece_cid=piece_cid, piece_size=piece_size, data_cid=data_cid, price=self.price,
                                 price_mul=price_mul, miner=self.miner, wallet=self.wallet)
                    self.deals[filename] = deal
                else:
                    #Only create car file and add result to excel
                    tmp_file_name = filename
                    if tmp_file_name.find(".car") ==-1:
                        tmp_file_name = filename + ".car"
                    csv_writer.writerow({"file_name":tmp_file_name,"data_cid":data_cid,"piece_cid":piece_cid,
                                         "piece_size":piece_size,"price_mul":price_mul})
        except:
            logging.info("Exception at  " + str(traceback.print_exc()))
        finally:
            if csv_file:
                csv_file.close()


    def generate_car(self, file):
        cmd = "lotus client generate-car " + file + " " + file + ".car"
        logging.info(cmd)
        result, err = Helpers.run_cmd(cmd)
        # logging.info(str(result))
        return file + ".car"

    def generate_comm(self, car_file):
        # lotus client commP <inputCarFilePath> <minerAddress>
        cmd = "lotus client commP " + car_file
        logging.info(cmd)
        results, err = Helpers.run_cmd(cmd)

        for a_result in results:
            line_rs = a_result.strip().split(os.linesep)
            for a_line in line_rs:
                a_line = a_line.replace("b\'", "").replace("\'", "").strip()
                a_line_arr = a_line.split("\\n")
                for line in a_line_arr:
                    if line.find("CID") > -1:
                        # logging.info("Cid -> " + line)
                        cid = line.split(":")[-1].strip()
                    if line.find("Piece size:") > -1:
                        piece_size_and_type = line.split(":")[-1].strip()
                        piece_size = piece_size_and_type.split(" ")[0]
                        piece_type = piece_size_and_type.split(" ")[-1]
                        ps, price_mul = self.cal_size(piece_size, piece_type)
        return cid, ps, price_mul

    def propose_deals(self):
        # lotus client deal --manual-piece-cid=CID --manual-piece-size=datasize <Data CID> <miner> <price> <duration>
        if self.miner:
            ts = datetime.today().strftime('%Y%m%d%H%M')
            csv_filename = 'deal_' + str(self.miner) + "_" + str(ts)  + ".csv"
            with open(csv_filename, mode='w') as csv_file:
                fields_name = ['file_name', 'deal_cid']
                csv_writer = csv.DictWriter(csv_file, fieldnames=fields_name)
                csv_writer.writeheader()
                if self.csv_data:
                    #file_name,data_cid,piece_cid,piece_size,price_mul
                    with open(self.csv_data,mode='r') as csv_read:
                        csv_reader = csv.reader(csv_read, delimiter=',')
                        line_count = 0
                        for row in csv_reader:
                            if line_count == 0:
                                print(", ".join(row))
                                line_count += 1
                            else:
                                line_count += 1
                                print(" ### Import number " + str(line_count))
                                try:
                                    file_with_path = os.path.join(self.folder_path, row[0])
                                    total_price = "{:.12f}".format(float(self.price) * float(row[4]))
                                    epoch = Helpers.get_epoch_from_date()
                                    cmd = "lotus client deal --from {} --start-epoch {} --fast-retrieval  --manual-piece-cid {} --manual-piece-size {} {} {} {} {} ". \
                                        format(self.wallet, str(epoch),row[2], row[3], row[1], self.miner, total_price,
                                               self.duration)
                                    logging.info(cmd)
                                    rs, err = Helpers.run_cmd(cmd)
                                    deal_cid = rs[0].replace("b\'", "").replace("\\n", "").replace("'", "")
                                    logging.info("deal_cid=" + deal_cid)
                                    file_name = row[0]
                                    if str(file_name).find(".car") == -1:
                                        file_name = row[0] + ".car"

                                    csv_writer.writerow({"file_name": file_name.replace(".car.car",".car"), "deal_cid": deal_cid})
                                except:
                                    print("error in create deals " + str(traceback.print_exc()))
                else:
                    print("Not found csv data file")
                    # for k, v in self.deals.items():
                    #     total_price = "{:.12f}".format(float(v.price) * v.price_mul)
                    #     # logging.info("total price " + str(total_price) )
                    #     cmd = "lotus client deal --from {} --manual-piece-cid {} --manual-piece-size {} {} {} {} {}". \
                    #         format(v.wallet, v.piece_cid, v.piece_size, v.data_cid, v.miner, total_price, v.duration)
                    #     # logging.info("File: " + str(k))
                    #     logging.info(cmd)
                    #     rs, err = Helpers.run_cmd(cmd)
                    #     deal_cid = rs[0].replace("b\'", "").replace("\\n", "").replace("'", "")
                    #     logging.info("deal_cid=" + deal_cid)
                    #     csv_writer.writerow({"file_name": k + ".car", "deal_cid": deal_cid})

    def cal_size(self, piece_size, piece_unit):
        logging.info("Piece size and type " + str(piece_size) + " " + str(piece_unit))
        piece_price_mul = 1
        if piece_unit.find("MiB") > -1:
            val = int(float(str(piece_size).strip()))
        elif piece_unit == "GiB":
            piece_price_mul = math.ceil(float(str(piece_size).strip()))
            val = int(float(str(piece_size).strip()) * 1024)
            if int(float(piece_size)) > 16:
                piece_price_mul = 32
            elif int(float(piece_size)) > 8:
                piece_price_mul = 16
            elif int(float(piece_size)) > 4:
                piece_price_mul = 8
            elif int(float(piece_size)) > 2:
                piece_price_mul = 4
            elif int(float(piece_size)) > 1:
                piece_price_mul = 2
        piece_size_in_bytes = self.calculate_piece_size(val)
        logging.info("piece size in byte " + str(piece_size_in_bytes))
        return piece_size_in_bytes, piece_price_mul

    def calculate_piece_size(self, data_size_in_Mb):
        temp_in_bytes = 1024 * 1024 * data_size_in_Mb
        temp_2 = math.ceil(math.log2(math.ceil(temp_in_bytes / 127)))
        val_in_bytes = 127 * pow(2, temp_2)
        return val_in_bytes

    def get_file_from_folder(self, folder, filter_type=None):
        files = []
        if os.path.exists(folder):
            pathlist = Path(folder).glob('**/*.*')
            if filter_type:
                pathlist = Path(folder).glob(filter_type)
            # logging.info("from " + str(pathlist))
            for path in pathlist:
                path_in_str = str(path)

                logging.info(path_in_str)
                files.append(path_in_str)
        else:
            logging.info("Path does not exist")
        return files


def main(argv):
    miner = None
    price = None
    folder_path = None
    wallet = None
    csv_data = None
    propose_deals=False
    try:
        opts, args = getopt.getopt(argv, "h:m:p:f:w:d:propose-deals", ["help=", "miner=", "price=", "folder=", "wallet=","csv_data=","propose_deals"])
    except getopt.GetoptError:
        print('gen_deals.py [-m <miner_id> -p <price_per_GiB> -f <folder_path> -w <wallet_addr> -o <csv_data>]')
        sys.exit(2)
    for opt, arg in opts:
        if opt in ["-h", "--help"]:
            print('gen_deals.py [-m <miner_id> -p <price_per_GiB> -f <folder_path> -w <wallet_addr> -o <csv_data>]')
            sys.exit()
        elif opt in ["-m", "--miner"]:
            miner = arg
        elif opt in ["-p", "--price"]:
            price = arg
        elif opt in ["-f", "--folder"]:
            folder_path = arg
        elif opt in ["-w", "--wallet"]:
            wallet = arg
        elif opt in ["-d","--csv_data"]:
            csv_data = arg
        elif opt in ["-propose-deals","--propose_deals"]:
            propose_deals = True
    off_deals = OfflineDeals(folder_path, miner, price, wallet,csv_data)
    if not propose_deals:
        off_deals.create_data_cid()
    if propose_deals and miner:
        off_deals.propose_deals()


if __name__ == '__main__':
    Helpers.get_epoch_from_date()
    if (len(sys.argv)) == 1:
        print('gen_deals.py -m <miner_id> -p <price_per_GiB> -f <folder_path> -w <wallet_addr>')
        sys.exit()
    main(sys.argv[1:])

