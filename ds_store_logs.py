from datetime import datetime, timedelta
import re
import os
import shutil


districts = {
    "chg" : "Чернігівська область",
    "chk" : "Черкаська область",
    "chn" : "Чернівецька область",
    "dne" : "Дніпровська область",
    "ifr" : "Івано-Франківська область",
    "kha" : "Харківська область",
    "khm" : "Хмельницька область",
    "kir" : "Кіровоградська область",
    "kre" : "Полтавська область",
    "krr" : "Дніпровська область",
    "lvv" : "Львівська область",
    "nik" : "Миколаївська область",
    "pol" : "Полтавська область",
    "rov" : "Рівненська область",
    "sum" : "Сумська область",
    "ter" : "Тернопільська область",
    "vin" : "Вінницька область",
    "vol" : "Волинська область",
    "zak" : "Закарпатська область",
    "zap" : "Дніпровська область",
    "zyt" : "Житомирська область",
}
__log_file_pattern = r'\d*{date}_(:?\d+?_)?\w+\.log'
__date_format = "%y%m%d"
__date_long_format = "%d.%m.%Y"
__log_name_format = '{date}_upgrade_{node}.log'
__node_header_pattern = re.compile(r'\b[a-z]{2}\d+?-[a-z]*?[a-z]{3}\d+?\b', re.IGNORECASE)
__node_district_pattern = re.compile(r'\b[a-z]{2}\d+?-[a-z]*?([a-z]{3})\d+?\b', re.IGNORECASE)
__export_file_header = "Початок;Закінчення;Область;NE Type;NE;ID;Result;Підстава;Відповідальний;Інші учасники;Logs;Коментар;Change Data;User"
__export_file_line_pattern = "{start_date} 23:00;{today_long} 4:00;{district};DS;{node};7.0.R13;Ok;Рекомендація Alcatel;Малько Павло Миколайович;;{log_file_path};;;"
__export_file_name_pattern = "{date}_DS_Upgrade.txt"
__log_path = "\\\\10.44.1.140\\om\\Logs\DS\\{node}\\2017\\{date}_upgrade_{node}"


def get_log_file_store_path(node_name):
    """
    
    :param node_name: name of nodes
    :type node_name: str
    :return: path of log files on server 
    :rtype: str
    """
    return __log_path.format(node=node_name, datetime=today)


def get_switchlog_line(file_name):
    """
    
    :param file_name: generate string for import into Switchlog
    :type file_name: str
    :return: generated line without EOL 
    :rtype: str
    """
    try:
        with open(file_name, 'r') as file:
            file_content = file.read()
            file.close()
            try:
                node = __node_header_pattern.findall(file_content)[0]
                district_str = __node_district_pattern.findall(file_content)[0]
                if district_str in districts:
                    district = districts[district_str]
                else:
                    district = "Всі міста"

                return __export_file_line_pattern.format(start_date = yesterday_long,
                                                         today_long = today_long,
                                                         district=district,
                                                         node=node,
                                                         log_file_path=get_log_file_store_path(node))
            except Exception as e:
                print("Error determinate node name/district for file [{0}]".format(file_name))
                print(str(e))
                return ""
    except OSError as e:
        print("Can't read file [{0}]".format(file_name))
        print(str(e))
        return ""


def rename_log_file(log_file_name):
    """
    
    :param log_file_name:  rename file with log according content (node), if file with correct name alredy exist, than
            append source file to already exist file
    :type log_file_name: str
    :return: new log file name
    :rtype: str
    """
    try:
        with open(log_file_name, "r") as log_file:
            log_file_content = log_file.read()
            if len(set(__node_header_pattern.findall(log_file_content))) == 1:
                node_name = __node_header_pattern.findall(log_file_content).pop().lower()
                target_log_file_name = __log_name_format.format(date=today, node=node_name)
                if not os.access(target_log_file_name, os.F_OK):
                    try:
                        os.rename(log_file_name, target_log_file_name)
                    except Exception as e:
                        print("Error rename file [{0}] --> [{1}]".format(log_file_name, target_log_file_name))
                        print(str(e))
                    else:
                        return target_log_file_name
                else:
                    try:
                        with open(target_log_file_name, "a+") as target_log_file:
                            target_log_file.write("\n"+log_file_content)
                            target_log_file.close()
                            return target_log_file
                    except Exception as e:
                        print("Error while appending data from [{0}] to [{1}]".format(log_file_name, target_log_file_name))
                        print(str(e))
            else:
                print("")
    except OSError as e:
        print("Can't open file [{0}]".format(log_file_name))
        print(str(e))


def transfer_file(source_file_name, remote_path):
    """
    
    :param source_file_name:  copied file name
    :type source_file_name: str
    :param remote_path: path on destination server
    :type remote_path: str
    :return: True if file copied
    :rtype: bool
    """
    try:
        os.mkdir(remote_path)
        shutil.copy(source_file_name, remote_path)
        return True
    except Exception as e:
        print("Error copy file [{0}] -->> [{1}]".format(source_file_name, remote_path))
        print(str(e))
        return False


if __name__ == "__main__":
    today = datetime.today().strftime(__date_format)
    yesterday_long = datetime.strftime(datetime.now() - timedelta(days=1), __date_long_format)
    today_long = datetime.today().strftime(__date_long_format)
    today_log_file_pattern = __log_file_pattern.format(date=today)

    log_files = os.listdir()

    file_transfer_queue = {}
    for log_file_name in sorted(log_files):
        if re.search(today_log_file_pattern, log_file_name, re.I):
            file_transfer_queue[rename_log_file(log_file_name)] = "yes"

    export_file_lines = ""
    for transfered_file_name in file_transfer_queue:
        if transfer_file(transfered_file_name, get_log_file_store_path(transfered_file_name)):
            export_file_lines += get_switchlog_line(transfered_file_name) + "\n"

    if export_file_lines:
        try:
            with open(__export_file_name_pattern.format(date=today), "w") as export_file:
                export_file.write(__export_file_header + "\n" + export_file_lines)
                export_file.close()
        except Exception as e:
            print("Cannot write Export file [{0}]".format(__export_file_name_pattern.format(date=today)))
            print(str(e))
