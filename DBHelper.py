import os.path
import pyodbc
import json
import re
import datetime
import sys
import getopt

QUICK_SAVE_PATH = "C:\\temp\\quicksave.bak"
HELP_MSG='''
        Format: dbhelper.py -v <DBVersion> -c <Config_File_Path> -u
                -v(Required in command mode) specify the database version in Zero Repo
                -c specify the config json file path 
                -u run the upgrade sql script specified in config.json after restore DB backup
        * If no arguments specified, it will enter the interactive mode'''
def quick_save(config_options):
    '''Save the DB specified in config to temp folder'''    
    conn  = pyodbc.connect(config_options['CONNECTION_STRING'],autocommit=True);
    cursor = conn.cursor()
    print 'Backing up DB  to quick save path at {0}...'.format(QUICK_SAVE_PATH)
    sql = backup_db_to_file(cursor,config_options['DATABASE_NAME'],QUICK_SAVE_PATH)    
    cursor.close();

def quick_restore(config_options):
    '''Restore the DB specified from temp folder'''
    conn  = pyodbc.connect(config_options['CONNECTION_STRING'],autocommit=True);    
    cursor = conn.cursor()
    restore_db(cursor,config_options['DATABASE_NAME'],QUICK_SAVE_PATH )
    cursor.close() 

def restore_db(cursor,dbName,backup_full_path):
    '''Restore the DBName database from backup_full_path backup file'''
    
    sql = get_restore_sql(dbName, backup_full_path)
    print "Restore {0} from {1} ".format(dbName,backup_full_path)
    cursor.execute(sql)
    while cursor.nextset():
        pass

def get_restore_sql(dbName, backupFilePath):
    '''Generate the sql to Restore(WITH REPLACE) the specified {dbName} with the backup file at {backupFilePath}'''
    #Existence check for the backupFilePath
    if (not os.path.exists(backupFilePath)) or (not os.path.isfile(backupFilePath)):
        raise OSError("File not found at {0} or it's not a valid file path.".format(backupFilePath));
    sql = '''
        /*
                Author: Eric Dong
                Description: Take DB off line and recover it with another DB copy	
        */
        USE master;
        IF EXISTS(SELECT * FROM master.dbo.sysdatabases WHERE name = '{DATABASE_NAME}')
        BEGIN
	        ALTER DATABASE {DATABASE_NAME} SET OFFLINE WITH ROLLBACK IMMEDIATE;
        END
        
        RESTORE DATABASE {DATABASE_NAME} 
        FROM DISK ='{BACKUP_PATH}'
        WITH REPLACE,RECOVERY ; '''    
    sql = sql.format(DATABASE_NAME=dbName,BACKUP_PATH=backupFilePath);    
    return sql
                       
def get_login_replace_sql(dbName, userName):
    ''' Generate the sql to re-map userName at dbName database '''
    sql = '''
            /*
                    Author: Eric Dong
                    Description: Drop previous reports user and remapping the "reports" user
                    Note: Please make sure there is a "reports" login exists otherwise it will rise error.
            */
            USE {DATABASE_NAME}
            DROP user {USER_NAME}
            CREATE USER {USER_NAME}
            FROM LOGIN {USER_NAME}
            WITH DEFAULT_SCHEMA = dbo  '''
    sql = sql.format(DATABASE_NAME=dbName,USER_NAME=userName);    
    return sql

def backup_db_to_file(db_cursor,dbName,full_backup_path):
    '''Backup a db to the path specified as a file'''
    sql = get_backup_sql(dbName,full_backup_path)
    print "Backing up {0} to {1}...".format(dbName,full_backup_path)
    db_cursor.execute(sql)
    while db_cursor.nextset():
        pass

def backup_db_to_folder(db_cursor,dbName, backupFileDirectory):
    '''Backup a db to the directory specified with time stamp prefix'''
    if not (os.path.exists(backupFileDirectory) and os.path.isdir(backupFileDirectory)):
        raise ValueError("The backup file path:{0} is not valid".format(backupFileDirectory))
    ts = datetime.datetime.now()
    time_stamp = 'M{0}-D{1}-{2:0<2}{3:0<2}{4:0<2}'.format(ts.month,ts.day,ts.hour,ts.minute,ts.second)
    full_backup_path = os.path.join(backupFileDirectory, dbName + time_stamp + '.bak')
    backup_db_to_file(db_cursor,dbName,full_backup_path)

def get_backup_sql(dbName, backupFilePath):
    ''' Generate the sql to backup a {dbName} to {backupFilePath}'''
    sql = '''
            /*
                    Author: Eric Dong
                    Description: Fully backup a database
                    Notes: Please make sure the user you use have access to the disk path you specified.
            */
            USE master            
            BACKUP DATABASE {DATABASE_NAME}
                    TO DISK = '{BACKUP_PATH}'
                    WITH FORMAT ,
                    MEDIANAME = 'FULL BACKUP BY DB FACTORY',
                    NAME = 'FULL BK OF {DATABASE_NAME}';            '''
    sql = sql.format(DATABASE_NAME=dbName,BACKUP_PATH = backupFilePath);    
    return sql

def read_config_file(configFilePath = 'config.json'):
    '''Read the required config options from the json config file'''    
    with open(configFilePath) as json_data:
        d= json.load(json_data)
        json_data.close()
    if validateConfigOptions(d):
        return d
    else:
        raise ValueError("Invalid config file")

def validateConfigOptions(options):
    '''Validate to make sure all mandatory options exists'''
    requiredList = 'DATABASE_NAME','ZERO_DB_BACKUP_DIRECTORY','NEW_BACKUP_DIRECTORY','CONNECTION_STRING'
    errMsgs = []
    for req in requiredList:
        if not options.has_key(req):
            errMsgs.append('Missing required key {0} in config file.'.format(req))
    if len(errMsgs) > 0:
        print errMsgs
        return False
    else:
        return True

def split_go_to_batch(script):
    '''For SQL clients like pyodbc, GO statement is not recognized so we need to manually split scripts into batches'''
    goRegex = r'[\n|;]?\s*?GO\s*?[\n|;]'
    scripts = re.split(goRegex,script)
    return scripts

def get_available_zero_db(zeropath):
    '''Check the available zero dbs and return an array of filename and version number. Result sort by version descending'''
    
    if(not os.path.exists(zeropath) and os.path.isdir(zeropath)):
        raise IOError('Path {0} does not exist or is not a valid directory'.format(zeropath))
    db_files = filter(lambda name: name.index('.bak')>0 , os.listdir(zeropath))
    db_versions = map(lambda name:name[:name.index('.bak')],db_files)
    db_versions .sort()    
    db_versions .reverse()        
    def _tmp_to_db_info(vname):
        return {
                'Version':vname,
                'Path':os.path.join(zeropath,vname + '.bak')
            }

    db_list = map(_tmp_to_db_info,db_versions)
    return db_list
   
def get_specified_zero_db(db_ver,zeropath):
    '''Get available zero dbs from the zero db repo, then get the version and path info based on specification'''    
    db_list = get_available_zero_db(zeropath)
    if(db_list.count ==0):
        raise ValueError(r'There is no DB backup files in Zero DB Repo')
    if(db_ver.lower()!="latest"):
        for db_info in db_list:
            if(db_info['Version'] == db_ver):
                return db_info
    else:
        #the latest version is the first element
        return db_list[0]

def enter_interactive_mode():
    '''The interactive mode has a menu to let user select operations to be done'''        
    config_options = read_config_file()    
    output_config_options(config_options)
    while(True):
        print '''------------------DB HELPER----------------------
    0. Test Connection
    1. Reload the Config File
    2. Quick Save DB
    3. Quick Restore DB
    4. Check Zero DB Repo
    5. Get DB of a version(Restore from Zero Repo)            
    6. Get Latest DB (Restore from Zero Repo then run latest upgrade.sql)
    7. Help for command line mode
    8. Exit'''
        option = raw_input("Please select:")    
        if(len(option) == 1 and option in ['1','2','3','4','5','6','7','8','0']):
            if option == '0':
                conn  = pyodbc.connect(config_options['CONNECTION_STRING'],autocommit=True);
                cursor = conn.cursor()
                cursor.execute(r"SELECT GETDATE()")
                cursor.close()
                print "Connection test successful!"
            if(option == '1'):
                config_options = read_config_file();
                output_config_options(config_options)
            if(option == '2'):
                #TODO:Add empty check
                quick_save(config_options)
            if(option == '3'):
                #TODO:ADD emtpy check
                quick_restore(config_options)
            if(option == '4'):
                #TODO:ADD emtpy check
                avai_dbs = get_available_zero_db(config_options['ZERO_DB_BACKUP_DIRECTORY'])
                if( len(avai_dbs) !=0):
                    print "-----------------AVAILABLE DB BACKUPS--------------------------------"
                    print "{0:^10s} {1:<30s}".format("Version","Path")
                    for db in avai_dbs:
                        print "{0:<10s} {1:<30s}".format(db["Version"],db["Path"])
            if(option == '5'):
                db_ver = raw_input("Please enter the version you would like to update:")
                update_specified(db_ver,config_options)
            if(option == '6'):
                update_latest(config_options)
            if(option =='7'):
                print HELP_MSG
            if(option == '8'):
                sys.exit()
def update_specified(db_ver,config_options):    
    db_info = get_specified_zero_db(db_ver,config_options['ZERO_DB_BACKUP_DIRECTORY'])
    if db_info is None:
        print "Could not find the Version:{0} in Zero Repo, please verify the input.".format(db_ver)
        return
    print db_info
    conn  = pyodbc.connect(config_options['CONNECTION_STRING'],autocommit=True);
    cursor = conn.cursor()
    if (config_options.has_key('CREATE_BACKUP_BEFORE_UPGRADE') and config_options['CREATE_BACKUP_BEFORE_UPGRADE'].upper() == 'TRUE'):
        backup_db_to_folder(cursor,config_options['DATABASE_NAME'],config_options['NEW_BACKUP_DIRECTORY'])
    restore_db(cursor,config_options['DATABASE_NAME'],db_info['Path'])

    sql = get_login_replace_sql(config_options['DATABASE_NAME'],'reports');
    cursor.execute(sql)

    print "Done!"

def run_update_script(cursor,config_options):
    if (config_options.has_key('UPGRADE_SCRIPT_PATH')):        
        execute_sql_file(cursor,config_options['UPGRADE_SCRIPT_PATH'])
        print 'Upgraded DB to latest with {0}'.format(config_options['UPGRADE_SCRIPT_PATH'])    
    else:
        print 'No UPGRADE_SCRIPT_PATH specified in config.json. Skipped script update.'

def update_latest(config_options):
    db_info = get_specified_zero_db('latest',config_options['ZERO_DB_BACKUP_DIRECTORY'])
    if db_info is None:
        print "Could not find any backups in Zero Repo.Please very the config or contact admin".format(db_ver)
        return
    print db_info
    conn  = pyodbc.connect(config_options['CONNECTION_STRING'],autocommit=True);
    cursor = conn.cursor()
    if (config_options.has_key('CREATE_BACKUP_BEFORE_UPGRADE') and config_options['CREATE_BACKUP_BEFORE_UPGRADE'].upper() == 'TRUE'):
        backup_db_to_folder(cursor,config_options['DATABASE_NAME'],config_options['NEW_BACKUP_DIRECTORY'])
    restore_db(cursor,config_options['DATABASE_NAME'],db_info['Path'])

    sql = get_login_replace_sql(config_options['DATABASE_NAME'],'reports');
    cursor.execute(sql)

    run_update_script(cursor,config_options)

    cursor.close();                
    print "Done!"

def output_config_options(config_options):
    print "----------------CURRENT CONFIG.JSON CONTENTS------------------------"
    for key in config_options.keys():
        print "  {0:<30s}:{1:<100s}".format(key,config_options[key])
    print "-----------------END OF CONFIG.JSON CONTENTS------------------------\n"

def execute_sql_file(cursor,filepath):
    '''Execute a script file at filepath'''
    with open (filepath, "r") as sqlfile:
        script = sqlfile.read()                
    scripts = split_go_to_batch(script)
    for index,batch in enumerate(scripts):
        #print 'Executing the NO.{0} batch of update script'.format(index)
        if((not batch.strip())or (batch.strip().upper()=="GO")):
          cursor.execute(batch)

def __main__(argv):    
    if len(argv) == 0:
        #No additional parameters appended, use menu mode
        enter_interactive_mode()
    else:
        try:
            opts,args = getopt.getopt(argv,"v:c:u")
        except getopt.GetoptError:
            print HELP_MSG
            sys.exit(2)
        run_upgrade = False
        config_path = "config.json"
        db_ver = None
        print opts
        for opt,arg in opts:
            if opt=='-v':
                db_ver = arg
            elif opt == '-c':
                config_path = arg
            elif opt=='-u':
                run_upgrade = True                                                        
        if db_ver is None:
            raise getopt.GetoptError("-version must be specified as database version to be restored")

        config_options = read_config_file(config_path);
        output_config_options(config_options)
        update_specified(db_ver, config_options)       
        if run_upgrade:
            cursor = pyodbc.connect(config_options['CONNECTION_STRING'],autocommit=True).cursor()
            run_update_script(cursor,config_options)
            cursor.close()
        return           

if __name__ == '__main__':
    __main__(sys.argv[1:])
