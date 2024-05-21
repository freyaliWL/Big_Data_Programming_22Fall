from cassandra.cluster import Cluster
import sys,os,gzip
import re
from cassandra.query import BatchStatement
import uuid

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# def unique_random_id()

def seperate_lines(lines):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    temp = re.match(line_re, lines)
    #return temp
    if temp:
        pairs = line_re.split(lines)
        return (pairs[1], int(pairs[4]))
    else:
        return None

def main(inputs,table):
    
    bytes_count = 0

    session.execute('DROP TABLE IF EXISTS ' +table)
    session.execute('create table ' +table + '(host TEXT, id TEXT, bytes INT, PRIMARY KEY(host, id));')
    insert_query = session.prepare('insert into nasalogs (host, id, bytes) values (?,?,?);')

    batch_statement = BatchStatement()

    for f in os.listdir(inputs):
        with gzip.open(os.path.join(inputs, f), 'rt', encoding='utf-8') as logfile:
            for line in logfile:
                fields = seperate_lines(line)
                #print(fields)
                if fields:
                    batch_statement.add(insert_query,(fields[0], str(uuid.uuid4()), fields[1]))
               
                    bytes_count = bytes_count+1
                    #set batch size to 150
                    if(bytes_count > 150):
                        #batch_count = batch_count+1
                        session.execute(batch_statement)
                        batch_statement.clear()
                        bytes_count = 0

if __name__ == '__main__':
    cluster = Cluster(['node1.local', 'node2.local'])
    #rows = session.execute('SELECT path, bytes FROM nasalogs WHERE host=%s', [somehost])

    inputs = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]
    session = cluster.connect(keyspace)

    main(inputs, table)