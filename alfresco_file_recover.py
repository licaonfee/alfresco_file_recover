import psycopg2
import psycopg2.extras
import os
import shutil

ALFRESCO_CONNSTR = "dbname=alfresco user=alfresco password=alfresco host=localhost"
ALFRESCO_PATH = "/opt/alfresco-community/alf_data/contentstore/"
RECOVERY_PATH = "/tmp/RECOVER"


conn = psycopg2.connect(ALFRESCO_CONNSTR)

def get_nodes(conn):
    q = """
    SELECT 
        alf_node.id, alf_node.audit_creator 
    FROM
        alf_node 
          INNER JOIN alf_qname
            ON alf_node.type_qname_id = alf_qname.id 
          INNER JOIN alf_namespace
            ON alf_qname.ns_id=alf_namespace.id
    WHERE 
        alf_qname.local_name='content' 
        AND alf_namespace.uri='http://www.alfresco.org/model/content/1.0' """
    cur = conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
    cur.execute(q)
    rows = cur.fetchall()
    cur.close()
    return rows

def get_file(conn, node_id):
    q = """
    SELECT 
       max(string_value) filename,max(long_value) url_id
    FROM 
        alf_node_properties
          INNER JOIN alf_qname
            ON alf_node_properties.qname_id = alf_qname.id
          INNER JOIN alf_namespace
            ON alf_qname.ns_id=alf_namespace.id
    WHERE 
        node_id=%s AND alf_qname.local_name IN('name','content') 
        AND alf_namespace.uri = 'http://www.alfresco.org/model/content/1.0'
    GROUP BY
        node_id
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
    cur.execute(q, (node_id,))
    rows = cur.fetchall()
    cur.close()
    return rows

def get_url(conn, url_id):
    q = """
    SELECT 
        content_url
    FROM 
        alf_content_url u
        INNER JOIN
        alf_content_data d 
        ON u.id=d.content_url_id
    WHERE
        d.id=%s
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
    cur.execute(q, (url_id,))
    rows = cur.fetchone()
    cur.close()
    return rows.content_url.replace("store://",ALFRESCO_PATH,1)


for row in get_nodes(conn):
    for f in get_file(conn, row.id):
        path = os.path.join(RECOVERY_PATH, row.audit_creator)
        url = get_url(conn, f.url_id)
        try:
            pass
            os.makedirs(path)
        except OSError,err:
            if err.errno!=17 :
                print(err)
                continue
        shutil.copy(url, os.path.join(path,f.filename))
        print(os.path.join(path,f.filename))
