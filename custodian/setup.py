from pyArango.connection import Connection, Database
from pyArango import theExceptions
from dotenv import load_dotenv
import json, yaml, os

conn = None
db: Database
load_dotenv(".env")

print(os.getenv("arango_url"))
print()

def _bootstrap(self):
    global db
    try:
        db.createCollection(name="policies")
    except theExceptions.CreationError:
        pass
    try:
        db.createCollection(name="c7x_scans")
    except theExceptions.CreationError:
        pass

def _connect():
    global conn, db
    if conn:
        conn = conn
        db = conn[os.getenv("arango_db")]
        return
    conn = Connection(
        arangoURL=os.getenv("arango_url"),
        username=os.getenv("arango_user"),
        password=os.getenv("arango_pwd"),
        max_retries=2)

    db = conn[os.getenv("arango_db")]
    _bootstrap()


_connect()

allowed = {
    "aws-config": True,
    "ec2": True,
    "eni": True,
    "kms": True,
    "route-table": True,
    "security-group": True,
    "vpc": True,
    "account": True,
    "cloudtrail": True,
    "efs": True,
    "iam": True,
    "rds": True,
    "s3": True,
    "sns": True,
}


for root, dirs, files in os.walk(os.getcwd(), topdown=True):
    for name in dirs:
        if not (allowed.get(name, False)):
            continue
        print(os.path.join(root, name))
        for r, dirs, files in os.walk(os.path.join(root, name), topdown=True):
            for name in files:
                with open(os.path.join(r, name)) as f:
                    d = yaml.safe_load(f)
                    for policy in d["policies"]:
                        if policy["mode"]["type"] != "periodic":
                            policy["mode"]["type"] = "pull"
                        policy["_key"] = policy["name"]
                        db.AQLQuery(
                            """
                            UPSERT { _key: @key } 
                            INSERT @doc 
                            UPDATE @doc IN policies OPTIONS {waitForSync: True}
                            """,
                            bindVars={
                                "key": policy["_key"],
                                "doc": policy,
                            })