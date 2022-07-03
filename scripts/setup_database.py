import mysql.connector
import os
from dotenv import load_dotenv
load_dotenv()

host = os.getenv("DB_HOST")
user = os.getenv("DB_USER")
coder = os.getenv("DB_CODER")
coder_pass = os.getenv("DB_CODER_PASSWORD")
webuser = os.getenv("DB_WEBUSER")
webuser_pass = os.getenv("DB_WEBUSER_PASSWORD")

# Connect to mysql
mysql_instance = mysql.connector.connect(
    host=host,
    user=user,
)

mycursor = mysql_instance.cursor()

# Create database
# mycursor.execute("CREATE DATABASE ncaa_play_by_play")
mycursor.execute("USE ncaa_play_by_play")
# Create users
mycursor.execute("CREATE USER %s@%s IDENTIFIED BY %s" , (coder, host, coder_pass))
mycursor.execute("CREATE USER %s@%s IDENTIFIED BY %s" , (webuser, host, webuser_pass))

# Grant privileges
mycursor.execute("GRANT ALL PRIVILEGES ON ncaa_play_by_play.* TO %s@%s" , (coder, host))
mycursor.execute("GRANT SELECT ON ncaa_play_by_play.* TO %s@%s" , (webuser, host))
mycursor.execute("FLUSH PRIVILEGES;")