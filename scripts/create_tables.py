import mysql.connector
import os
from dotenv import load_dotenv
load_dotenv()

host = os.getenv("DB_HOST")
coder = os.getenv("DB_CODER")
coder_pass = os.getenv("DB_CODER_PASSWORD")

# Connect to mysql database
play_by_play = mysql.connector.connect(
    host=host,
    user=coder,
    password=coder_pass,
    database="ncaa_play_by_play"
)
mycursor = play_by_play.cursor()

def remove_existing_table(table_name):
    '''Takes in a table name; checks if the table exists, removes it if it does'''
    mycursor.execute("SET foreign_key_checks = 0")
    drop_table = "DROP TABLE IF EXISTS {}".format(table_name)
    mycursor.execute(drop_table)
    mycursor.execute("SET foreign_key_checks = 1")
    return drop_table

def create_table(table_name, columns):
    '''Takes in a table name and a list of columns; creates the table'''
    remove_existing_table(table_name)
    create_table = """CREATE TABLE `{}` (
    {}
    )
    """.format(table_name, columns)
    mycursor.execute(create_table)
    return create_table

# Create rebounds table
create_table("rebounds", """`type` varchar(12) NOT NULL UNIQUE,
                        `id` INT(255) NOT NULL AUTO_INCREMENT,
                        PRIMARY KEY (`id`)""")


# # Create shots table
create_table("shots", """	`type` varchar(12) NOT NULL,
	`sub_type` varchar(12),
	`basket_type` varchar(12),
	`points` INT(1),
	`made` BOOLEAN NOT NULL,
	`id` INT(255) NOT NULL AUTO_INCREMENT,
	UNIQUE(`type`, `sub_type`, `basket_type`, `points`, `made`),
	PRIMARY KEY (`id`)""")


# Create timeouts table
create_table("timeouts", """`type` varchar(15) NOT NULL UNIQUE,
	`id` INT(255) NOT NULL AUTO_INCREMENT,
	PRIMARY KEY (`id`)""")

# Create turnovers table
create_table("turnovers", """`type` varchar(12) NOT NULL UNIQUE,
	`id` INT(255) NOT NULL AUTO_INCREMENT,
	PRIMARY KEY (`id`)""")

# Create assists table
create_table("assists", """`type` varchar(14) NOT NULL UNIQUE,
	`id` INT(255) NOT NULL AUTO_INCREMENT,
	PRIMARY KEY (`id`)""")

# Create divisions table
create_table("divisions", """`name` varchar(20) NOT NULL UNIQUE,
	`alias` varchar(5) NOT NULL,
	`id` INT(255) NOT NULL AUTO_INCREMENT,
	PRIMARY KEY (`id`)""")

# Create conferences table
create_table("conferences", """`name` varchar(255) NOT NULL UNIQUE,
	`alias` varchar(10) NOT NULL,
	`id` INT(255) NOT NULL AUTO_INCREMENT,
	`division_id` INT(255),
	PRIMARY KEY (`id`),
    FOREIGN KEY (`division_id`) REFERENCES `divisions`(`id`)""")

# Create teams table
create_table("teams", """`id` INT NOT NULL AUTO_INCREMENT,
	`name` varchar(17) NOT NULL,
	`alias` varchar(5) NOT NULL,
	`market` varchar(30) NOT NULL,
	`conference_id` INT,
	PRIMARY KEY (`id`),
	UNIQUE(`name`, `market`),
	FOREIGN KEY (`conference_id`) REFERENCES `conferences`(`id`)""")

# Create players table
create_table("players", """`name` varchar(255) NOT NULL,
	`jersey_number` INT,
	`id` INT NOT NULL AUTO_INCREMENT,
	`team_id` INT,
	PRIMARY KEY (`id`),
    UNIQUE (`name`, `team_id`, `jersey_number`),
    FOREIGN KEY (`team_id`) REFERENCES `teams`(`id`)""")

# Create tournaments table
create_table("tournaments", """`id` INT(255) NOT NULL AUTO_INCREMENT,
	`name` varchar(10) NOT NULL,
	`type` varchar(25),
	UNIQUE(`name`, `type`),
	PRIMARY KEY (`id`)""")

# Create venues table
create_table("venues", """`id` INT(255) NOT NULL AUTO_INCREMENT,
	`name` varchar(255) UNIQUE NOT NULL,
	`city` varchar(16),
	`state` varchar(2),
	`address` varchar(45),
	`zip` varchar(5),
	`country_code` varchar(3),
	`capacity` varchar(8),
	PRIMARY KEY (`id`)""")

# Create games table
create_table("games", """`season` varchar(4) NOT NULL,
	`id` INT(255) NOT NULL AUTO_INCREMENT,
	`scheduled_date` DATETIME(6) NOT NULL,
	`attendance` INT(10),
	`neutral_site` BOOLEAN NOT NULL,
	`conference_game` BOOLEAN,
	`tournament_id` INT,
	`round` varchar(50),
	`game_no` varchar(50),
	`away_team_id` INT(255),
	`home_team_id` INT(255),
	`venue_id` INT(255),
	`home_basket_start` varchar(5),
	PRIMARY KEY (`id`),
    FOREIGN KEY (`away_team_id`) REFERENCES `teams`(`id`),
    FOREIGN KEY (`home_team_id`) REFERENCES `teams`(`id`),
    FOREIGN KEY (`venue_id`) REFERENCES `venues`(`id`),
    FOREIGN KEY (`tournament_id`) REFERENCES `tournaments`(`id`)""")


# Create events table
create_table("events", """`id` INT(255) NOT NULL AUTO_INCREMENT,
	`game_id` INT(255) NOT NULL,
	`period` INT(1) NOT NULL,
	`game_clock` varchar(5) NOT NULL,
	`elapsed_time_seconds` INT(4) NOT NULL,
	`team_id` INT(255),
	`player_id` INT(255),
	`timestamp` TIMESTAMP NOT NULL,
	`description` varchar(255) NOT NULL,
	`coord_x` INT(4),
	`coord_y` INT(4),
	`team_with_possession_id` INT(255),
	PRIMARY KEY (`id`),
    FOREIGN KEY (`game_id`) REFERENCES `games`(`id`),
    FOREIGN KEY (`team_id`) REFERENCES `teams`(`id`),
    FOREIGN KEY (`player_id`) REFERENCES `players`(`id`),
    FOREIGN KEY(`team_with_possession_id`) REFERENCES `teams`(`id`)""")
	

# Create event_type table
create_table("event_types", """`id` INT(255) NOT NULL AUTO_INCREMENT,
	`event_id` INT(255) NOT NULL,
	`type` varchar(255) NOT NULL,
	`rebound_id` INT(255),
	`shot_id` INT(255),
	`turnover_id` INT(255),
	`timeout_id` INT(255),
	`assist_id` INT(255),
	PRIMARY KEY (`id`),
	FOREIGN KEY (`event_id`) REFERENCES `events`(`id`),
    FOREIGN KEY (`rebound_id`) REFERENCES `rebounds`(`id`),
    FOREIGN KEY (`shot_id`) REFERENCES `shots`(`id`),
    FOREIGN KEY (`turnover_id`) REFERENCES `turnovers`(`id`),
	FOREIGN KEY (`assist_id`) REFERENCES `assists`(`id`),
    FOREIGN KEY (`timeout_id`) REFERENCES `timeouts`(`id`)""")




# Print tables to screen
mycursor.execute("SHOW TABLES")
for x in mycursor:
    print(x)
