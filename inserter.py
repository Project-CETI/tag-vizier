import mysql.connector
from mysql.connector import errorcode
from swarmapi import SwarmAPI
import sched, time


class SwarmBase:
    def __init__(
        self,
        config={
            "user": "shashank",
            "password": "mariamaria",
            "host": "localhost",
            "raise_on_warnings": True,
            "use_pure": False,
        },
    ):
        try:
            self.cnx = mysql.connector.connect(**config)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        else:
            print("MariaDB connection successful.")
            self.cursor = self.cnx.cursor()

        self.swarm = SwarmAPI()

        print("Initializing database ...")
        self.init_db()

    def _init_tables(self):
        DB_NAME = "SWARM"

        TABLES = {}

        TABLES["telemetry"] = (
            "CREATE TABLE IF NOT EXISTS `telemetry` ("
            "  `packetId` int NOT NULL,"
            "  `telemetryVersion` int NOT NULL,"
            "  `telemetryAt` varchar(40) NOT NULL,"
            "  `telemetryLatitude` float(32) NOT NULL,"
            "  `telemetryLongitude` float(32) NOT NULL,"
            "  `telemetryAltitude` int(12) NOT NULL,"
            "  `telemetryCourse` int(12) NOT NULL,"
            "  `telemetrySpeed` int(12) NOT NULL,"
            "  `telemetryBatteryVoltage` float(12) NOT NULL,"
            "  `telemetryBatteryCurrent` float(12) NOT NULL,"
            "  `telemetryTemperatureK` int(32) NOT NULL,"
            "  `deviceType` int(4) NOT NULL,"
            "  `deviceId` int(32) NOT NULL,"
            " PRIMARY KEY (`packetId`)"
            ") ENGINE=InnoDB"
        )

        TABLES["data"] = (
            "CREATE TABLE IF NOT EXISTS `data` ("
            "  `messageId` int NOT NULL,"
            "  `packetId` int NOT NULL,"
            "  `deviceType` int(4) NOT NULL,"
            "  `deviceId` int(32) NOT NULL,"
            "  `viaDeviceId` int(32) NOT NULL,"
            "  `dataType` int(4) NOT NULL,"
            "  `userApplicationId` int(32) NOT NULL,"
            "  `organizationId` int(32) NOT NULL,"
            "  `len` int(16) NOT NULL,"
            "  `data` varchar(1000) NOT NULL,"
            "  `ackPacketId` int(12) NOT NULL,"
            "  `status` int(4) NOT NULL,"
            "  `hiveRxTime` varchar(40) NOT NULL,"
            " PRIMARY KEY (`packetId`) "
            ") ENGINE=InnoDB"
        )

        try:
            self.cursor.execute("USE {}".format(DB_NAME))
        except mysql.connector.Error as err:
            print("Database {} does not exist.".format(DB_NAME))
            if err.errno == errorcode.ER_BAD_DB_ERROR:
                self._create_database(DB_NAME)
                print("Database {} created successfully.".format(DB_NAME))
                self.cnx.database = DB_NAME
            else:
                print(err)
                exit(1)

        for table_name in TABLES:
            table_description = TABLES[table_name]
            try:
                print("Creating table {}: ".format(table_name), end="")
                self.cursor.execute(table_description)
            except mysql.connector.Error as err:
                print(err.msg)
            else:
                print("OK")

        self._define_logs()

    def _define_logs(self):
        self.add_data = (
            "INSERT IGNORE INTO data "
            "(messageId, packetId, deviceType, deviceId, viaDeviceId, dataType,  "
            "userApplicationId, organizationId, len, data, "
            "ackPacketId, status, hiveRxTime) VALUES ("
            "%(messageId)s, %(packetId)s, %(deviceType)s, %(deviceId)s, %(viaDeviceId)s, %(dataType)s,  "
            "%(userApplicationId)s, %(organizationId)s, %(len)s, %(data)s, "
            "%(ackPacketId)s, %(status)s, %(hiveRxTime)s)"
        )
        self.add_telem = (
            "INSERT IGNORE INTO telemetry "
            "(packetId, telemetryVersion, telemetryAt, telemetryLatitude,"
            "telemetryLongitude, telemetryAltitude, telemetryCourse, "
            "telemetrySpeed, telemetryBatteryVoltage, "
            "telemetryBatteryCurrent, telemetryTemperatureK, deviceType, "
            "deviceId) "
            "VALUES (%(packetId)s, %(telemetryVersion)s, %(telemetryAt)s, %(telemetryLatitude)s,"
            "%(telemetryLongitude)s, %(telemetryAltitude)s, %(telemetryCourse)s, "
            "%(telemetrySpeed)s, %(telemetryBatteryVoltage)s, "
            "%(telemetryBatteryCurrent)s, %(telemetryTemperatureK)s, %(deviceType)s, "
            "%(deviceId)s)"
        )

    def _create_database(self, DB_NAME):
        try:
            self.cursor.execute(
                "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME)
            )
        except mysql.connector.Error as err:
            print("Failed creating database: {}".format(err))
            exit(1)

    def _log_dping(self, ping):
        """
        Log data ping into MariaDB database named 'data'.

        Follows the structure of the database. The insertion method is described in the log structure attribute `add_data`.

        Input:
            ping: One JSON data message. Ensure packet labels match database labels.
        Returns: None
        """
        self.cursor.execute(self.add_data, ping)

    def _log_tping(self, ping):
        """
        Log telemetry ping into MariaDB database named 'telemetry'.

        Follows the structure of the database. The insertion method is described in the log structure attribute `add_telem`.

        Input:
            One JSON telemetry message. Ensure packet labels match database labels.
        Returns: None
        """
        self.cursor.execute(self.add_telem, ping)

    def _log_recent(self, count=10, use_cached=False):
        """
        Logs all recently retrieved messages. Ignores duplicates.

        Input:
            count: `int` (Optional) Selects how many recent messages are fetched from the native SWARM server. Range [10,1000]
            use_cached: `boolean` (Optional) Chooses to use data cached from previous retrieval if True. Default to False.
        """

        self.swarm.get_recent_data(count=count, use_cached=use_cached)

        for tping in self.swarm.recent_telemetry:
            try:
                self._log_tping(tping)
            except:
                self.tel_dup_num += 1
        print("Logged all unique telemetry pings.")
        if self.tel_dup_num > 100:
            print(
                "WARNING! %s duplicate telemetry log entries attempted."
                % self.tel_dup_num
            )
            print("Duplicate entry attempts exceeds 100 ... resetting attempt counter.")
            self.tel_dup_num = 0

        for dping in self.swarm.recent_data:
            try:
                self._log_dping(dping)
            except:
                self.dat_dup_num += 1
        print("Logged all unique data pings.")
        if self.dat_dup_num > 100:
            print(
                "WARNING! %s duplicate data log entries attempted." % self.dat_dup_num
            )
            print("Duplicate entry attempts exceeds 100 ... resetting attempt counter.")
            self.dat_dup_num = 0

        self.cnx.commit()
        print("Logs committed.")

    def init_db(self):
        self._init_tables()

        self._define_logs()
        print("Logging is defined.")

        self.tel_dup_num = 0
        self.dat_dup_num = 0
        print("Fetching all retained server history...")
        self._log_recent(count=1000, use_cached=False)

    def update(self):
        self._log_recent(count=10, use_cached=False)


if __name__ == "__main__":
    sb = SwarmBase()
    s = sched.scheduler(time.time, time.sleep)
    s.enter(60, 1, sb.update)
    s.run()
