import psycopg2


def connect():
    username = 'username'
    password = 'password'
    host = 'host'
    port = 'port'
    db = 'db'

    try:
        connection = psycopg2.connect(
            user=username,
            password=password,
            port=port,
            database=db)

        cursor = connection.cursor()

        cursor.execute("""SELECT entry.id AS "Id", entry.user_id AS "UserId", mrcuser.username AS "UserName", season.season_name AS "Season",
entry.station_id AS "IssuingPointId", station.station_name AS "IssuingPointName", entry.house_hold_number AS "HouseHoldNumber",
entry.house_hold_name AS "HouseHoldName", entry.house_hold_mob_number AS "HouseHoldMobNumber",
entry.total_number_of_male AS "TotalNetsRequired", entry.veo_or_meo_name AS "VeoOrMeoName", entry.user_id AS "VeoId",
entry.veo_id AS "VillageId", village.village_name AS "VillageName", village.ward_id AS "WardId", ward.ward_name AS "WardName",
district.id AS "DistrictId", district.district_name AS "DistrictName", region.id AS "RegionId", region.region_name AS "RegionName",
issue.total_nets AS "ActualIssuedNets", issue.reason AS "Reason",

CASE WHEN entry.id > 0 THEN '2019-2020'
END AS Campaign

FROM
mrcuser, entry, village, station, season, ward, district, region, issue

WHERE
entry.user_id = mrcuser.id AND
entry.station_id = station.id AND
entry.season_id = season.id AND
(entry.veo_id = village.id AND mrcuser.role_for = village.id) AND
(entry.station_id = station.id AND station.village_id = village.id) AND
(village.ward_id = ward.id AND ward.district_id = district.id) AND
district.region_id = region.id AND
entry.id = issue.entry_id AND
entry.data_flag = 'reviewed'
""")

        r = [dict((cursor.description[i][0], value) \
                  for i, value in enumerate(row)) for row in cursor.fetchall()]

        records = []
        for record in r:
            records.append(record)

        print("array", records)

        for rec in records:
            print("object", rec)
        # print(records)


    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL::", error)
    finally:
        # closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

def getConnectionData():
    username = 'username'
    password = 'password'
    host = 'host'
    port = 'port'
    db = 'db'

    try:
        connection = psycopg2.connect(
            user=username,
            password=password,
            port=port,
            database=db)

        cursor = connection.cursor()

        cursor.execute("SELECT * FROM msdDistribution_integration")

        integration = cursor.fetchall()

        print("integration: ", integration)
    except:
        print("exception: ")
    finally:
        print("finnally: ")

if __name__ == "__main__":
    connect()
