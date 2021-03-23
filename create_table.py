def create_table():
    with open('Odata2019File.csv') as f:
        header = [h.strip('"') for h in f.readline().strip().split(';')]
        header.append('year')

    begin_creation = """CREATE TABLE results ("""

    end_creation = """);"""

    for names in header:
        if names == "OUTID":datatype = "varchar(100) PRIMARY KEY"
        elif names == "Birth" or names == "year": datatype = "smallint"
        elif "Ball" in names: datatype = "numeric"
        else: datatype = "TEXT"
        continuation = """\n    {} {},""".format(names, datatype)
        begin_creation += continuation

    creation = begin_creation[:-1] + end_creation
    return creation

# def create_table(conn):
#     command = (
#         """
#         DROP TABLE IF EXISTS results;

#         CREATE TABLE results
#         (
#         OUTID               TEXT,
#         Birth               integer,
#         SEXTYPENAME         TEXT,
#         REGNAME             TEXT,
#         AREANAME            TEXT,
#         TERNAME             TEXT,
#         REGTYPENAME         TEXT,
#         TerTypeName         TEXT,
#         ClassProfileNAME    TEXT,
#         ClassLangName       TEXT,
#         EONAME              TEXT,
#         EOTYPENAME          TEXT,
#         EORegName           TEXT,
#         EOAreaName          TEXT,
#         EOTerName           TEXT,
#         EOParent            TEXT,
#         UkrTest             TEXT,
#         UkrTestStatus       TEXT,
#         UkrBall100          numeric(10,2),
#         UkrBall12           integer,
#         UkrBall             integer,
#         UkrAdaptScale       integer,
#         UkrPTName           TEXT,
#         UkrPTRegName        TEXT,
#         UkrPTAreaName       TEXT,
#         UkrPTTerName        TEXT,
#         histTest            TEXT,
#         HistLang            TEXT,
#         histTestStatus      TEXT,
#         histBall100         numeric(10,2),
#         histBall12          integer,
#         histBall            integer,
#         histPTName          TEXT,
#         histPTRegName       TEXT,
#         histPTAreaName      TEXT,
#         histPTTerName       TEXT,
#         mathTest            TEXT,
#         mathLang            TEXT,
#         mathTestStatus      TEXT,
#         mathBall100         numeric(10,2),
#         mathBall12          integer,
#         mathBall            integer,
#         mathPTName          TEXT,
#         mathPTRegName       TEXT,
#         mathPTAreaName      TEXT,
#         mathPTTerName       TEXT,
#         physTest            TEXT,
#         physLang            TEXT,
#         physTestStatus      TEXT,
#         physBall100         numeric(10,2),
#         physBall12          integer,
#         physBall            integer,
#         physPTName          TEXT,
#         physPTRegName       TEXT,
#         physPTAreaName      TEXT,
#         physPTTerName       TEXT,
#         chemTest            TEXT,
#         chemLang            TEXT,
#         chemTestStatus      TEXT,
#         chemBall100         numeric(10,2),
#         chemBall12          integer,
#         chemBall            integer,
#         chemPTName          TEXT,
#         chemPTRegName       TEXT,
#         chemPTAreaName      TEXT,
#         chemPTTerName       TEXT,
#         bioTest             TEXT,
#         bioLang             TEXT,
#         bioTestStatus       TEXT,
#         bioBall100          numeric(10,2),
#         bioBall12           integer,
#         bioBall             integer,
#         bioPTName           TEXT,
#         bioPTRegName        TEXT,
#         bioPTAreaName       TEXT,
#         bioPTTerName        TEXT,
#         geoTest             TEXT,
#         geoLang             TEXT,
#         geoTestStatus       TEXT,
#         geoBall100          numeric(10,2),
#         geoBall12           integer,
#         geoBall             integer,
#         geoPTName           TEXT,
#         geoPTRegName        TEXT,
#         geoPTAreaName       TEXT,
#         geoPTTerName        TEXT,
#         engTest             TEXT,
#         engTestStatus       TEXT,
#         engBall100          numeric(10,2),
#         engBall12           integer,
#         engDPALevel         TEXT,
#         engBall             integer,
#         engPTName           TEXT,
#         engPTRegName        TEXT,
#         engPTAreaName       TEXT,
#         engPTTerName        TEXT,
#         fraTest             TEXT,
#         fraTestStatus       TEXT,
#         fraBall100          numeric(10,2),
#         fraBall12           integer,
#         fraDPALevel         TEXT,
#         fraBall             integer,
#         fraPTName           TEXT,
#         fraPTRegName        TEXT,
#         fraPTAreaName       TEXT,
#         fraPTTerName        TEXT,
#         deuTest             TEXT,
#         deuTestStatus       TEXT,
#         deuBall100          numeric(10,2),
#         deuBall12           integer,
#         deuDPALevel         TEXT,
#         deuBall             integer,
#         deuPTName           TEXT,
#         deuPTRegName        TEXT,
#         deuPTAreaName       TEXT,
#         deuPTTerName        TEXT,
#         spaTest             TEXT,
#         spaTestStatus       TEXT,
#         spaBall100          numeric(10,2),
#         spaBall12           integer,
#         spaDPALevel         TEXT,
#         spaBall             integer,
#         spaPTName           TEXT,
#         spaPTRegName        TEXT,
#         spaPTAreaName       TEXT,
#         spaPTTerName        TEXT,
#         zno_year            integer
#         )""")
#     cur = conn.cursor()
#     cur.execute(command)
#     cur.close()