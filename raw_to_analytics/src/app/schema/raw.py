import polars as pl


BETS_INFORMATION_RAW = {
    'schema':{
        "customer": pl.String,  # varchar
        "transId": pl.Int64,  # bigint
        "refno": pl.Int64,  # bigint
        "custId": pl.Int64,
        "transDate": pl.String,
        "oddsId": pl.Int64,
        "hdp1": pl.Float64,  # decimal
        "hdp2": pl.Float64,  # decimal
        "odds": pl.Float64,  # decimal
        "stake": pl.Float64,  # decimal
        "status": pl.String,  # varchar
        "winlost": pl.Float64,  # decimal
        "liveHomeScore": pl.Int32,
        "liveAwayScore": pl.Int32,
        "liveIndicator": pl.Boolean,  # bit
        "betteam": pl.String,  # varchar
        "creator": pl.String,  # Nuevos - VarChar en la db, pero contiene valores numericos
        "comstatus": pl.String,  # Nuevos
        "winlostdate": pl.String,
        "betfrom": pl.String,  # varchar
        "sportsFrom": pl.Int16,     # Nuevos - 1, 2 tinyint
        "agtPT": pl.Float32,  # Nuevos - Decimal
        "maPT": pl.Float32,  # Nuevos - Decimal
        "smaPT": pl.Float32,  # Nuevos - Decimal
        "totalPT": pl.Float32,  # Nuevos - Decimal
        "agtWinlost": pl.Float32,  # Nuevos - Decimal
        "maWinlost": pl.Float32,  # Nuevos - Decimal
        "smaWinlost": pl.Float32,  # Nuevos - Decimal
        "playerDiscount": pl.Float32,  # Nuevos - Decimal
        "agtDiscount": pl.Float32,  # Nuevos - Decimal
        "maDiscount": pl.Float32,  # Nuevos - Decimal
        "smaDiscount": pl.Float32,  # Nuevos - Decimal
        "playerComm": pl.Float32,  # Nuevos - Decimal
        "agtComm": pl.Float32,  # Nuevos - Decimal
        "maComm": pl.Float32,  # Nuevos - Decimal
        "smaComm": pl.Float32,  # Nuevos - Decimal
        "actualRate": pl.Float64,  # decimal
        "matchId": pl.Int64,
        "mOdds": pl.Int32,     # Nuevos
        "agtId": pl.Int64,
        "maId": pl.Int32,  # Nuevos - min 6 max 54 474 287 int
        "smaId": pl.Int32,  # Nuevos - min 5 max 54 474 287 int
        # Nuevos - min 0 max 1 tinyint (probablemente binario)
        "ruben": pl.Int32,
        "statusWinlost": pl.Int16,  # tinyint
        "bettype": pl.Int16,  # tinyint
        "currency": pl.Int16,  # tinyint
        "actual_Stake": pl.Float64,  # decimal
        "transDesc": pl.String,  # varchar
        "ip": pl.String,  # varchar
        "userName": pl.String,  # varchar
        "currencyStr": pl.String,  # varchar
        "oddsStyle": pl.Int64,
        "betStatus": pl.String,
        "creatorName": pl.String,        # Nuevos - nvarchar 50
        "sportId": pl.Int64,
        "leagueId": pl.Int64,
        "dangerLevel": pl.Int64,
        "countryCode": pl.String,
        "directCustId": pl.Int32,           # Nuevos - min 08066 max 73 204 287 int
        "matchResultId": pl.Int64,
        "newBetType": pl.Int64,
        "displayType": pl.Int64,
        "betCondition": pl.String,  # varchar
        "betTypeGroupId": pl.Int64,
        "memberStatus": pl.Int32,      # Nuevos max 256
        "betPage": pl.Int16,  # tinyint
        # Nuevos  - No hay datos en la tabla pero esta definido como int
        "locationId": pl.Int32,
        "tstamp": pl.String,
        "acceptingStatus": pl.Int32,    # Nuevos - min 0 max 5 int
        "rejectReason": pl.Int32,     # Nuevos - min 0 max 100 int
        "checkTime": pl.String,     # Nuevos - datetime
        "actualOdds": pl.Float64,  # decimal
        "isAutoAccept": pl.Boolean,  # bit
        # Nuevos - No hay datos en la tabla pero esta definido como BigInt
        "extRefID": pl.Int64,
        # Nuevos - No hay datos en la tabla pero esta definido como Varchar 50
        "delAccountID": pl.String,
        # Nuevos - No hay datos en la tabla pero esta definido como decimal
        "lastCashBalance": pl.Float32,
        "modifyDate": pl.String,
        "actionId": pl.Int32,     # Nuevos - No hay datos en la tabla pero esta definido como int
        "webId": pl.Int64,
        "streamerId": pl.Int32,        # 0, -1 Nuevos
        "remark": pl.String,  # varchar
        "fullTimeHomeScore": pl.Int32,  # min 0 Max 148  Nuevos
        "fullTimeAwayScore": pl.Int32,  # min 0 Max 148  Nuevos
        "firstHalfHomeScore": pl.Int32,  # min 0 Max 76  Nuevos
        "firstHalfAwayScore": pl.Int32,  # min 0 Max 76 Nuevos
        "mpBonusRatio": pl.Float32,  # Nuevos - decimal
        "oddsProvider": pl.Int32,  # min 0 max 0  Nuevos
        "minBet": pl.Float32,  # decimal
        "maxBet": pl.Float32,  # decimal
        "oddsMaxBet": pl.Float32,  # decimal
        "fingerprint": pl.String,  # varchar
        "betGameTime": pl.String,  # varchar
        "settledTime": pl.String,     # Nuevos datetime "2025-02-13T11:21:15.73"
        "originalId": pl.Int64,     # Nuevos
        "customizeWebId": pl.Int32,     # min 19 max 1210 Nuevos
        "subBet"        : pl.List(
            pl.Struct({
                "transId":          pl.Int64,
                "transDate":        pl.String,
                "refNo":            pl.Int64,
                "custId":           pl.Int64,
                "oddsId":           pl.Int64,
                "hdp1":             pl.Float64,
                "hdp2":             pl.Float64,
                "odds":             pl.Float64,
                "betteam":          pl.String,
                "matchId":          pl.Int64,
                "matchDate":        pl.String,           # solo fecha
                "ruben":            pl.Int64,
                "statusWinlost":    pl.Int64,
                "bettype":          pl.Int64,
                "sportId":          pl.Int64,
                "matchResultId":    pl.Int64,
                "newBetType":       pl.Int64,
                "displayType":      pl.Int64,
                "betCondition":     pl.String,
                "betTypeGroupId":   pl.Int64,
                "tStamp":           pl.String,
                "status":           pl.String,
                "liveHomeScore":    pl.Int64,
                "liveAwayScore":    pl.Int64,
                "liveindicator":    pl.Boolean,
                "actualOdds":       pl.Float64,
                "checkTime":        pl.String,
                "acceptingStatus":  pl.Int64,
                "rejectReason":     pl.Int64,
                "fullTimeHomeScore":pl.Int64,          # acepta nulos
                "fullTimeAwayScore":pl.Int64,
                "firstHalfHomeScore":pl.Int64,
                "firstHalfAwayScore":pl.Int64
            })
            )
            
        },
    'information': {
        "field_id": ['customer', 'transId'],
        "field_modify_date": 'modifyDate',
        "field_tstamp": 'tstamp',
        "fields_datetime": ['transDate', 'checkTime', 'modifyDate', 'settledTime'],
        "fields_date": ['winlostdate'],
        "fields_special_datetime": [],
    },
    'to_insert': {
        'master': 'TransDate',
        'analytics': 'Winlostdate'
    }
}
