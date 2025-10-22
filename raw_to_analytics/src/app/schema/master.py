import polars as pl


CONTROL_SCHEMA = {
    'index': pl.UInt32,
    'day': pl.String,
    'insertion_type': pl.String,
    'n_registers': pl.UInt32,
    'duration': pl.Float32,
    'start_execution': pl.String,
    'end_execution': pl.String,
    'comments': pl.String
}

BETS_INFORMATION_MASTER = {
    'schema':{
            "Customer": pl.String,  # varchar
        "TransId": pl.UInt64,  # bigint
        "Refno": pl.UInt64,  # bigint
        "CustId": pl.UInt64,
        "TransDate": pl.Datetime,
        "OddsId": pl.UInt64,
        "Hdp1": pl.Float64,  # decimal
        "Hdp2": pl.Float64,  # decimal
        "Odds": pl.Float64,  # decimal
        "Stake": pl.Float64,  # decimal
        "Status": pl.String,  # varchar
        "Winlost": pl.Float64,  # decimal
        "LiveHomeScore": pl.Int16,
        "LiveAwayScore": pl.Int16,
        "LiveIndicator": pl.Boolean,  # bit
        "Betteam": pl.String,  # varchar
        "Creator": pl.String,  # Nuevos - VarChar en la db, pero contiene valores numericos
        "Comstatus": pl.String,  # Nuevos
        "Winlostdate": pl.Date,
        "Betfrom": pl.String,  # varchar
        "SportsFrom": pl.UInt8,     # Nuevos - 1, 2 tinyint
        "AgtPT": pl.Float32,  # Nuevos - Decimal
        "MaPT": pl.Float32,  # Nuevos - Decimal
        "SmaPT": pl.Float32,  # Nuevos - Decimal
        "TotalPT": pl.Float32,  # Nuevos - Decimal
        "AgtWinlost": pl.Float32,  # Nuevos - Decimal
        "MaWinlost": pl.Float32,  # Nuevos - Decimal
        "SmaWinlost": pl.Float32,  # Nuevos - Decimal
        "PlayerDiscount": pl.Float32,  # Nuevos - Decimal
        "AgtDiscount": pl.Float32,  # Nuevos - Decimal
        "MaDiscount": pl.Float32,  # Nuevos - Decimal
        "SmaDiscount": pl.Float32,  # Nuevos - Decimal
        "PlayerComm": pl.Float32,  # Nuevos - Decimal
        "AgtComm": pl.Float32,  # Nuevos - Decimal
        "MaComm": pl.Float32,  # Nuevos - Decimal
        "SmaComm": pl.Float32,  # Nuevos - Decimal<
        "ActualRate": pl.Float64,  # decimal
        "MatchId": pl.UInt32,
        "MOdds": pl.Int32,     # Nuevos -- desconocido
        "AgtId": pl.UInt32,
        "MaId": pl.UInt32,  # Nuevos - min 6 max 54 474 287 int
        "SmaId": pl.UInt32,  # Nuevos - min 5 max 54 474 287 int
        # Nuevos - min 0 max 1 tinyint (probablemente binario)
        "Ruben": pl.UInt8,
        "StatusWinlost": pl.Int16,  # tinyint
        "Bettype": pl.Int16,  # tinyint
        "Currency": pl.Int16,  # tinyint
        "Actual_Stake": pl.Float64,  # decimal
        "TransDesc": pl.String,  # varchar
        "Ip": pl.String,  # varchar
        "UserName": pl.String,  # varchar
        "CurrencyStr": pl.String,  # varchar
        "OddsStyle": pl.Int32,
        "BetStatus": pl.String,
        "CreatorName": pl.String,        # Nuevos - nvarchar 50
        "SportId": pl.UInt32,
        "LeagueId": pl.UInt32,
        "DangerLevel": pl.UInt8,
        "CountryCode": pl.String,
        "DirectCustId": pl.Int32,           # Nuevos - min 08066 max 73 204 287 int
        "MatchResultId": pl.UInt32,
        "NewBetType": pl.Int32,
        "DisplayType": pl.Int32,
        "BetCondition": pl.String,  # varchar
        "BetTypeGroupId": pl.UInt32,
        "MemberStatus": pl.Int32,      # Nuevos max 256
        "BetPage": pl.Int16,  # tinyint
        # Nuevos  - No hay datos en la tabla pero esta definido como int
        "LocationId": pl.UInt32,
        "Tstamp": pl.String,
        "AcceptingStatus": pl.Int16,    # Nuevos - min 0 max 5 int
        "RejectReason": pl.Int16,     # Nuevos - min 0 max 100 int
        "CheckTime": pl.Datetime,     # Nuevos - datetime
        "ActualOdds": pl.Float64,  # decimal
        "IsAutoAccept": pl.Boolean,  # bit
        # Nuevos - No hay datos en la tabla pero esta definido como BigInt
        "ExtRefID": pl.UInt32,
        # Nuevos - No hay datos en la tabla pero esta definido como Varchar 50
        "DelAccountID": pl.UInt32,
        # Nuevos - No hay datos en la tabla pero esta definido como decimal
        "LastCashBalance": pl.Float32,
        "ModifyDate": pl.Datetime,       # Datetime
        # Nuevos - No hay datos en la tabla pero esta definido como int
        "ActionId": pl.UInt32,
        "WebId": pl.UInt32,
        "StreamerId": pl.Int8,        # 0, -1 Nuevos
        "Remark": pl.String,  # varchar
        "FullTimeHomeScore": pl.Int16,  # min 0 Max 148  Nuevos
        "FullTimeAwayScore": pl.Int16,  # min 0 Max 148  Nuevos
        "FirstHalfHomeScore": pl.Int16,  # min 0 Max 76  Nuevos
        "FirstHalfAwayScore": pl.Int16,  # min 0 Max 76 Nuevos
        "MpBonusRatio": pl.Float32,  # Nuevos - decimal
        "OddsProvider": pl.Int16,  # min 0 max 0  Nuevos
        "MinBet": pl.Float32,  # decimal
        "MaxBet": pl.Float32,  # decimal
        "OddsMaxBet": pl.Float32,  # decimal
        "Fingerprint": pl.String,  # varchar
        "BetGameTime": pl.String,  # varchar
        "SettledTime": pl.Datetime,     # Nuevos datetime "2025-02-13T11:21:15.73"
        "OriginalId": pl.UInt32,     # Nuevos
        "CustomizeWebId": pl.UInt32,     # min 19 max 1210 Nuevos
        "SubBet"        : pl.List(
            pl.Struct({
                "transId":          pl.Int64,
                "transDate":        pl.Datetime,
                "refNo":            pl.Int64,
                "custId":           pl.Int64,
                "oddsId":           pl.Int64,
                "hdp1":             pl.Float64,
                "hdp2":             pl.Float64,
                "odds":             pl.Float64,
                "betteam":          pl.String,
                "matchId":          pl.Int64,
                "matchDate":        pl.Datetime,           # solo fecha
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
                "checkTime":        pl.Datetime,
                "acceptingStatus":  pl.Int64,
                "rejectReason":     pl.Int64,
                "fullTimeHomeScore":pl.Int64,          # acepta nulos
                "fullTimeAwayScore":pl.Int64,
                "firstHalfHomeScore":pl.Int64,
                "firstHalfAwayScore":pl.Int64
            })
    ),
    "__comment": pl.String
    },
}
