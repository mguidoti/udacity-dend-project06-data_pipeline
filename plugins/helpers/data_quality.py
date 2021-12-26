class DataQuality:
    checks = {
        "songplays": {
            "not_null_columns": ["playid",
                                 "sessionid"],
            "min_number_rows": 1,
        },
        "artists": {
            "not_null_columns": ["artistid"],
            "min_number_rows": 1,
        },
        "songs": {
            "not_null_columns": ["artistid",
                                 "songid"],
            "min_number_rows": 1,
        },
        "time": {
            "not_null_columns": ["start_time"],
            "min_number_rows": 1,
        },
        "users": {
            "not_null_columns": ["userid"],
            "min_number_rows": 1,
        }
    }