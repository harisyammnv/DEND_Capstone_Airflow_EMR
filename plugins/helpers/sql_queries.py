class SqlQueries:
    """
    all create queries
    """

    immigration = """
CREATE TABLE IF NOT EXISTS immigration (
                    cicid INT PRIMARY KEY,
                    entry_year INT,
                    entry_month INT,
                    country_id INT REFERENCES i94res(country_id),
                    res_id INT REFERENCES i94res(country_id),
                    port_id VARCHAR REFERENCES i94ports(port_code),
                    arrival_date DATE,
                    mode_id INT REFERENCES i94mode(mode_id),
                    state_code VARCHAR REFERENCES i94addr(state_code),
                    departure_date DATE,
                    departure_deadline DATE,
                    age INT,
                    visa_reason_id INT REFERENCES i94visa(visa_id),
                    count INT,
                    visa_post VARCHAR REFERENCES visa_ports(visa_post_code),
                    matched_flag VARCHAR,
                    birth_year INT,
                    gender VARCHAR,
                    ins_num VARCHAR,
                    airline_abbr VARCHAR REFERENCES airlines(airline_iata_code),
                    admission_num FLOAT,
                    flight_no VARCHAR,
                    visa_type VARCHAR REFERENCES visa_type(visa_type)
                    );"""
    staging_immigration = """
                CREATE TABLE IF NOT EXISTS staging_immigration (
                    cicid INT PRIMARY KEY,
                    entry_year INT,
                    entry_month INT,
                    country_id INT,
                    res_id INT,
                    port_id VARCHAR,
                    arrival_date DATE,
                    mode_id INT,
                    state_code VARCHAR,
                    departure_date DATE,
                    departure_deadline DATE,
                    age INT,
                    visa_reason_id INT,
                    count INT,
                    visa_post VARCHAR,
                    matched_flag VARCHAR,
                    birth_year INT,
                    gender VARCHAR,
                    ins_num VARCHAR,
                    airline_abbr VARCHAR,
                    admission_num FLOAT,
                    flight_no VARCHAR,
                    visa_type VARCHAR
                    );"""

    airports = """
                CREATE TABLE IF NOT EXISTS airport_codes (
                            icao_code VARCHAR,
                            airport_type VARCHAR,
                            airport_name VARCHAR,
                            elevation_ft FLOAT,
                            continent VARCHAR,
                            municipality VARCHAR,
                            nearest_city VARCHAR REFERENCES us_cities_demographics(city)
                            iata_code VARCHAR,
                            airport_latitude FLOAT,
                            airport_longitude FLOAT,
                            country VARCHAR,
                            state_code VARCHAR
                            );"""

    i94ports = """
                CREATE TABLE IF NOT EXISTS i94ports (
                port_code VARCHAR PRIMARY KEY,
                port_city VARCHAR,
                state_code_or_country VARCHAR
                );"""

    i94visa = """
               CREATE TABLE IF NOT EXISTS i94visa (
               visa_id INT PRIMARY KEY,
               visa_purpose VARCHAR
               );"""

    i94mode = """
                CREATE TABLE IF NOT EXISTS i94mode (
                mode_id INT PRIMARY KEY,
                transportation_mode VARCHAR);"""

    i94addr = """
              CREATE TABLE IF NOT EXISTS i94addr (
              state_code VARCHAR PRIMARY KEY,
              state_name VARCHAR
              );"""

    i94res = """
             CREATE TABLE IF NOT EXISTS i94res (
             country_id INT PRIMARY KEY,
             country_name VARCHAR
             );"""

    visa_type = """
             CREATE TABLE IF NOT EXISTS visa_type (
             visa_type VARCHAR PRIMARY KEY,
             visa_type_description VARCHAR
             );"""

    visa_port_of_issue = """
             CREATE TABLE IF NOT EXISTS visa_ports (
             port_of_issue VARCHAR,
             visa_post_code VARCHAR PRIMARY KEY
             );
             """

    airlines = """
             CREATE TABLE IF NOT EXISTS airlines (
             airline_name VARCHAR,
             airline_iata_code VARCHAR PRIMARY KEY,
             airline_three_digit_code VARCHAR,
             airline_icao_code VARCHAR,
             origin_country VARCHAR
             );"""

    us_cities_demographics = """
            CREATE TABLE IF NOT EXISTS us_cities_demographics (
            city VARCHAR PRIMARY KEY,
            state VARCHAR,
            median_age FLOAT,
            male_population INT ,
            female_population INT ,
            total_population INT,
            num_of_veterans INT,
            foreign_born INT,
            avg_household_size FLOAT,
            state_code VARCHAR REFERENCES i94addr(state_code),
            predominant_race VARCHAR,
            count INT);"""

    create_dim_tables = [i94ports, i94visa, i94mode, i94addr, i94res, airlines, visa_port_of_issue, visa_type, us_cities_demographics, airports,staging_immigration]
    create_fact_tables = [immigration]
    tables = ["i94ports", "i94visa",
              "i94mode", "i94addr", "i94res", "airlines", "visa_ports","visa_type","us_cities_demographics", "airport_codes"]
    parquet_tables= ["lake/i94_meta_data/port_codes/",
                     "lake/i94_meta_data/visa/",
                     "lake/i94_meta_data/transportation/",
                     "lake/i94_meta_data/state_codes/",
                     "lake/i94_meta_data/country_codes/",
                     "lake/codes/airline_codes/",
                     "lake/visa-issue-port/", "lake/visa-type/", "lake/demographics/","lake/codes/airport_codes/"]

    count_check = """SELECT CASE WHEN COUNT(*) > 1 THEN 1 ELSE 0 END AS non_empty FROM project.{}"""

# "lake/codes/country_code/", "lake/codes/port-of-entry-codes/",
