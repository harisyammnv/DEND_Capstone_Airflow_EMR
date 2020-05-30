class SqlQueries:
    """
    all create queries
    """

    immigration = """
                CREATE TABLE IF NOT EXISTS project.immigration (
                    cicid INT,
                    entry_year INT,
                    entry_month INT,
                    country_id INT,
                    res_id INT,
                    port_id VARCHAR,
                    mode_id INT,
                    state_code VARCHAR,
                    departure_date DATE,
                    departure_deadline DATE,
                    age INT,
                    visa_reason_id INT,
                    count INT,
                    visa_post VARCHAR,
                    matched_flag VARCHAR,
                    gender VARCHAR,
                    ins_num VARCHAR,
                    airline_abbr VARCHAR,
                    admission_num FLOAT,
                    flight_no VARCHAR,
                    visa_type VARCHAR
                    );
                """

    airports = """
                CREATE TABLE IF NOT EXISTS project.airport_codes (
                            icao_code VARCHAR,
                            type VARCHAR,
                            name VARCHAR,
                            airport_latitude FLOAT,
                            airport_longitude FLOAT,
                            elevation_ft FLOAT,
                            continent VARCHAR,
                            country VARCHAR,
                            state_code VARCHAR,
                            municipality VARCHAR,
                            iata_code VARCHAR,
                            );
               """

    i94ports = """
                CREATE TABLE IF NOT EXISTS project.i94ports (
                port_code VARCHAR,
                port_city VARCHAR,
                state_code_or_country VARCHAR
                );
                """

    i94visa = """
               CREATE TABLE IF NOT EXISTS project.i94visa (
               visa_code INT,
               visa_purpose VARCHAR
               );
              """

    i94mode = """
                CREATE TABLE IF NOT EXISTS project.i94mode (
                mode_id INT,
                transportation_mode VARCHAR
                );
              """

    i94addr = """
              CREATE TABLE IF NOT EXISTS project.i94addr (
              state_code VARCHAR,
              state_name VARCHAR
              );
              """

    i94res = """
             CREATE TABLE IF NOT EXISTS project.i94res (
             country_id INT,
             country_name VARCHAR
             );
             """

    visa_type = """
             CREATE TABLE IF NOT EXISTS project.visa_type (
             visa_type VARCHAR,
             visa_type_description VARCHAR
             );
             """

    visa_port_of_issue = """
             CREATE TABLE IF NOT EXISTS project.visa_ports (
             port_of_issue VARCHAR,
             visa_post_code VARCHAR
             );
             """

    airlines = """
             CREATE TABLE IF NOT EXISTS project.airlines (
             airline_name VARCHAR,
             airline_iata_code VARCHAR,
             airline_3_digit_code VARCHAR,
             airline_icao_code VARCHAR,
             origin_country VARCHAR
             );
             """

    us_cities_demographics = """
            CREATE TABLE IF NOT EXISTS project.us_cities_demographics (
            city VARCHAR,
            state VARCHAR,
            median_age FLOAT,
            male_population INT ,
            female_population INT ,
            total_population INT,
            num_of_veterans INT,
            foreign_born INT,
            avg_household_size FLOAT,
            state_code VARCHAR,
            predominant_race VARCHAR,
            count INT
            );
            """

    drop_tables = """
    DROP TABLE IF EXISTS project.immigration;
    DROP TABLE IF EXISTS project.airport_codes;
    DROP TABLE IF EXISTS project.i94port;
    DROP TABLE IF EXISTS project.i94visa;
    DROP TABLE IF EXISTS project.i94mode;
    DROP TABLE IF EXISTS project.i94addr;
    DROP TABLE IF EXISTS project.i94res;
    DROP TABLE IF EXISTS project.us_cities_demographics;
    DROP TABLE IF EXISTS project.airlines;
    DROP TABLE IF EXISTS project.visa_port_of_issue;
    DROP TABLE IF EXISTS project.visa_type
    """

    create_tables = immigration + airports + i94ports + i94visa + i94mode + i94addr + i94res + us_cities_demographics + airlines + visa_port_of_issue + visa_type
    tables = ["immigration", "airport_codes", "i94ports", "i94visa",
              "i94mode", "i94addr", "i94res", "us_cities_demographics", "airlines", "visa_port_of_issue","visa_type"]

    copy_csv_cmd = """
    COPY project.{} FROM '{}'
    CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}'
    IGNOREHEADER 1
    DELIMITER '{}'
    COMPUPDATE OFF
    TRUNCATECOLUMNS
    CSV;
    """
    create_schema = """
    CREATE schema IF NOT EXISTS project;
    """
    count_check = """SELECT CASE WHEN COUNT(*) > 1 THEN 1 ELSE 0 END AS non_empty FROM project.{}"""

    copy_parquet_cmd = """
                        COPY project.{} FROM '{}'
                        IAM_ROLE '{}'
                        FORMAT AS PARQUET;
                       """