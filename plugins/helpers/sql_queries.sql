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
                    visa_reason_id INT REFERENCES i94visa(visa_code),
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
                    );

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
                            );


CREATE TABLE IF NOT EXISTS i94ports (
                port_code VARCHAR PRIMARY KEY,
                port_city VARCHAR,
                state_code_or_country VARCHAR
                );

               CREATE TABLE IF NOT EXISTS i94visa (
               visa_code INT PRIMARY KEY,
               visa_purpose VARCHAR
               );

CREATE TABLE IF NOT EXISTS i94mode (
                mode_id INT PRIMARY KEY,
                transportation_mode VARCHAR);


CREATE TABLE IF NOT EXISTS i94addr (
              state_code VARCHAR PRIMARY KEY,
              state_name VARCHAR
              );


CREATE TABLE IF NOT EXISTS i94res (
             country_id INT PRIMARY KEY,
             country_name VARCHAR
             );


CREATE TABLE IF NOT EXISTS visa_type (
             visa_type VARCHAR PRIMARY KEY,
             visa_type_description VARCHAR
             );


CREATE TABLE IF NOT EXISTS visa_ports (
             port_of_issue VARCHAR,
             visa_post_code VARCHAR PRIMARY KEY
             );



CREATE TABLE IF NOT EXISTS airlines (
             airline_name VARCHAR,
             airline_iata_code VARCHAR PRIMARY KEY,
             airline_three_digit_code VARCHAR,
             airline_icao_code VARCHAR,
             origin_country VARCHAR
             );


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
            count INT);