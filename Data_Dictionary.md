## DEND Capstone DWH Data Dictionary

### Data Dictionary Dimension Tables

#### Airport_codes Table
 * icao_code: string (nullable = true) - location indicator - four-letter code designating aerodromes around the world
 * airport_type: string (nullable = true) - type of the airport
 * airport_name: string (nullable = true) - Name of the airport
 * elevation_ft: double (nullable = true) - elevation of the airport
 * continent: string (nullable = true) - continent of the airport
 * country: string (nullable = true) - country of the airport eg. United States
 * state_code: string (nullable = true) - State of the U.S.
 * municipality: string (nullable = true) - municipality of State of the U.S.
 * nearest_city: string (nullable = true) - nearest city to the airport
 * iata_code: string (nullable = true) - IATA code for the airport
 * airport_latitude: float (nullable = true) - airport's GPS latitude
 * airport_longitude: float (nullable = true) - airport's GPS latitude
 
#### U.S. cities demographics Table
 * city: string - PRIMARY KEY - Full city name
 * state: string (nullable = true) - Full state name
 * state_code: string (nullable = true) - Abbreviated state code
 * median_age: double (nullable = true) - Median Age per state
 * male_population: int (nullable = true) - Male population
 * female_population: int (nullable = true) - Female population
 * total_population: int (nullable = true) - Total population
 * num_of_veterans: int (nullable = true) - Num of Veteran population
 * foreign_born: int (nullable = true) - Foreign-Born population
 * avg_house_hold_size: float (nullable = true) - average house hold size
 * predominant_race: string (nullable = true) - race in the city

#### Visa Type Table
 * visa_type: string - PRIMARY KEY - US Visa Type
 * visa_type_description: string (nullable = true)  - US Visa Type Description

#### I94addr Table
 * state_code: string - PRIMARY KEY - Abbreviated US state name
 * state_name: string (nullable = true) - Full US State name
 
#### I94ports Table
 * port_code: string - PRIMARY KEY - port of entry code
 * port_city: string (nullable = true) - port of entry code
 * state_code_or_country: string (nullable = true) - Abbr. US State name or country
 
#### I94res Table
 * country_id: int - PRIMARY KEY - ID of the country in USIS Database
 * country_name: string (nullable = true) - Full name of the country according to USIS Database
 
#### I94mode Table
 * mode_id: int - PRIMARY KEY - mode of transport ID
 * transportation_mode: string (nullable = true) - mode of transport
 
#### I94visa Table
 * visa_id: string - PRIMARY KEY - ID of the visa issued for a reason
 * visa_purpose: string (nullable = true) - purpose stating the entry for US

#### visa_issue_post Table
 * visa_post_code: string - PRIMARY KEY - code of the city where visa is issued
 * port_of_issue: string (nullable = true) - Full name of the city where the visa is issued
 
#### airlines Table
 * airline_name: string (nullable = true) - name of the airlines
 * airline_iata_code: string - PRIMARY KEY - Standard IATA code designated around the world
 * airline_three_digit_code: string (nullable = true) - Standard 3 digit code designated around the world
 * airline_icao_code: string (nullable = true) - Standard ICAO code designated around the world
 * airline_country: string (nullable = true) - Home/Origin country of the airlines

#### immigration Table
 * cicid: int - PRIMARY KEY - id of the entry in immigration data
 * entry_year: int (nullable = true)  - Entry year into US
 * entry_month: int (nullable = true)  - Entry month into US
 * country_id: int (nullable = true) - Origin Country ID
 * res_id: int (nullable = true) - residence Country ID
 * port_id: string (nullable = true) - entry port id
 * arrival_date: date (nullable = true) - arrival date into US
 * mode_id: int (nullable = true) - id for mode of transport 
 * state_code: string (nullable = true) - state code in US
 * departure_date: date (nullable = true) - departure date from US
 * departure_deadline: date (nullable = true) - visa expiry date from US
 * age: int (nullable = true) - age of the individual entered into US
 * visa_reason_id: int (nullable = true) - id for the visa issuance for a reason
 * count: int (nullable = true) - statistics count
 * visa_post: string (nullable = true) - post code for the visa
 * matched_flag: string (nullable = true) - details matched during entry and exit
 * birth_year: int (nullable = true) - individual's year of birth
 * gender: string (nullable = true) - individual's gender
 * ins_num: string (nullable = true) - INS number
 * airline_abbr: string (nullable = true) - Airline's IATA code
 * admission-num: float (nullable = true) - admission_num given at the entry port in US
 * flight_no: string (nullable = true) - arrival flight number
 * visa_type: string (nullable = true) - type of visa issued for the individual 
