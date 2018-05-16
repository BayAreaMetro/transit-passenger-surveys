
# Variable Dictionary

Below is an example variable dictionary used to convert survey variables as delivered from vendors into the standard MTC format. The "Generic_Variable" and "Generic_Response" fields are the standard MTC survey field. This example is taken from the San Francisco MUNI survey conducted by ETC Institute. 


<br/>



| **Survey_Variable**          | **Survey_Response**                                            | **Generic_Variable**             | **Generic_Response**                     |
|:-----------------------------|:---------------------------------------------------------------|:---------------------------------|:-----------------------------------------|
| final_change_to_access_mode  | Walk all the way                                               | access_mode                      | walk                                     |
| final_change_to_access_mode  | Walked all the way                                             | access_mode                      | walk                                     |
| final_change_to_access_mode  | Bike (BIKE SHARE)                                              | access_mode                      | bike                                     |
| final_change_to_access_mode  | Bike (PERSONAL BIKE)                                           | access_mode                      | bike                                     |
| final_change_to_access_mode  | Taxi                                                           | access_mode                      | knr                                      |
| final_change_to_access_mode  | Used Uber or similar service                                   | access_mode                      | knr                                      |
| final_change_to_access_mode  | Was dropped off (Not a service)                                | access_mode                      | knr                                      |
| final_change_to_access_mode  | Drove alone and parked                                         | access_mode                      | pnr                                      |
| final_change_to_access_mode  | Drove or rode with others and parked                           | access_mode                      | pnr                                      |
| final_change_to_access_mode  | Shuttle                                                        | access_mode                      | knr                                      |
| final_change_to_access_mode  | Skateboard/Longboard                                           | access_mode                      | walk                                     |
| final_change_to_access_mode  | Wheelchair/Scooter                                             | access_mode                      | walk                                     |
| at_school_prior_to_orig_purp | No                                                             | at_school_prior_to_orig_purp     | not at school before surveyed trip       |
| at_school_prior_to_orig_purp | Yes                                                            | at_school_prior_to_orig_purp     | at school before surveyed trip           |
| at_work_prior_to_orig_purp   | No                                                             | at_work_prior_to_orig_purp       | not at work before surveyed trip         |
| at_work_prior_to_orig_purp   | Yes                                                            | at_work_prior_to_orig_purp       | at work before surveyed trip             |
| final_destin_lat             | NONCATEGORICAL                                                 | dest_lat                         | NONCATEGORICAL                           |
| final_destin_lon             | NONCATEGORICAL                                                 | dest_lon                         | NONCATEGORICAL                           |
| dest_purp                    | Your usual WORKPLACE                                           | dest_purp                        | work                                     |
| dest_purp                    | Work related                                                   | dest_purp                        | work-related                             |
| dest_purp                    | Your HOME                                                      | dest_purp                        | home                                     |
| dest_purp                    | Your Hotel                                                     | dest_purp                        | other discretionary                      |
| dest_purp                    | Hotel Residence (VISITOR ONLY)                                 | dest_purp                        | other discretionary                      |
| dest_purp                    | Social or recreational                                         | dest_purp                        | social recreation                        |
| dest_purp                    | Shopping                                                       | dest_purp                        | shopping                                 |
| dest_purp                    | School (K-12) (student only)                                   | dest_purp                        | school                                   |
| dest_purp                    | College or University (students only)                          | dest_purp                        | college                                  |
| dest_purp                    | Airport (airline passengers only)                              | dest_purp                        | other discretionary                      |
| dest_purp                    | Medical or dental                                              | dest_purp                        | other maintenance                        |
| dest_purp                    | Dining or coffee                                               | dest_purp                        | eat out                                  |
| dest_purp                    | Escorting others (e.g. children or elderly)                    | dest_purp                        | escorting                                |
| dest_purp                    | Personal business (DMV, bank, library, etc.)                   | dest_purp                        | other maintenance                        |
| dest_purp                    | Other                                                          | dest_purp                        | other maintenance                        |
| final_change_to_egress_mode  | Walk all the way                                               | egress_mode                      | walk                                     |
| final_change_to_egress_mode  | Walked all the way                                             | egress_mode                      | walk                                     |
| final_change_to_egress_mode  | Bike (BIKE SHARE)                                              | egress_mode                      | bike                                     |
| final_change_to_egress_mode  | Bike (PERSONAL BIKE)                                           | egress_mode                      | bike                                     |
| final_change_to_egress_mode  | Taxi                                                           | egress_mode                      | knr                                      |
| final_change_to_egress_mode  | Be picked up by Uber or similar service                        | egress_mode                      | knr                                      |
| final_change_to_egress_mode  | Be picked up by someone (Not a service)                        | egress_mode                      | knr                                      |
| final_change_to_egress_mode  | Drove alone and parked                                         | egress_mode                      | pnr                                      |
| final_change_to_egress_mode  | Get in a parked vehicle and drive alone                        | egress_mode                      | pnr                                      |
| final_change_to_egress_mode  | Get in a parked vehicle and drive with others                  | egress_mode                      | pnr                                      |
| final_change_to_egress_mode  | Shuttle                                                        | egress_mode                      | knr                                      |
| final_change_to_egress_mode  | Skateboard/Longboard                                           | egress_mode                      | walk                                     |
| final_change_to_egress_mode  | Wheelchair/Scooter                                             | egress_mode                      | walk                                     |
| work_status                  | Not employed                                                   | work_status                      | non-worker                               |
| work_status                  | Employed                                                       | work_status                      | full- or part-time                       |
| workers                      | None (0)                                                       | workers                          | zero                                     |
| workers                      | One (1)                                                        | workers                          | one                                      |
| workers                      | Two (2)                                                        | workers                          | two                                      |
| workers                      | Three (3)                                                      | workers                          | three                                    |
| workers                      | Four (4)                                                       | workers                          | four                                     |
| workers                      | Five (5)                                                       | workers                          | five                                     |
| workers                      | Six (6)                                                        | workers                          | six or more                              |
| workers                      | Seven (7)                                                      | workers                          | six or more                              |
| workers                      | Eight (8)                                                      | workers                          | six or more                              |
| workers                      | Nine (9)                                                       | workers                          | six or more                              |
| workers                      | Ten or More (10+)                                              | workers                          | six or more                              |
| eng_proficient               | Very well                                                      | eng_proficient                   | VERY WELL                                |
| eng_proficient               | Well                                                           | eng_proficient                   | WELL                                     |
| eng_proficient               | Not well                                                       | eng_proficient                   | NOT WELL                                 |
| eng_proficient               | Not at all                                                     | eng_proficient                   | NOT AT ALL                               |
| fare_category                | Adult                                                          | fare_category                    | adult                                    |
| fare_category                | Disabled/Medicare Card Holder                                  | fare_category                    | disabled                                 |
| fare_category                | Free Muni for People with Disabilities                         | fare_category                    | disabled                                 |
| fare_category                | Free Muni for Seniors                                          | fare_category                    | senior                                   |
| fare_category                | Free Muni for Youth                                            | fare_category                    | youth                                    |
| fare_category                | Low Income (Lifeline) Pass                                     | fare_category                    | adult                                    |
| fare_category                | Senior                                                         | fare_category                    | senior                                   |
| fare_category                | Youth                                                          | fare_category                    | youth                                    |
| at_work_after_dest_purp      | No                                                             | at_work_after_dest_purp          | not at work after surveyed trip          |
| at_work_after_dest_purp      | Yes                                                            | at_work_after_dest_purp          | at work after surveyed trip              |
| household_income             | Less than $10,000                                              | household_income                 | under $10,000                            |
| household_income             | $10,000-$24,999                                                | household_income                 | $10,000 to $25,000                       |
| household_income             | $25,000-$34,999                                                | household_income                 | $25,000 to $35,000                       |
| household_income             | $35,000-$39,999                                                | household_income                 | $35,000 to $50,000                       |
| household_income             | $40,000-$49,999                                                | household_income                 | $35,000 to $50,000                       |
| household_income             | $50,000-$59,999                                                | household_income                 | $50,000 to $75,000                       |
| household_income             | $60,000-$74,999                                                | household_income                 | $50,000 to $75,000                       |
| household_income             | $75,000-$99,999                                                | household_income                 | $75,000 to $100,000                      |
| household_income             | $100,000-$149,999                                              | household_income                 | $100,000 to $150,000                     |
| household_income             | $150,000-$199,999                                              | household_income                 | $150,000 or higher                       |
| household_income             | $200,000 or more                                               | household_income                 | $150,000 or higher                       |
| hispanic                     | No                                                             | hispanic                         | NOT HISPANIC/LATINO OR OF SPANISH ORIGIN |
| hispanic                     | Yes                                                            | hispanic                         | HISPANIC/LATINO OR OF SPANISH ORIGIN     |
| home_lat                     | NONCATEGORICAL                                                 | home_lat                         | NONCATEGORICAL                           |
| home_lon                     | NONCATEGORICAL                                                 | home_lon                         | NONCATEGORICAL                           |
| id                           | NONCATEGORICAL                                                 | ID                               | NONCATEGORICAL                           |
| language_at_home_binary      | No                                                             | language_at_home_binary          | ENGLISH ONLY                             |
| language_at_home_binary      | Yes                                                            | language_at_home_binary          | OTHER                                    |
| language_at_home_detail      | NONCATEGORICAL                                                 | language_at_home_detail          | NONCATEGORICAL                           |
| final_origin_lat             | NONCATEGORICAL                                                 | orig_lat                         | NONCATEGORICAL                           |
| final_origin_lon             | NONCATEGORICAL                                                 | orig_lon                         | NONCATEGORICAL                           |
| orig_purp                    | Your usual WORKPLACE                                           | orig_purp                        | work                                     |
| orig_purp                    | Work related                                                   | orig_purp                        | work-related                             |
| orig_purp                    | Your HOME                                                      | orig_purp                        | home                                     |
| orig_purp                    | Your Hotel                                                     | orig_purp                        | other discretionary                      |
| orig_purp                    | Hotel Residence (VISITOR ONLY)                                 | orig_purp                        | other discretionary                      |
| orig_purp                    | Social or recreational                                         | orig_purp                        | social recreation                        |
| orig_purp                    | Shopping                                                       | orig_purp                        | shopping                                 |
| orig_purp                    | School (K-12) (student only)                                   | orig_purp                        | school                                   |
| orig_purp                    | College or University (students only)                          | orig_purp                        | college                                  |
| orig_purp                    | Medical or dental                                              | orig_purp                        | other maintenance                        |
| orig_purp                    | Dining or coffee                                               | orig_purp                        | eat out                                  |
| orig_purp                    | Escorting others (e.g. children or elderly)                    | orig_purp                        | escorting                                |
| orig_purp                    | Personal business (DMV, bank, library, etc.)                   | orig_purp                        | other maintenance                        |
| orig_purp                    | Airport (airline passengers only)                              | orig_purp                        | other discretionary                      |
| orig_purp                    | Hotel  (GUEST)                                                 | orig_purp                        | other discretionary                      |
| fare_medium                  | OTHER PASS                                                     | fare_medium                      | pass                                     |
| fare_medium                  | BY CLIPPER                                                     | fare_medium                      | clipper                                  |
| fare_medium                  | BY CASH OR PAPER                                               | fare_medium                      | cash                                     |
| fare_medium                  | Muni Mobile app                                                | fare_medium                      | pass                                     |
| persons                      | One (1)                                                        | persons                          | one                                      |
| persons                      | Two (2)                                                        | persons                          | two                                      |
| persons                      | Three (3)                                                      | persons                          | three                                    |
| persons                      | Four (4)                                                       | persons                          | four                                     |
| persons                      | Five (5)                                                       | persons                          | five                                     |
| persons                      | Six (6)                                                        | persons                          | six                                      |
| persons                      | Seven (7)                                                      | persons                          | seven                                    |
| persons                      | Eight (8)                                                      | persons                          | eight                                    |
| persons                      | Nine (9)                                                       | persons                          | nine                                     |
| persons                      | Ten or More (10+)                                              | persons                          | ten or more                              |
| race_dmy_ind                 | Yes                                                            | race_dmy_ind                     | 1                                        |
| race_dmy_ind                 | No                                                             | race_dmy_ind                     | 0                                        |
| race_dmy_ind                 | N/A                                                            | race_dmy_ind                     | 0                                        |
| race_dmy_asn                 | Yes                                                            | race_dmy_asn                     | 1                                        |
| race_dmy_asn                 | No                                                             | race_dmy_asn                     | 0                                        |
| race_dmy_asn                 | N/A                                                            | race_dmy_asn                     | 0                                        |
| race_dmy_blk                 | Yes                                                            | race_dmy_blk                     | 1                                        |
| race_dmy_blk                 | No                                                             | race_dmy_blk                     | 0                                        |
| race_dmy_blk                 | N/A                                                            | race_dmy_blk                     | 0                                        |
| race_dmy_hwi                 | Yes                                                            | race_dmy_hwi                     | 1                                        |
| race_dmy_hwi                 | No                                                             | race_dmy_hwi                     | 0                                        |
| race_dmy_hwi                 | N/A                                                            | race_dmy_hwi                     | 0                                        |
| race_dmy_wht                 | Yes                                                            | race_dmy_wht                     | 1                                        |
| race_dmy_wht                 | No                                                             | race_dmy_wht                     | 0                                        |
| race_dmy_wht                 | N/A                                                            | race_dmy_wht                     | 0                                        |
| race_other_string            | NONCATEGORICAL                                                 | race_other_string                | NONCATEGORICAL                           |
| gender                       | Male                                                           | gender                           | male                                     |
| gender                       | Female                                                         | gender                           | female                                   |
| gender                       | Other                                                          | gender                           | other                                    |
| school_lat                   | NONCATEGORICAL                                                 | school_lat                       | NONCATEGORICAL                           |
| school_lon                   | NONCATEGORICAL                                                 | school_lon                       | NONCATEGORICAL                           |
| student_status               | Not a student                                                  | student_status                   | non-student                              |
| student_status               | Part Time college/university/vocational/technical/trade school | student_status                   | full- or part-time                       |
| student_status               | Full Time college/university/vocational/technical/trade school | student_status                   | full- or part-time                       |
| student_status               | Other                                                          | student_status                   | full- or part-time                       |
| student_status               | Grade 9-12                                                     | student_status                   | full- or part-time                       |
| student_status               | Grade K-5                                                      | student_status                   | full- or part-time                       |
| student_status               | Grade 6-8                                                      | student_status                   | full- or part-time                       |
| completed_date               | NONCATEGORICAL                                                 | date_string                      | NONCATEGORICAL                           |
| completed_time               | NONCATEGORICAL                                                 | time_string                      | NONCATEGORICAL                           |
| depart_hour                  | Before 5 am                                                    | depart_hour                      | 4                                        |
| depart_hour                  | 5 - 5:59 am                                                    | depart_hour                      | 5                                        |
| depart_hour                  | 6 - 6:59 am                                                    | depart_hour                      | 6                                        |
| depart_hour                  | 7 - 7:59 am                                                    | depart_hour                      | 7                                        |
| depart_hour                  | 8 - 8:59 am                                                    | depart_hour                      | 8                                        |
| depart_hour                  | 9 - 9:59 am                                                    | depart_hour                      | 9                                        |
| depart_hour                  | 10 - 10:59 am                                                  | depart_hour                      | 10                                       |
| depart_hour                  | 11 - 11:59 am                                                  | depart_hour                      | 11                                       |
| depart_hour                  | 12 - 12:59 pm                                                  | depart_hour                      | 12                                       |
| depart_hour                  | 1 - 1:59 pm                                                    | depart_hour                      | 13                                       |
| depart_hour                  | 2 - 2:59 pm                                                    | depart_hour                      | 14                                       |
| depart_hour                  | 3 - 3:59 pm                                                    | depart_hour                      | 15                                       |
| depart_hour                  | 4 - 4:59 pm                                                    | depart_hour                      | 16                                       |
| depart_hour                  | 5 - 5:59 pm                                                    | depart_hour                      | 17                                       |
| depart_hour                  | 6 - 6:59 pm                                                    | depart_hour                      | 18                                       |
| depart_hour                  | 7 - 7:59 pm                                                    | depart_hour                      | 19                                       |
| depart_hour                  | 8 - 8:59 pm                                                    | depart_hour                      | 20                                       |
| depart_hour                  | 9 - 9:59 pm                                                    | depart_hour                      | 21                                       |
| depart_hour                  | 10 - 10:59 pm                                                  | depart_hour                      | 22                                       |
| depart_hour                  | 11 pm or later                                                 | depart_hour                      | 23                                       |
| return_hour                  | Before 5 am                                                    | return_hour                      | 4                                        |
| return_hour                  | 5 - 5:59 am                                                    | return_hour                      | 5                                        |
| return_hour                  | 6 - 6:59 am                                                    | return_hour                      | 6                                        |
| return_hour                  | 7 - 7:59 am                                                    | return_hour                      | 7                                        |
| return_hour                  | 8 - 8:59 am                                                    | return_hour                      | 8                                        |
| return_hour                  | 9 - 9:59 am                                                    | return_hour                      | 9                                        |
| return_hour                  | 10 - 10:59 am                                                  | return_hour                      | 10                                       |
| return_hour                  | 11 - 11:59 am                                                  | return_hour                      | 11                                       |
| return_hour                  | 12 - 12:59 pm                                                  | return_hour                      | 12                                       |
| return_hour                  | 1 - 1:59 pm                                                    | return_hour                      | 13                                       |
| return_hour                  | 2 - 2:59 pm                                                    | return_hour                      | 14                                       |
| return_hour                  | 3 - 3:59 pm                                                    | return_hour                      | 15                                       |
| return_hour                  | 4 - 4:59 pm                                                    | return_hour                      | 16                                       |
| return_hour                  | 5 - 5:59 pm                                                    | return_hour                      | 17                                       |
| return_hour                  | 6 - 6:59 pm                                                    | return_hour                      | 18                                       |
| return_hour                  | 7 - 7:59 pm                                                    | return_hour                      | 19                                       |
| return_hour                  | 8 - 8:59 pm                                                    | return_hour                      | 20                                       |
| return_hour                  | 9 - 9:59 pm                                                    | return_hour                      | 21                                       |
| return_hour                  | 10 - 10:59 pm                                                  | return_hour                      | 22                                       |
| return_hour                  | 11 pm or later                                                 | return_hour                      | 23                                       |
| final_trip_first_route       | NONCATEGORICAL                                                 | first_route_before_survey_board  | NONCATEGORICAL                           |
| final_trip_second_route      | NONCATEGORICAL                                                 | second_route_before_survey_board | NONCATEGORICAL                           |
| final_trip_third_route       | NONCATEGORICAL                                                 | third_route_before_survey_board  | NONCATEGORICAL                           |
| final_trip_to_first_route    | NONCATEGORICAL                                                 | first_route_after_survey_alight  | NONCATEGORICAL                           |
| final_trip_to_second_route   | NONCATEGORICAL                                                 | second_route_after_survey_alight | NONCATEGORICAL                           |
| final_trip_to_third_route    | NONCATEGORICAL                                                 | third_route_after_survey_alight  | NONCATEGORICAL                           |
| vehicles                     | None (0)                                                       | vehicles                         | zero                                     |
| vehicles                     | One (1)                                                        | vehicles                         | one                                      |
| vehicles                     | Two (2)                                                        | vehicles                         | two                                      |
| vehicles                     | Three (3)                                                      | vehicles                         | three                                    |
| vehicles                     | Four (4)                                                       | vehicles                         | four or more                             |
| vehicles                     | Five (5)                                                       | vehicles                         | four or more                             |
| vehicles                     | Six (6)                                                        | vehicles                         | four or more                             |
| vehicles                     | Seven (7)                                                      | vehicles                         | four or more                             |
| vehicles                     | Eight (8)                                                      | vehicles                         | four or more                             |
| vehicles                     | Nine (9)                                                       | vehicles                         | four or more                             |
| vehicles                     | Ten or more (10+)                                              | vehicles                         | four or more                             |
| at_school_after_dest_purp    | No                                                             | at_school_after_dest_purp        | not at school after surveyed trip        |
| at_school_after_dest_purp    | Yes                                                            | at_school_after_dest_purp        | at school after surveyed trip            |
| workplace_lat                | NONCATEGORICAL                                                 | workplace_lat                    | NONCATEGORICAL                           |
| workplace_lon                | NONCATEGORICAL                                                 | workplace_lon                    | NONCATEGORICAL                           |
| year_born_four_digit         | NONCATEGORICAL                                                 | year_born_four_digit             | NONCATEGORICAL                           |
| survey_production_type       | Online                                                         | survey_type                      | tablet_pi                                |
| survey_production_type       | Offline                                                        | survey_type                      | tablet_pi                                |
| survey_production_type       | CATI                                                           | survey_type                      | brief_cati                               |
| survey_language              |                                                                | interview_language               | ENGLISH                                  |
| survey_language              | English                                                        | interview_language               | ENGLISH                                  |
| survey_language              | Spanish                                                        | interview_language               | SPANISH                                  |
| survey_language              | Chinese - Cantonese                                            | interview_language               | CHINESE                                  |
| survey_language              | Chinese - Mandarin                                             | interview_language               | RUSSIAN                                  |
| survey_language              | Other                                                          | interview_language               | OTHER                                    |
| first_board_lat              | NONCATEGORICAL                                                 | first_board_lat                  | NONCATEGORICAL                           |
| first_board_lon              | NONCATEGORICAL                                                 | first_board_lon                  | NONCATEGORICAL                           |
| last_alight_lat              | NONCATEGORICAL                                                 | last_alight_lat                  | NONCATEGORICAL                           |
| last_alight_lon              | NONCATEGORICAL                                                 | last_alight_lon                  | NONCATEGORICAL                           |
| tunnel_weight_factor         | NONCATEGORICAL                                                 | weight                           | NONCATEGORICAL                           |
| final_boarding_lat           | NONCATEGORICAL                                                 | survey_board_lat                 | NONCATEGORICAL                           |
| final_boarding_lon           | NONCATEGORICAL                                                 | survey_board_lon                 | NONCATEGORICAL                           |
| final_alighting_lat          | NONCATEGORICAL                                                 | survey_alight_lat                | NONCATEGORICAL                           |
| final_alighting_lon          | NONCATEGORICAL                                                 | survey_alight_lon                | NONCATEGORICAL                           |
| clipper_detail               | CASH VALUE ON CLIPPER                                          | clipper_detail                   | clipper (e-cash)                         |
| clipper_detail               | "M" MONTHLY PASS ON CLIPPER                                    | clipper_detail                   | clipper (pass)                           |
| clipper_detail               | "A" MONTHLY PASS ON CLIPPER                                    | clipper_detail                   | clipper (pass)                           |
| clipper_detail               | Other                                                          | clipper_detail                   | clipper                                  |
| final_route_surveyed         | NONCATEGORICAL                                                 | route                            | NONCATEGORICAL                           |
| number_transfers_orig_board  | 0                                                              | number_transfers_orig_board      | 0                                        |
| number_transfers_orig_board  | 1                                                              | number_transfers_orig_board      | 1                                        |
| number_transfers_orig_board  | 2                                                              | number_transfers_orig_board      | 2                                        |
| number_transfers_orig_board  | 3                                                              | number_transfers_orig_board      | 3                                        |
| number_transfers_orig_board  | 4                                                              | number_transfers_orig_board      | 4                                        |
| number_transfers_alight_dest | 0                                                              | number_transfers_alight_dest     | 0                                        |
| number_transfers_alight_dest | 1                                                              | number_transfers_alight_dest     | 1                                        |
| number_transfers_alight_dest | 2                                                              | number_transfers_alight_dest     | 2                                        |
| number_transfers_alight_dest | 3                                                              | number_transfers_alight_dest     | 3                                        |
| number_transfers_alight_dest | 4                                                              | number_transfers_alight_dest     | 4                                        |

<br/>  

[Return to summary](README.md)