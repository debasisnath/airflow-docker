--- address w poc


this is working select query  

---

SELECT  pzip       AS permanent_address_pin -- Pin_Code 
       ,PAD1       AS permanent_address_line1 -- Address 
       ,PAD2       AS permanent_address_line2 -- Address 
       ,PAD3       AS permanent_address_line3 -- Address 
       ,PAD4       AS permanent_address_line4 -- Address 
       ,PCITY      AS permanent_address_city -- City 
       ,PSTATE     AS permanent_address_state_code
       ,PSTATE_NME AS permanent_address_state_name -- State 
       ,PLOC       AS permanent_address_location
       ,ZADDRTYPE  AS address_type
       ,ZMPH       AS mobile_phone -- Telephone 
       ,APH        AS alternate_phone_number -- Telephone 
FROM bbledl.tb_cic_cif_snp_edl
WHERE full_dte = '2025-03-26'
LIMIT 3
;

--
understand that and modify the code below

-- ===================
-- DISTINCT

SELECT  DISTINCT(PCITY)  AS d_city
FROM bbledl.tb_cic_cif_snp_edl
WHERE full_dte = '2025-03-26'
order by d_city

SELECT DISTINCT PSTATE AS d_PSTATE
FROM bbledl.tb_cic_cif_snp_edl
WHERE full_dte = '2025-03-26'
ORDER BY d_PSTATE;

-- D_STATE
SELECT DISTINCT DSC_STATENAME AS D_STATE
FROM bbledl.tb_cic_cif_snp_edl
WHERE full_dte = '2025-03-26'
ORDER BY D_STATE;

======
Agartala,Agra,Ahmedabad,Ajmer,Akola,Aligarh,Allahabad,Amravati,Amritsar,Aurangabad,
Bangalore,Bareilly,Bathinda,Belgaum,Bengaluru,Bhagalpur,Bhilai,Bhiwandi,Bhopal,Bhubaneswar,Bikaner,BokaroSteelCity,
Chandigarh,Chennai,Coimbatore,Cuttack,
Daman, Darbhanga, Davanagere, Dehradun, Delhi, Deoghar, Dewas, Dhanbad, Dharamshala, Dharwad, Dholpur, Dibrugarh, Dindigul, Dispur, Diu, Dombivli, Durg, Durgapur, Dwarka,
Erode,
Faridabad, Farrukhabad, Fatehabad, Fatehpur, Firozabad,
Gandhidham, Gandhinagar, Gangtok, Gaya, Ghaziabad, Gwalior, Guwahati, Gurgaon,
Haldwani-cum-Kathgodam, Hyderabad, Howrah, Hisar, Hubli-Dharwad,
Imphal, Indore, Itanagar, Itarsi,
Jabalpur, Jaipur, Jalandhar, Jammu, Jamnagar, Jamshedpur, Jhansi, Jodhpur, Junagadh,
Kakinada, Kalyan-Dombivli, Kanpur, Karimnagar, Kochi, Kolhapur, Kolkata, Kota, Kozhikode, Kurnool,
Lucknow, Ludhiana, Latur,
Madurai, Malegaon, Mangalore, Mathura, Meerut, Mira-Bhayandar, Moradabad, Mumbai, Muzaffarpur, Mysore,
Nagpur, Nanded, Nashik, Navi Mumbai, Nellore, Noida,
Ongole,
Palakkad, Panaji, Panipat, Patiala, Patna, Pimpri-Chinchwad, Pondicherry, Porbandar, Pune, Purnia,

Raichur, Raipur, Rajahmundry, Rajkot, Ranchi, Ratlam, Rewa, Rishikesh, Rohtak, Rourkela,
Saharanpur, Salem, Sangli, Shillong, Shimla, Sikar, Siliguri, Solapur, Srinagar, Surat,
Thane, Thiruvananthapuram, Tiruchirappalli, Tirunelveli, Tirupati, Thoothukudi, Tumkur,
Udaipur, Ujjain, Ulhasnagar,
Vadodara, Varanasi, Vasai-Virar, Vellore, Vijayawada, Visakhapatnam,
Warangal,

Yamunanagar, Yavatmal


'Agartala', 'Agra', 'Ahmedabad', 'Ajmer', 'Akola', 'Aligarh', 'Allahabad', 'Amravati', 'Amritsar', 'Aurangabad', 'Bangalore', 'Bareilly', 'Bathinda', 'Belgaum', 'Bengaluru', 'Bhagalpur', 'Bhilai', 'Bhiwandi', 'Bhopal', 'Bhubaneswar', 'Bikaner', 'BokaroSteelCity', 'Chandigarh', 'Chennai', 'Coimbatore', 'Cuttack', 'Daman', 'Darbhanga', 'Davanagere', 'Dehradun', 'Delhi', 'Deoghar', 'Dewas', 'Dhanbad', 'Dharamshala', 'Dharwad', 'Dholpur', 'Dibrugarh', 'Dindigul', 'Dispur', 'Diu', 'Dombivli', 'Durg', 'Durgapur', 'Dwarka', 'Erode', 'Faridabad', 'Farrukhabad', 'Fatehabad', 'Fatehpur', 'Firozabad', 'Gandhidham', 'Gandhinagar', 'Gangtok', 'Gaya', 'Ghaziabad', 'Gwalior', 'Guwahati', 'Gurgaon', 'Haldwani-cum-Kathgodam', 'Hyderabad', 'Howrah', 'Hisar', 'Hubli-Dharwad', 'Imphal', 'Indore', 'Itanagar', 'Itarsi', 'Jabalpur', 'Jaipur', 'Jalandhar', 'Jammu', 'Jamnagar', 'Jamshedpur', 'Jhansi', 'Jodhpur', 'Junagadh', 'Kakinada', 'Kalyan-Dombivli', 'Kanpur', 'Karimnagar', 'Kochi', 'Kolhapur', 'Kolkata', 'Kota', 'Kozhikode', 'Kurnool', 'Lucknow', 'Ludhiana', 'Latur', 'Madurai', 'Malegaon', 'Mangalore', 'Mathura', 'Meerut', 'Mira-Bhayandar', 'Moradabad', 'Mumbai', 'Muzaffarpur', 'Mysore', 'Nagpur', 'Nanded', 'Nashik', 'Navi Mumbai', 'Nellore', 'Noida', 'Ongole', 'Palakkad', 'Panaji', 'Panipat', 'Patiala', 'Patna', 'Pimpri-Chinchwad', 'Pondicherry', 'Porbandar', 'Pune', 'Purnia', 'Raichur', 'Raipur', 'Rajahmundry', 'Rajkot', 'Ranchi', 'Ratlam', 'Rewa', 'Rishikesh', 'Rohtak', 'Rourkela', 'Saharanpur', 'Salem', 'Sangli', 'Shillong', 'Shimla', 'Sikar', 'Siliguri', 'Solapur', 'Srinagar', 'Surat', 'Thane', 'Thiruvananthapuram', 'Tiruchirappalli', 'Tirunelveli', 'Tirupati', 'Thoothukudi', 'Tumkur', 'Udaipur', 'Ujjain', 'Ulhasnagar', 'Vadodara', 'Varanasi', 'Vasai-Virar', 'Vellore', 'Vijayawada', 'Visakhapatnam', 'Warangal', 'Yamunanagar', 'Yavatmal'

=====
'Andhra Pradesh', 'Assam', 'Bihar', 'Chhattisgarh', 'Dadra & Nagar Haveli', 'Goa', 'Gujarat', 'Haryana', 'Himachal Pradesh', 'Jharkhand', 'Karnataka', 'Madhya Pradesh', 'Maharashtra', 'Manipur', 'Meghalaya', 'Mizoram', 'NCT of Delhi', 'Nagaland', 'Orissa', 'Pondicherry', 'Punjab', 'Rajasthan', 'Sikkim', 'Tamil Nadu', 'Telangana', 'Tripura', 'UTTARAKHAND', 'Uttar Pradesh', 'West Bengal'


=====
--- W OK
-- Commercial Data Quality Index

--  S_No )Address

WITH DataQualityScoring AS (
    SELECT 
        pzip AS Pin_Code,
        PAD1 AS Address_Line1,
        PAD2 AS Address_Line2,
        PAD3 AS Address_Line3,
        PAD4 AS Address_Line4,
        PCITY AS City,
        PSTATE AS State_Code,
        DSC_STATENAME AS State_Name,
        ZMPH AS Mobile_Phone,
        APH AS Alternate_Phone,
        STAT AS customer_status_code,
        ACN AS  customer_id,
        -- Address Validation {w->3}
        CASE 
            WHEN LENGTH(CONCAT(PAD1, PAD2, PAD3, PAD4)) >= 5 
                 AND CONCAT(PAD1, PAD2, PAD3, PAD4) NOT LIKE '%Same as above%'
                 AND CONCAT(PAD1, PAD2, PAD3, PAD4) NOT LIKE '%null%'
                 AND CONCAT(PAD1, PAD2, PAD3, PAD4) NOT LIKE '%#NA%'
                 AND CONCAT(PAD1, PAD2, PAD3, PAD4) NOT LIKE '%zzzzz%'
                 AND CONCAT(PAD1, PAD2, PAD3, PAD4) NOT LIKE '%none%'
                 AND CONCAT(PAD1, PAD2, PAD3, PAD4) NOT LIKE '%abcde%'
                 AND CONCAT(PAD1, PAD2, PAD3, PAD4) NOT LIKE '%$%' 
                 AND CONCAT(PAD1, PAD2, PAD3, PAD4) NOT LIKE '%*%' THEN 3
            ELSE 0
        END AS Address_Score,

        -- City Validation (Hardcoded List) {w->3}
        CASE 
            WHEN PCITY IN ('Agartala', 'Agra', 'Ahmedabad', 'Ajmer', 'Akola', 'Aligarh', 'Allahabad', 'Amravati', 'Amritsar', 'Aurangabad', 'Bangalore', 'Bareilly', 'Bathinda', 'Belgaum', 'Bengaluru', 'Bhagalpur', 'Bhilai', 'Bhiwandi', 'Bhopal', 'Bhubaneswar', 'Bikaner', 'BokaroSteelCity', 'Chandigarh', 'Chennai', 'Coimbatore', 'Cuttack', 'Daman', 'Darbhanga', 'Davanagere', 'Dehradun', 'Delhi', 'Deoghar', 'Dewas', 'Dhanbad', 'Dharamshala', 'Dharwad', 'Dholpur', 'Dibrugarh', 'Dindigul', 'Dispur', 'Diu', 'Dombivli', 'Durg', 'Durgapur', 'Dwarka', 'Erode', 'Faridabad', 'Farrukhabad', 'Fatehabad', 'Fatehpur', 'Firozabad', 'Gandhidham', 'Gandhinagar', 'Gangtok', 'Gaya', 'Ghaziabad', 'Gwalior', 'Guwahati', 'Gurgaon', 'Haldwani-cum-Kathgodam', 'Hyderabad', 'Howrah', 'Hisar', 'Hubli-Dharwad', 'Imphal', 'Indore', 'Itanagar', 'Itarsi', 'Jabalpur', 'Jaipur', 'Jalandhar', 'Jammu', 'Jamnagar', 'Jamshedpur', 'Jhansi', 'Jodhpur', 'Junagadh', 'Kakinada', 'Kalyan-Dombivli', 'Kanpur', 'Karimnagar', 'Kochi', 'Kolhapur', 'Kolkata', 'Kota', 'Kozhikode', 'Kurnool', 'Lucknow', 'Ludhiana', 'Latur', 'Madurai', 'Malegaon', 'Mangalore', 'Mathura', 'Meerut', 'Mira-Bhayandar', 'Moradabad', 'Mumbai', 'Muzaffarpur', 'Mysore', 'Nagpur', 'Nanded', 'Nashik', 'Navi Mumbai', 'Nellore', 'Noida', 'Ongole', 'Palakkad', 'Panaji', 'Panipat', 'Patiala', 'Patna', 'Pimpri-Chinchwad', 'Pondicherry', 'Porbandar', 'Pune', 'Purnia', 'Raichur', 'Raipur', 'Rajahmundry', 'Rajkot', 'Ranchi', 'Ratlam', 'Rewa', 'Rishikesh', 'Rohtak', 'Rourkela', 'Saharanpur', 'Salem', 'Sangli', 'Shillong', 'Shimla', 'Sikar', 'Siliguri', 'Solapur', 'Srinagar', 'Surat', 'Thane', 'Thiruvananthapuram', 'Tiruchirappalli', 'Tirunelveli', 'Tirupati', 'Thoothukudi', 'Tumkur', 'Udaipur', 'Ujjain', 'Ulhasnagar', 'Vadodara', 'Varanasi', 'Vasai-Virar', 'Vellore', 'Vijayawada', 'Visakhapatnam', 'Warangal', 'Yamunanagar', 'Yavatmal') 
            THEN 3
            ELSE 0
        END AS City_Score,

        -- Pin_Code Validation {w->3}
        CASE 
            WHEN LENGTH(pzip) = 6 AND pzip BETWEEN '100000' AND '999999' THEN 3
            ELSE 0
        END AS Pin_Code_Score,

        -- State Validation (Hardcoded List) {w->3}
        CASE 
            WHEN DSC_STATENAME IN ('Andhra Pradesh', 'Assam', 'Bihar', 'Chhattisgarh', 'Dadra & Nagar Haveli', 'Goa', 'Gujarat', 'Haryana', 'Himachal Pradesh', 'Jharkhand', 'Karnataka', 'Madhya Pradesh', 'Maharashtra', 'Manipur', 'Meghalaya', 'Mizoram', 'NCT of Delhi', 'Nagaland', 'Orissa', 'Pondicherry', 'Punjab', 'Rajasthan', 'Sikkim', 'Tamil Nadu', 'Telangana', 'Tripura', 'UTTARAKHAND', 'Uttar Pradesh', 'West Bengal')
            THEN 3
            ELSE 0
        END AS State_Score,

        -- Telephone Validation {w->3}
        CASE 
            WHEN (ZMPH IS NOT NULL AND LENGTH(ZMPH) >= 10)
                 OR (APH IS NOT NULL AND LENGTH(APH) >= 10) THEN 3
            ELSE 0
        END AS Telephone_Score
    FROM bbledl.tb_cic_cif_snp_edl
    WHERE full_dte = '2025-03-26'
    AND STAT != 4
),
FinalDataQualityIndex AS (
    SELECT 
        customer_id,
        customer_status_code,
        Pin_Code,
        City,
        State_Code,
        State_Name,
        Mobile_Phone,
        Alternate_Phone,
        Address_Score,
        City_Score,
        Pin_Code_Score,
        State_Score,
        Telephone_Score,
        -- Calculate Total Score
        (Address_Score + City_Score + Pin_Code_Score + State_Score + Telephone_Score) AS Total_Score,
        -- Calculate Data Quality Index
        (Address_Score + City_Score + Pin_Code_Score + State_Score + Telephone_Score) / 15.0 * 100 AS Data_Quality_Index
    FROM DataQualityScoring
)
SELECT *
FROM FinalDataQualityIndex


LIMIT 10;
